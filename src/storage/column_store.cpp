#include "../../include/storage/column_store.h"
#include <ctime>
#include <cstring>
#include <iostream>
#include <fcntl.h>
#include <unistd.h>

namespace flexql {

ColumnStore::ColumnStore(std::shared_ptr<Table> t)
    : table(t), row_count(0), pk_col_idx(-1), flush_count(0), flush_interval_batches(DEFAULT_FLUSH_INTERVAL_BATCHES)
{
    data_file_path = "data/tables/" + t->getName() + ".dat";
    
    // Open raw POSIX fd for fdatasync/fadvise (page cache pressure control)
    data_fd = open(data_file_path.c_str(), O_RDWR | O_CREAT, 0644);

    // Open fstream in in/out binary mode
    data_file.open(data_file_path, std::ios::in | std::ios::out | std::ios::binary);
    if (!data_file.is_open()) {
        // Create file
        std::ofstream create_file(data_file_path, std::ios::binary);
        create_file.close();
        data_file.open(data_file_path, std::ios::in | std::ios::out | std::ios::binary);
        // Reopen POSIX fd too if file didn't exist before
        if (data_fd < 0)
            data_fd = open(data_file_path.c_str(), O_RDWR, 0644);
    }

    // Find INT or DECIMAL primary key column for indexing
    const auto& schema = table->getSchema();
    for (size_t i = 0; i < schema.size(); i++) {
        if (schema[i].is_primary_key && (schema[i].type == DataType::INT || schema[i].type == DataType::DECIMAL)) {
            pk_col_idx = static_cast<int>(i);
            pk_index = std::make_unique<BTree>();
            break;
        }
    }
    // Note: BTree index is only created for explicitly declared PRIMARY KEY columns.
    // Tables without PK use full-scan for WHERE queries (no unnecessary RAM usage).

    // Scan file to rebuild row_count and BTree index.
    // fastSkipRow() is used when there is no PK index: it seeks past each row
    // using only stored lengths — zero heap allocations — so scanning a 300MB+
    // leftover file does NOT saturate RAM.
    data_file.seekg(0, std::ios::end);
    size_t file_size = data_file.tellg();
    data_file.seekg(0, std::ios::beg);

    const auto& schema_ref = table->getSchema();

    while (data_file.good() && static_cast<size_t>(data_file.tellg()) < file_size) {
        size_t current_offset = data_file.tellg();

        if (pk_index) {
            // PK table: only deserialize the PK column to extract the key
            // This avoids full row deserialization, saving memory for large tables
            bool is_deleted;
            if (!data_file.read(reinterpret_cast<char*>(&is_deleted), sizeof(bool))) break;
            time_t expiry_time;
            if (!data_file.read(reinterpret_cast<char*>(&expiry_time), sizeof(time_t))) break;

            if (!is_deleted && (expiry_time == 0 || std::time(nullptr) < expiry_time)) {
                // Read PK value and skip other columns
                bool read_pk_value = false;
                for (size_t i = 0; i < schema_ref.size(); i++) {
                    if (static_cast<int>(i) == pk_col_idx) {
                        // Read PK value
                        if (schema_ref[i].type == DataType::INT) {
                            int pk_val;
                            if (!data_file.read(reinterpret_cast<char*>(&pk_val), sizeof(int))) { read_pk_value = false; break; }
                            pk_index->insert(pk_val, current_offset);
                        } else if (schema_ref[i].type == DataType::DECIMAL) {
                            double pk_val_double;
                            if (!data_file.read(reinterpret_cast<char*>(&pk_val_double), sizeof(double))) { read_pk_value = false; break; }
                            pk_index->insert(static_cast<int>(pk_val_double), current_offset);
                        }
                        read_pk_value = true;
                    } else {
                        // Skip other column data
                        if (schema_ref[i].type == DataType::INT) {
                            data_file.seekg(sizeof(int), std::ios::cur);
                        } else if (schema_ref[i].type == DataType::DECIMAL) {
                            data_file.seekg(sizeof(double), std::ios::cur);
                        } else if (schema_ref[i].type == DataType::DATETIME) {
                            data_file.seekg(sizeof(time_t), std::ios::cur);
                        } else if (schema_ref[i].type == DataType::VARCHAR) {
                            size_t len = 0;
                            if (!data_file.read(reinterpret_cast<char*>(&len), sizeof(size_t))) { read_pk_value = false; break; }
                            if (len > 0) data_file.seekg(static_cast<std::streamoff>(len), std::ios::cur);
                        }
                    }
                    if (!data_file.good()) { read_pk_value = false; break; }
                }
                if (!read_pk_value) break; // Error reading PK or skipping data
            } else {
                // If row is deleted or expired, skip its data without indexing
                bool skip_ok = true;
                for (const auto& col : schema_ref) {
                    if (col.type == DataType::INT) {
                        data_file.seekg(sizeof(int), std::ios::cur);
                    } else if (col.type == DataType::DECIMAL) {
                        data_file.seekg(sizeof(double), std::ios::cur);
                    } else if (col.type == DataType::DATETIME) {
                        data_file.seekg(sizeof(time_t), std::ios::cur);
                    } else if (col.type == DataType::VARCHAR) {
                        size_t len = 0;
                        if (!data_file.read(reinterpret_cast<char*>(&len), sizeof(size_t))) { skip_ok = false; break; }
                        if (len > 0) data_file.seekg(static_cast<std::streamoff>(len), std::ios::cur);
                    }
                    if (!data_file.good()) { skip_ok = false; break; }
                }
                if (!skip_ok || !data_file.good()) break;
            }
        } else {
            // No PK: fast-skip each row with zero allocations.
            // Read deleted flag + expiry, then seek past each column.
            bool is_deleted;
            if (!data_file.read(reinterpret_cast<char*>(&is_deleted), sizeof(bool))) break;
            data_file.seekg(sizeof(time_t), std::ios::cur); // skip expiry_time

            bool skip_ok = true;
            for (const auto& col : schema_ref) {
                if (col.type == DataType::INT) {
                    data_file.seekg(sizeof(int), std::ios::cur);
                } else if (col.type == DataType::DECIMAL) {
                    data_file.seekg(sizeof(double), std::ios::cur);
                } else if (col.type == DataType::DATETIME) {
                    data_file.seekg(sizeof(time_t), std::ios::cur);
                } else if (col.type == DataType::VARCHAR) {
                    size_t len = 0;
                    if (!data_file.read(reinterpret_cast<char*>(&len), sizeof(size_t))) {
                        skip_ok = false; break;
                    }
                    if (len > 0) data_file.seekg(static_cast<std::streamoff>(len), std::ios::cur);
                }
                if (!data_file.good()) { skip_ok = false; break; }
            }
            if (!skip_ok || !data_file.good()) break;
        }
        row_count++;
    }
}

ColumnStore::~ColumnStore() {
    if (data_file.is_open()) {
        data_file.flush();
        data_file.close();
    }
    if (data_fd >= 0) {
        fdatasync(data_fd);
        close(data_fd);
        data_fd = -1;
    }
}

void ColumnStore::writeRowAtEnd(const Row& row, size_t& out_offset) {
    std::lock_guard<std::recursive_mutex> lock(file_mutex);
    data_file.clear(); // Clear EOF flags
    data_file.seekp(0, std::ios::end);
    out_offset = data_file.tellp();

    bool is_deleted = row.deleted;
    data_file.write(reinterpret_cast<const char*>(&is_deleted), sizeof(bool));
    data_file.write(reinterpret_cast<const char*>(&row.expiry_time), sizeof(time_t));

    const auto& schema = table->getSchema();
    for (size_t i = 0; i < schema.size(); i++) {
        if (i < row.values.size()) {
            const Value& v = row.values[i];
            if (schema[i].type == DataType::INT) {
                int out_val = (v.type == DataType::DECIMAL) ? static_cast<int>(v.data.decimal_val) : v.data.int_val;
                data_file.write(reinterpret_cast<const char*>(&out_val), sizeof(int));
            } else if (schema[i].type == DataType::DECIMAL) {
                double out_val = (v.type == DataType::INT) ? static_cast<double>(v.data.int_val) : v.data.decimal_val;
                data_file.write(reinterpret_cast<const char*>(&out_val), sizeof(double));
            } else if (schema[i].type == DataType::DATETIME) {
                data_file.write(reinterpret_cast<const char*>(&v.data.datetime_val), sizeof(time_t));
            } else if (schema[i].type == DataType::VARCHAR) {
                size_t len = v.data.varchar_val ? std::strlen(v.data.varchar_val) : 0;
                data_file.write(reinterpret_cast<const char*>(&len), sizeof(size_t));
                if (len > 0) {
                    data_file.write(v.data.varchar_val, len);
                }
            }
        } else {
            // Write defaults
            if (schema[i].type == DataType::INT) {
                int def = 0; data_file.write(reinterpret_cast<const char*>(&def), sizeof(int));
            } else if (schema[i].type == DataType::DECIMAL) {
                double def = 0.0; data_file.write(reinterpret_cast<const char*>(&def), sizeof(double));
            } else if (schema[i].type == DataType::DATETIME) {
                time_t def = 0; data_file.write(reinterpret_cast<const char*>(&def), sizeof(time_t));
            } else if (schema[i].type == DataType::VARCHAR) {
                size_t def = 0; data_file.write(reinterpret_cast<const char*>(&def), sizeof(size_t));
            }
        }
    }
}

Row ColumnStore::readRowAtOffset(size_t offset) const {
    Row row;
    std::lock_guard<std::recursive_mutex> lock(file_mutex);
    data_file.clear();
    data_file.seekg(offset, std::ios::beg);

    bool is_deleted;
    if (!data_file.read(reinterpret_cast<char*>(&is_deleted), sizeof(bool))) {
        row.deleted = true;
        return row;
    }
    row.deleted = is_deleted;

    data_file.read(reinterpret_cast<char*>(&row.expiry_time), sizeof(time_t));

    const auto& schema = table->getSchema();
    for (size_t i = 0; i < schema.size(); i++) {
        if (schema[i].type == DataType::INT) {
            int val; data_file.read(reinterpret_cast<char*>(&val), sizeof(int));
            row.values.push_back(Value(val));
        } else if (schema[i].type == DataType::DECIMAL) {
            double val; data_file.read(reinterpret_cast<char*>(&val), sizeof(double));
            row.values.push_back(Value(val));
        } else if (schema[i].type == DataType::DATETIME) {
            time_t val; data_file.read(reinterpret_cast<char*>(&val), sizeof(time_t));
            row.values.push_back(Value(val));
        } else if (schema[i].type == DataType::VARCHAR) {
            size_t len; data_file.read(reinterpret_cast<char*>(&len), sizeof(size_t));
            if (len > 0) {
                std::string s(len, '\0');
                data_file.read(&s[0], len);
                row.values.push_back(Value(s));
            } else {
                row.values.push_back(Value(""));
            }
        }
    }

    // Check expiry
    if (!row.deleted && row.expiry_time > 0 && std::time(nullptr) >= row.expiry_time) {
        row.deleted = true;
    }
    row.file_offset = offset;
    return row;
}

void ColumnStore::insertRow(const Row& row) {
    size_t offset = 0;
    writeRowAtEnd(row, offset);
    row_count++;

    // Update B-Tree index on primary key
    if (pk_index && pk_col_idx >= 0 && pk_col_idx < static_cast<int>(row.values.size())) {
        if (row.values[pk_col_idx].type == DataType::INT) {
            pk_index->insert(row.values[pk_col_idx].data.int_val, offset);
        } else if (row.values[pk_col_idx].type == DataType::DECIMAL) {
            pk_index->insert(static_cast<int>(row.values[pk_col_idx].data.decimal_val), offset);
        }
    }
}

void ColumnStore::insertBatch(std::vector<Row>& rows) {
    if (rows.empty()) return;

    const auto& schema = table->getSchema();

    // Pre-build write buffer: serialize all rows into a contiguous byte vector
    std::vector<char> write_buf;
    // Reserve a rough estimate: (1 + 8 + 4*5cols) * rows ~ 50 bytes per row
    write_buf.reserve(rows.size() * 80);

    // Track per-row offsets for BTree indexing
    std::vector<size_t> row_offsets;
    row_offsets.reserve(rows.size());

    // Single lock for entire batch
    std::lock_guard<std::recursive_mutex> lock(file_mutex);
    data_file.clear();
    data_file.seekp(0, std::ios::end);
    size_t base_offset = data_file.tellp();

    for (auto& row : rows) {
        size_t row_start = base_offset + write_buf.size();
        row_offsets.push_back(row_start);

        // Serialize: deleted flag
        bool is_deleted = row.deleted;
        write_buf.insert(write_buf.end(),
            reinterpret_cast<const char*>(&is_deleted),
            reinterpret_cast<const char*>(&is_deleted) + sizeof(bool));

        // Serialize: expiry_time
        write_buf.insert(write_buf.end(),
            reinterpret_cast<const char*>(&row.expiry_time),
            reinterpret_cast<const char*>(&row.expiry_time) + sizeof(time_t));

        // Serialize each column value
        for (size_t i = 0; i < schema.size(); i++) {
            if (i < row.values.size()) {
                const Value& v = row.values[i];
                if (schema[i].type == DataType::INT) {
                    int out_val = (v.type == DataType::DECIMAL) ? static_cast<int>(v.data.decimal_val) : v.data.int_val;
                    write_buf.insert(write_buf.end(),
                        reinterpret_cast<const char*>(&out_val),
                        reinterpret_cast<const char*>(&out_val) + sizeof(int));
                } else if (schema[i].type == DataType::DECIMAL) {
                    double out_val = (v.type == DataType::INT) ? static_cast<double>(v.data.int_val) : v.data.decimal_val;
                    write_buf.insert(write_buf.end(),
                        reinterpret_cast<const char*>(&out_val),
                        reinterpret_cast<const char*>(&out_val) + sizeof(double));
                } else if (schema[i].type == DataType::DATETIME) {
                    write_buf.insert(write_buf.end(),
                        reinterpret_cast<const char*>(&v.data.datetime_val),
                        reinterpret_cast<const char*>(&v.data.datetime_val) + sizeof(time_t));
                } else if (schema[i].type == DataType::VARCHAR) {
                    size_t len = v.data.varchar_val ? std::strlen(v.data.varchar_val) : 0;
                    write_buf.insert(write_buf.end(),
                        reinterpret_cast<const char*>(&len),
                        reinterpret_cast<const char*>(&len) + sizeof(size_t));
                    if (len > 0) {
                        write_buf.insert(write_buf.end(), v.data.varchar_val, v.data.varchar_val + len);
                    }
                }
            } else {
                // Write defaults
                if (schema[i].type == DataType::INT) {
                    int def = 0;
                    write_buf.insert(write_buf.end(), reinterpret_cast<const char*>(&def), reinterpret_cast<const char*>(&def) + sizeof(int));
                } else if (schema[i].type == DataType::DECIMAL) {
                    double def = 0.0;
                    write_buf.insert(write_buf.end(), reinterpret_cast<const char*>(&def), reinterpret_cast<const char*>(&def) + sizeof(double));
                } else if (schema[i].type == DataType::DATETIME) {
                    time_t def = 0;
                    write_buf.insert(write_buf.end(), reinterpret_cast<const char*>(&def), reinterpret_cast<const char*>(&def) + sizeof(time_t));
                } else if (schema[i].type == DataType::VARCHAR) {
                    size_t def = 0;
                    write_buf.insert(write_buf.end(), reinterpret_cast<const char*>(&def), reinterpret_cast<const char*>(&def) + sizeof(size_t));
                }
            }
        }
    }

    // Single write for entire batch
    data_file.write(write_buf.data(), write_buf.size());

    // Update BTree index for all rows
    if (pk_index && pk_col_idx >= 0) {
        for (size_t r = 0; r < rows.size(); r++) {
            if (pk_col_idx < static_cast<int>(rows[r].values.size())) {
                if (rows[r].values[pk_col_idx].type == DataType::INT) {
                    pk_index->insert(rows[r].values[pk_col_idx].data.int_val, row_offsets[r]);
                } else if (rows[r].values[pk_col_idx].type == DataType::DECIMAL) {
                    pk_index->insert(static_cast<int>(rows[r].values[pk_col_idx].data.decimal_val), row_offsets[r]);
                }
            }
        }
    }

    row_count += rows.size();
}

void ColumnStore::insertBatchFlat(const std::vector<Value>& flat_values, size_t num_columns, time_t expiry) {
    if (flat_values.empty() || num_columns == 0) return;

    size_t num_rows = flat_values.size() / num_columns;
    const auto& schema = table->getSchema();

    // Pre-build write buffer
    std::vector<char> write_buf;
    write_buf.reserve(num_rows * 80);

    // Track per-row offsets for BTree indexing
    std::vector<size_t> row_offsets;
    row_offsets.reserve(num_rows);

    // Single lock for entire batch
    std::lock_guard<std::recursive_mutex> lock(file_mutex);
    data_file.clear();
    data_file.seekp(0, std::ios::end);
    size_t base_offset = data_file.tellp();

    for (size_t r = 0; r < num_rows; r++) {
        size_t row_start = base_offset + write_buf.size();
        row_offsets.push_back(row_start);

        // Serialize: deleted flag
        bool is_deleted = false;
        write_buf.insert(write_buf.end(), reinterpret_cast<const char*>(&is_deleted), reinterpret_cast<const char*>(&is_deleted) + sizeof(bool));

        // Serialize: expiry_time
        write_buf.insert(write_buf.end(), reinterpret_cast<const char*>(&expiry), reinterpret_cast<const char*>(&expiry) + sizeof(time_t));

        // Serialize each column value
        for (size_t col = 0; col < schema.size(); col++) {
            if (col < num_columns) {
                const Value& v = flat_values[r * num_columns + col];
                if (schema[col].type == DataType::INT) {
                    int out_val = (v.type == DataType::DECIMAL) ? static_cast<int>(v.data.decimal_val) : v.data.int_val;
                    write_buf.insert(write_buf.end(), reinterpret_cast<const char*>(&out_val), reinterpret_cast<const char*>(&out_val) + sizeof(int));
                } else if (schema[col].type == DataType::DECIMAL) {
                    double out_val = (v.type == DataType::INT) ? static_cast<double>(v.data.int_val) : v.data.decimal_val;
                    write_buf.insert(write_buf.end(), reinterpret_cast<const char*>(&out_val), reinterpret_cast<const char*>(&out_val) + sizeof(double));
                } else if (schema[col].type == DataType::DATETIME) {
                    write_buf.insert(write_buf.end(), reinterpret_cast<const char*>(&v.data.datetime_val), reinterpret_cast<const char*>(&v.data.datetime_val) + sizeof(time_t));
                } else if (schema[col].type == DataType::VARCHAR) {
                    size_t len = (v.type == DataType::VARCHAR && v.data.varchar_val) ? std::strlen(v.data.varchar_val) : 0;
                    write_buf.insert(write_buf.end(), reinterpret_cast<const char*>(&len), reinterpret_cast<const char*>(&len) + sizeof(size_t));
                    if (len > 0) {
                        write_buf.insert(write_buf.end(), v.data.varchar_val, v.data.varchar_val + len);
                    }
                }
            } else {
                // Default values
                if (schema[col].type == DataType::INT) {
                    int def = 0; write_buf.insert(write_buf.end(), reinterpret_cast<const char*>(&def), reinterpret_cast<const char*>(&def) + sizeof(int));
                } else if (schema[col].type == DataType::DECIMAL) {
                    double def = 0.0; write_buf.insert(write_buf.end(), reinterpret_cast<const char*>(&def), reinterpret_cast<const char*>(&def) + sizeof(double));
                } else if (schema[col].type == DataType::DATETIME) {
                    time_t def = 0; write_buf.insert(write_buf.end(), reinterpret_cast<const char*>(&def), reinterpret_cast<const char*>(&def) + sizeof(time_t));
                } else if (schema[col].type == DataType::VARCHAR) {
                    size_t def = 0; write_buf.insert(write_buf.end(), reinterpret_cast<const char*>(&def), reinterpret_cast<const char*>(&def) + sizeof(size_t));
                }
            }
        }
    }

    // Single write for entire batch
    data_file.write(write_buf.data(), write_buf.size());

    // Update BTree index
    if (pk_index && pk_col_idx >= 0 && pk_col_idx < static_cast<int>(num_columns)) {
        for (size_t r = 0; r < num_rows; r++) {
            const Value& v = flat_values[r * num_columns + pk_col_idx];
            if (v.type == DataType::INT) {
                pk_index->insert(v.data.int_val, row_offsets[r]);
            } else if (v.type == DataType::DECIMAL) {
                pk_index->insert(static_cast<int>(v.data.decimal_val), row_offsets[r]);
            }
        }
    }

    row_count += num_rows;
}

Row ColumnStore::getRow(size_t offset) const {
    return readRowAtOffset(offset);
}

std::vector<Row> ColumnStore::getAllRows() const {
    std::vector<Row> result;
    std::lock_guard<std::recursive_mutex> lock(file_mutex);
    data_file.clear();
    data_file.seekg(0, std::ios::end);
    size_t file_size = data_file.tellg();
    
    size_t current_offset = 0;
    while (current_offset < file_size) {
        Row row = readRowAtOffset(current_offset);
        if (!row.deleted) {
            result.push_back(std::move(row));
        }
        // Advance current_offset by querying the file pointer because variable lengths exist
        data_file.clear();
        current_offset = data_file.tellg();
    }
    return result;
}

std::vector<Row> ColumnStore::getRowsByColumnValue(size_t col_idx, const Value& v) const {
    std::vector<Row> result;
    std::lock_guard<std::recursive_mutex> lock(file_mutex);
    data_file.clear();
    data_file.seekg(0, std::ios::end);
    size_t file_size = data_file.tellg();
    
    size_t current_offset = 0;
    while (current_offset < file_size) {
        Row row = readRowAtOffset(current_offset);
        if (!row.deleted && col_idx < row.values.size() && compareValues(row.values[col_idx], v)) {
            result.push_back(std::move(row));
        }
        data_file.clear();
        current_offset = data_file.tellg();
    }
    return result;
}

std::vector<Row> ColumnStore::getRowsByColumnRange(size_t col_idx, const Value& start, const Value& end) const {
    std::vector<Row> result;
    std::lock_guard<std::recursive_mutex> lock(file_mutex);
    data_file.clear();
    data_file.seekg(0, std::ios::end);
    size_t file_size = data_file.tellg();
    
    size_t current_offset = 0;
    while (current_offset < file_size) {
        Row row = readRowAtOffset(current_offset);
        if (!row.deleted && col_idx < row.values.size() && 
            !lessThan(row.values[col_idx], start) && 
            !greaterThan(row.values[col_idx], end)) {
            result.push_back(std::move(row));
        }
        data_file.clear();
        current_offset = data_file.tellg();
    }
    return result;
}

Row ColumnStore::getRowByPK(int pk_value) const {
    if (!pk_index) {
        Row empty; empty.deleted = true;
        return empty;
    }
    bool found = false;
    size_t offset = pk_index->search(pk_value, found);
    if (!found) {
        Row empty; empty.deleted = true;
        return empty;
    }
    return readRowAtOffset(offset);
}

std::vector<size_t> ColumnStore::getPKRange(int lo, int hi) const {
    if (!pk_index) return {};
    return pk_index->rangeSearch(lo, hi);
}

void ColumnStore::deleteRow(size_t offset) {
    Row row = readRowAtOffset(offset);
    if (!row.deleted) {
        // Remove from B-Tree index
        if (pk_index && pk_col_idx >= 0 && pk_col_idx < static_cast<int>(row.values.size())) {
            int pk_val = 0;
            if (row.values[pk_col_idx].type == DataType::INT) pk_val = row.values[pk_col_idx].data.int_val;
            else if (row.values[pk_col_idx].type == DataType::DECIMAL) pk_val = static_cast<int>(row.values[pk_col_idx].data.decimal_val);
            pk_index->remove(pk_val);
        }

        std::lock_guard<std::recursive_mutex> lock(file_mutex);
        data_file.clear();
        data_file.seekp(offset, std::ios::beg);
        bool deleted = true;
        data_file.write(reinterpret_cast<const char*>(&deleted), sizeof(bool));
    }
}

void ColumnStore::deleteExpiredRows() {
    std::lock_guard<std::recursive_mutex> lock(file_mutex);
    data_file.clear();
    data_file.seekg(0, std::ios::end);
    size_t file_size = data_file.tellg();
    
    size_t current_offset = 0;
    while (current_offset < file_size) {
        Row row = readRowAtOffset(current_offset);
        if (!row.deleted && row.expiry_time > 0 && std::time(nullptr) >= row.expiry_time) {
            // Remove from B-Tree index
            if (pk_index && pk_col_idx >= 0 && pk_col_idx < static_cast<int>(row.values.size())) {
                int pk_val = 0;
                if (row.values[pk_col_idx].type == DataType::INT) pk_val = row.values[pk_col_idx].data.int_val;
                else if (row.values[pk_col_idx].type == DataType::DECIMAL) pk_val = static_cast<int>(row.values[pk_col_idx].data.decimal_val);
                pk_index->remove(pk_val);
            }
            
            data_file.clear();
            data_file.seekp(current_offset, std::ios::beg);
            bool deleted = true;
            data_file.write(reinterpret_cast<const char*>(&deleted), sizeof(bool));
        }
        data_file.clear(); // readRowAtOffset moves the pointer, so tellg is fine
        current_offset = data_file.tellg();
    }
}

long long ColumnStore::getMemoryUsageBytes() const {
    // The data is all on disk now!
    // We only hold the B-Tree in memory.
    long long btree_size = row_count * 24; // Approximation
    return btree_size + 1024; // Base overhead
}

void ColumnStore::flush() {
    std::lock_guard<std::recursive_mutex> lock(file_mutex);
    data_file.flush();

    flush_count++;
    // Force dirty pages to disk based on flush interval for fault tolerance.
    // Always fdatasync if flush_interval_batches is 1, or if it's the Nth flush.
    if (data_fd >= 0 && (flush_interval_batches == 1 || flush_count % flush_interval_batches == 0)) {
        fdatasync(data_fd);
    }
}

} // namespace flexql
