#include "../../include/storage/row_store.h"
#include <ctime>
#include <cstring>
#include <fcntl.h>
#include <unistd.h>

namespace flexql {

RowStore::RowStore(std::shared_ptr<Table> t)
    : table(t), row_count(0), data_fd(-1), flush_count(0),
      flush_interval_batches(DEFAULT_FLUSH_INTERVAL_BATCHES), pk_col_idx(-1)
{
    data_file_path = "data/tables/" + t->getName() + ".dat";

    data_fd = open(data_file_path.c_str(), O_RDWR | O_CREAT, 0644);
    data_file.open(data_file_path, std::ios::in | std::ios::out | std::ios::binary);
    if (!data_file.is_open()) {
        std::ofstream create_file(data_file_path, std::ios::binary);
        create_file.close();
        data_file.open(data_file_path, std::ios::in | std::ios::out | std::ios::binary);
        if (data_fd < 0) {
            data_fd = open(data_file_path.c_str(), O_RDWR, 0644);
        }
    }

    const auto& schema = table->getSchema();
    for (size_t i = 0; i < schema.size(); i++) {
        if (schema[i].is_primary_key &&
            (schema[i].type == DataType::INT || schema[i].type == DataType::DECIMAL)) {
            pk_col_idx = static_cast<int>(i);
            pk_index = std::make_unique<BTree>();
            break;
        }
    }

    data_file.seekg(0, std::ios::end);
    std::streamoff file_end = data_file.tellg();
    if (file_end <= 0) return;
    size_t file_size = static_cast<size_t>(file_end);
    data_file.seekg(0, std::ios::beg);

    while (data_file.good() && static_cast<size_t>(data_file.tellg()) < file_size) {
        size_t current_offset = static_cast<size_t>(data_file.tellg());

        bool is_deleted = false;
        if (!data_file.read(reinterpret_cast<char*>(&is_deleted), sizeof(bool))) break;
        time_t expiry_time = 0;
        if (!data_file.read(reinterpret_cast<char*>(&expiry_time), sizeof(time_t))) break;

        bool alive = !is_deleted && (expiry_time == 0 || std::time(nullptr) < expiry_time);

        if (pk_index && alive) {
            for (size_t i = 0; i < schema.size(); i++) {
                if (static_cast<int>(i) == pk_col_idx) {
                    if (schema[i].type == DataType::INT) {
                        int pk_val = 0;
                        if (!data_file.read(reinterpret_cast<char*>(&pk_val), sizeof(int))) return;
                        pk_index->insert(pk_val, current_offset);
                    } else {
                        double pk_val = 0;
                        if (!data_file.read(reinterpret_cast<char*>(&pk_val), sizeof(double))) return;
                        pk_index->insert(static_cast<int>(pk_val), current_offset);
                    }
                } else {
                    skipColumn(schema[i]);
                }
                if (!data_file.good()) return;
            }
        } else {
            for (const auto& col : schema) {
                skipColumn(col);
                if (!data_file.good()) return;
            }
        }
        row_count++;
    }
}

RowStore::~RowStore() {
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

void RowStore::skipColumn(const Column& col) const {
    if (col.type == DataType::INT) {
        data_file.seekg(sizeof(int), std::ios::cur);
    } else if (col.type == DataType::DECIMAL) {
        data_file.seekg(sizeof(double), std::ios::cur);
    } else if (col.type == DataType::DATETIME) {
        data_file.seekg(sizeof(time_t), std::ios::cur);
    } else if (col.type == DataType::VARCHAR) {
        size_t len = 0;
        if (!data_file.read(reinterpret_cast<char*>(&len), sizeof(size_t))) return;
        if (len > 0) data_file.seekg(static_cast<std::streamoff>(len), std::ios::cur);
    }
}

void RowStore::appendValue(std::vector<char>& buf, const Column& col, const Value* v) const {
    auto append = [&](const void* p, size_t n) {
        const char* c = static_cast<const char*>(p);
        buf.insert(buf.end(), c, c + n);
    };

    if (!v) {
        if (col.type == DataType::INT) {
            int def = 0; append(&def, sizeof(def));
        } else if (col.type == DataType::DECIMAL) {
            double def = 0.0; append(&def, sizeof(def));
        } else if (col.type == DataType::DATETIME) {
            time_t def = 0; append(&def, sizeof(def));
        } else {
            size_t def = 0; append(&def, sizeof(def));
        }
        return;
    }

    if (col.type == DataType::INT) {
        int out_val = (v->type == DataType::DECIMAL)
                          ? static_cast<int>(v->data.decimal_val)
                          : v->data.int_val;
        append(&out_val, sizeof(out_val));
    } else if (col.type == DataType::DECIMAL) {
        double out_val = (v->type == DataType::INT)
                             ? static_cast<double>(v->data.int_val)
                             : v->data.decimal_val;
        append(&out_val, sizeof(out_val));
    } else if (col.type == DataType::DATETIME) {
        append(&v->data.datetime_val, sizeof(time_t));
    } else if (col.type == DataType::VARCHAR) {
        size_t len = (v->type == DataType::VARCHAR && v->data.varchar_val)
                         ? std::strlen(v->data.varchar_val)
                         : 0;
        append(&len, sizeof(len));
        if (len > 0) append(v->data.varchar_val, len);
    }
}

void RowStore::writeRowAtEnd(const Row& row, size_t& out_offset) {
    std::lock_guard<std::recursive_mutex> lock(file_mutex);
    data_file.clear();
    data_file.seekp(0, std::ios::end);
    out_offset = static_cast<size_t>(data_file.tellp());

    std::vector<char> buf;
    bool is_deleted = row.deleted;
    buf.insert(buf.end(), reinterpret_cast<const char*>(&is_deleted),
               reinterpret_cast<const char*>(&is_deleted) + sizeof(bool));
    buf.insert(buf.end(), reinterpret_cast<const char*>(&row.expiry_time),
               reinterpret_cast<const char*>(&row.expiry_time) + sizeof(time_t));

    const auto& schema = table->getSchema();
    for (size_t i = 0; i < schema.size(); i++) {
        const Value* v = (i < row.values.size()) ? &row.values[i] : nullptr;
        appendValue(buf, schema[i], v);
    }
    data_file.write(buf.data(), static_cast<std::streamsize>(buf.size()));
}

Row RowStore::readRowAtOffset(size_t offset) const {
    Row row;
    std::lock_guard<std::recursive_mutex> lock(file_mutex);
    data_file.clear();
    data_file.seekg(static_cast<std::streamoff>(offset), std::ios::beg);

    bool is_deleted = false;
    if (!data_file.read(reinterpret_cast<char*>(&is_deleted), sizeof(bool))) {
        row.deleted = true;
        return row;
    }
    row.deleted = is_deleted;
    data_file.read(reinterpret_cast<char*>(&row.expiry_time), sizeof(time_t));

    const auto& schema = table->getSchema();
    for (const auto& col : schema) {
        if (col.type == DataType::INT) {
            int val = 0;
            data_file.read(reinterpret_cast<char*>(&val), sizeof(int));
            row.values.push_back(Value(val));
        } else if (col.type == DataType::DECIMAL) {
            double val = 0;
            data_file.read(reinterpret_cast<char*>(&val), sizeof(double));
            row.values.push_back(Value(val));
        } else if (col.type == DataType::DATETIME) {
            time_t val = 0;
            data_file.read(reinterpret_cast<char*>(&val), sizeof(time_t));
            row.values.push_back(Value(val));
        } else if (col.type == DataType::VARCHAR) {
            size_t len = 0;
            data_file.read(reinterpret_cast<char*>(&len), sizeof(size_t));
            if (len > 0) {
                std::string s(len, '\0');
                data_file.read(&s[0], static_cast<std::streamsize>(len));
                row.values.push_back(Value(s));
            } else {
                row.values.push_back(Value(""));
            }
        }
    }

    if (!row.deleted && row.expiry_time > 0 && std::time(nullptr) >= row.expiry_time) {
        row.deleted = true;
    }
    row.file_offset = offset;
    return row;
}

void RowStore::insertRow(const Row& row) {
    size_t offset = 0;
    writeRowAtEnd(row, offset);
    row_count++;

    if (pk_index && pk_col_idx >= 0 && pk_col_idx < static_cast<int>(row.values.size())) {
        const Value& v = row.values[pk_col_idx];
        if (v.type == DataType::INT) {
            pk_index->insert(v.data.int_val, offset);
        } else if (v.type == DataType::DECIMAL) {
            pk_index->insert(static_cast<int>(v.data.decimal_val), offset);
        }
    }
}

void RowStore::insertBatchFlat(const std::vector<Value>& flat_values, size_t num_columns, time_t expiry) {
    if (flat_values.empty() || num_columns == 0) return;

    size_t num_rows = flat_values.size() / num_columns;
    const auto& schema = table->getSchema();

    std::vector<char> write_buf;
    write_buf.reserve(num_rows * 80);
    std::vector<size_t> row_offsets;
    row_offsets.reserve(num_rows);

    std::lock_guard<std::recursive_mutex> lock(file_mutex);
    data_file.clear();
    data_file.seekp(0, std::ios::end);
    size_t base_offset = static_cast<size_t>(data_file.tellp());

    for (size_t r = 0; r < num_rows; r++) {
        row_offsets.push_back(base_offset + write_buf.size());

        bool is_deleted = false;
        write_buf.insert(write_buf.end(), reinterpret_cast<const char*>(&is_deleted),
                         reinterpret_cast<const char*>(&is_deleted) + sizeof(bool));
        write_buf.insert(write_buf.end(), reinterpret_cast<const char*>(&expiry),
                         reinterpret_cast<const char*>(&expiry) + sizeof(time_t));

        for (size_t col = 0; col < schema.size(); col++) {
            const Value* v = (col < num_columns) ? &flat_values[r * num_columns + col] : nullptr;
            appendValue(write_buf, schema[col], v);
        }
    }

    data_file.write(write_buf.data(), static_cast<std::streamsize>(write_buf.size()));

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

Row RowStore::getRow(size_t offset) const {
    return readRowAtOffset(offset);
}

std::vector<Row> RowStore::getAllRows() const {
    std::vector<Row> result;
    std::lock_guard<std::recursive_mutex> lock(file_mutex);
    data_file.clear();
    data_file.seekg(0, std::ios::end);
    std::streamoff file_end = data_file.tellg();
    if (file_end <= 0) return result;
    size_t file_size = static_cast<size_t>(file_end);

    size_t current_offset = 0;
    while (current_offset < file_size) {
        Row row = readRowAtOffset(current_offset);
        if (!row.deleted) result.push_back(std::move(row));
        data_file.clear();
        std::streamoff next = data_file.tellg();
        if (next < 0 || static_cast<size_t>(next) <= current_offset) break;
        current_offset = static_cast<size_t>(next);
    }
    return result;
}

Row RowStore::getRowByPK(int pk_value) const {
    if (!pk_index) {
        Row empty;
        empty.deleted = true;
        return empty;
    }
    bool found = false;
    size_t offset = pk_index->search(pk_value, found);
    if (!found) {
        Row empty;
        empty.deleted = true;
        return empty;
    }
    return readRowAtOffset(offset);
}

std::vector<size_t> RowStore::getPKRange(int lo, int hi) const {
    if (!pk_index) return {};
    return pk_index->rangeSearch(lo, hi);
}

void RowStore::deleteRow(size_t offset) {
    Row row = readRowAtOffset(offset);
    if (row.deleted) return;

    if (pk_index && pk_col_idx >= 0 && pk_col_idx < static_cast<int>(row.values.size())) {
        int pk_val = 0;
        if (row.values[pk_col_idx].type == DataType::INT) {
            pk_val = row.values[pk_col_idx].data.int_val;
        } else if (row.values[pk_col_idx].type == DataType::DECIMAL) {
            pk_val = static_cast<int>(row.values[pk_col_idx].data.decimal_val);
        }
        pk_index->remove(pk_val);
    }

    std::lock_guard<std::recursive_mutex> lock(file_mutex);
    data_file.clear();
    data_file.seekp(static_cast<std::streamoff>(offset), std::ios::beg);
    bool deleted = true;
    data_file.write(reinterpret_cast<const char*>(&deleted), sizeof(bool));
}

void RowStore::deleteExpiredRows() {
    std::lock_guard<std::recursive_mutex> lock(file_mutex);
    data_file.clear();
    data_file.seekg(0, std::ios::end);
    std::streamoff file_end = data_file.tellg();
    if (file_end <= 0) return;
    size_t file_size = static_cast<size_t>(file_end);

    size_t current_offset = 0;
    while (current_offset < file_size) {
        Row row = readRowAtOffset(current_offset);
        if (!row.deleted && row.expiry_time > 0 && std::time(nullptr) >= row.expiry_time) {
            if (pk_index && pk_col_idx >= 0 && pk_col_idx < static_cast<int>(row.values.size())) {
                int pk_val = 0;
                if (row.values[pk_col_idx].type == DataType::INT) {
                    pk_val = row.values[pk_col_idx].data.int_val;
                } else if (row.values[pk_col_idx].type == DataType::DECIMAL) {
                    pk_val = static_cast<int>(row.values[pk_col_idx].data.decimal_val);
                }
                pk_index->remove(pk_val);
            }
            data_file.clear();
            data_file.seekp(static_cast<std::streamoff>(current_offset), std::ios::beg);
            bool deleted = true;
            data_file.write(reinterpret_cast<const char*>(&deleted), sizeof(bool));
        }
        data_file.clear();
        std::streamoff next = data_file.tellg();
        if (next < 0 || static_cast<size_t>(next) <= current_offset) break;
        current_offset = static_cast<size_t>(next);
    }
}

long long RowStore::getMemoryUsageBytes() const {
    // Rows live on disk. RAM is the B-tree (~24 bytes per key) plus a little overhead.
    return static_cast<long long>(row_count) * 24 + 1024;
}

void RowStore::flush() {
    std::lock_guard<std::recursive_mutex> lock(file_mutex);
    data_file.flush();

    flush_count++;
    if (data_fd >= 0 && (flush_interval_batches == 1 || flush_count % flush_interval_batches == 0)) {
        fdatasync(data_fd);
        posix_fadvise(data_fd, 0, 0, POSIX_FADV_DONTNEED);
    }
}

} // namespace flexql
