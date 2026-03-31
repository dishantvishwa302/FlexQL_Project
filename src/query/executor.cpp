#include "../../include/query/executor.h"
#include "../../include/parser/parser.h"
#include "../../include/storage/database.h"
#include <chrono>
#include <ctime>
#include <algorithm>
#include <sstream>
#include <unordered_map>

namespace flexql {

QueryExecutor::QueryExecutor(std::shared_ptr<Database> db,
                             std::shared_ptr<LRUCache> lru)
    : database(db), cache(lru) {}

// ============ DISPATCH ============

QueryResult QueryExecutor::execute(const std::string& sql) {
    QueryResult result;
    result.success = true;
    result.stats.cache_hit = false;

    try {
        std::string upper_sql = sql;
        std::transform(upper_sql.begin(), upper_sql.end(), upper_sql.begin(), ::toupper);

        // Detect INNER JOIN before plain SELECT
        if (upper_sql.find("INNER") != std::string::npos &&
            upper_sql.find("JOIN") != std::string::npos)
        {
            Parser parser(sql);
            auto stmt = parser.parseJoin();
            if (parser.hasError()) {
                result.success = false;
                result.error_message = parser.getError();
                return result;
            }
            if (stmt) return executeJoin(*stmt);
        } else if (upper_sql.find("SELECT") != std::string::npos) {
            Parser parser(sql);
            auto stmt = parser.parseSelect();
            if (parser.hasError()) {
                result.success = false;
                result.error_message = parser.getError();
                return result;
            }
            if (stmt) return executeSelect(*stmt);
        } else if (upper_sql.find("INSERT") != std::string::npos) {
            Parser parser(sql);
            auto stmt = parser.parseInsert();
            if (parser.hasError()) {
                result.success = false;
                result.error_message = parser.getError();
                return result;
            }
            if (stmt) return executeInsert(*stmt);
        } else if (upper_sql.find("CREATE") != std::string::npos) {
            Parser parser(sql);
            auto stmt = parser.parseCreateTable();
            if (parser.hasError()) {
                result.success = false;
                result.error_message = parser.getError();
                return result;
            }
            if (stmt) return executeCreateTable(*stmt);
        } else if (upper_sql.find("DELETE") != std::string::npos) {
            Parser parser(sql);
            auto stmt = parser.parseDelete();
            if (parser.hasError()) {
                result.success = false;
                result.error_message = parser.getError();
                return result;
            }
            if (stmt) return executeDelete(*stmt);
        } else {
            result.success = false;
            result.error_message = "Unknown SQL statement";
        }

    } catch (const std::exception& e) {
        result.success = false;
        result.error_message = std::string("Exception: ") + e.what();
    }

    return result;
}

// ============ SELECT ============

QueryResult QueryExecutor::executeSelect(const SelectStatement& stmt) {
    auto start = std::chrono::high_resolution_clock::now();

    QueryResult result;
    result.success = true;
    result.stats.cache_hit = false;
    result.stats.rows_scanned = 0;
    result.stats.rows_returned = 0;

    // --- Cache check ---
    if (cache) {
        std::string cache_key = "SELECT:" + stmt.table_name;
        
        cache_key += ":C:";
        if (stmt.columns.empty()) {
            cache_key += "*";
        } else {
            for (const auto& c : stmt.columns) cache_key += c + ",";
        }
        
        if (stmt.has_where)
            cache_key += ":W:" + stmt.where.column_name + std::to_string((int)stmt.where.op) +
                         stmt.where.value.toString();
        
        if (stmt.has_order_by)
            cache_key += ":O:" + stmt.order_by.column_name + (stmt.order_by.is_desc ? "DESC" : "ASC");
                         
        std::string cached_val;
        if (cache->get(cache_key, cached_val)) {
            result.stats.cache_hit = true;
            auto end = std::chrono::high_resolution_clock::now();
            result.stats.execution_time_ms =
                std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
            // Deserialise the cached result (packed as row\tcol|col\n)
            std::istringstream ss(cached_val);
            std::string line;
            while (std::getline(ss, line)) {
                if (line.empty()) continue;
                ResultRow res_row;
                size_t tab = line.find('\t');
                if (tab == std::string::npos) continue;
                std::string names_part = line.substr(0, tab);
                std::string vals_part  = line.substr(tab + 1);
                // Split on |
                auto split_pipe = [](const std::string& s) {
                    std::vector<std::string> out;
                    size_t start = 0, pos;
                    while ((pos = s.find('|', start)) != std::string::npos) {
                        out.push_back(s.substr(start, pos - start));
                        start = pos + 1;
                    }
                    out.push_back(s.substr(start));
                    return out;
                };
                res_row.column_names = split_pipe(names_part);
                res_row.values       = split_pipe(vals_part);
                result.rows.push_back(std::move(res_row));
            }
            result.stats.rows_returned = result.rows.size();
            return result;
        }
    }

    try {
        auto table        = database->getTable(stmt.table_name);
        auto column_store = database->getColumnStore(stmt.table_name);

        // Pre-validate select columns
        if (!stmt.columns.empty()) {
            for (const auto& col_name : stmt.columns) {
                if (table->getColumnIndex(col_name) < 0) {
                    result.success = false;
                    result.error_message = "Unknown column: " + col_name;
                    return result;
                }
            }
        }

        std::vector<Row> matching_rows;

        if (stmt.has_where) {
            int col_idx = table->getColumnIndex(stmt.where.column_name);
            if (col_idx < 0) {
                result.success = false;
                result.error_message = "Column not found: " + stmt.where.column_name;
                return result;
            }

            // --- B-Tree index fast path (PK column, INT) ---
            bool used_index = false;
            if (column_store->hasPKIndex() && col_idx == column_store->getPKColIdx() &&
                stmt.where.value.type == DataType::INT)
            {
                int target = stmt.where.value.data.int_val;
                if (stmt.where.op == ComparisonOp::EQ) {
                    Row row = column_store->getRowByPK(target);
                    result.stats.rows_scanned = 1;
                    if (!row.deleted) matching_rows.push_back(std::move(row));
                    used_index = true;
                } else if (stmt.where.op == ComparisonOp::LT ||
                           stmt.where.op == ComparisonOp::LE ||
                           stmt.where.op == ComparisonOp::GT ||
                           stmt.where.op == ComparisonOp::GE)
                {
                    int lo = INT32_MIN, hi = INT32_MAX;
                    if (stmt.where.op == ComparisonOp::LT) hi = target - 1;
                    else if (stmt.where.op == ComparisonOp::LE) hi = target;
                    else if (stmt.where.op == ComparisonOp::GT) lo = target + 1;
                    else if (stmt.where.op == ComparisonOp::GE) lo = target;
                    auto row_ids = column_store->getPKRange(lo, hi);
                    result.stats.rows_scanned = row_ids.size();
                    for (size_t rid : row_ids) {
                        Row row = column_store->getRow(rid);
                        if (!row.deleted) matching_rows.push_back(std::move(row));
                    }
                    used_index = true;
                }
            }

            // --- Full scan fallback ---
            if (!used_index) {
                auto all_rows = column_store->getAllRows();
                result.stats.rows_scanned = all_rows.size();
                for (auto& row : all_rows) {
                    if (row.deleted) continue;
                    if (col_idx >= static_cast<int>(row.values.size())) continue;
                    const Value& row_val = row.values[col_idx];
                    bool matches = false;
                    switch (stmt.where.op) {
                        case ComparisonOp::EQ: matches = compareValues(row_val, stmt.where.value); break;
                        case ComparisonOp::NE: matches = notEqual(row_val, stmt.where.value); break;
                        case ComparisonOp::LT: matches = lessThan(row_val, stmt.where.value); break;
                        case ComparisonOp::LE: matches = lessEqual(row_val, stmt.where.value); break;
                        case ComparisonOp::GT: matches = greaterThan(row_val, stmt.where.value); break;
                        case ComparisonOp::GE: matches = greaterEqual(row_val, stmt.where.value); break;
                    }
                    if (matches) matching_rows.push_back(std::move(row));
                }
            }
        } else {
            matching_rows = column_store->getAllRows();
            result.stats.rows_scanned = matching_rows.size();
        }

        // Sort matching_rows if ORDER BY is present
        if (stmt.has_order_by) {
            int order_idx = table->getColumnIndex(stmt.order_by.column_name);
            if (order_idx >= 0) {
                bool desc = stmt.order_by.is_desc;
                std::sort(matching_rows.begin(), matching_rows.end(), 
                    [order_idx, desc](const Row& a, const Row& b) {
                        if (order_idx >= (int)a.values.size() || order_idx >= (int)b.values.size()) return false;
                        const Value& va = a.values[order_idx];
                        const Value& vb = b.values[order_idx];
                        if (va.type == DataType::INT && vb.type == DataType::INT) {
                            return desc ? (va.data.int_val > vb.data.int_val) : (va.data.int_val < vb.data.int_val);
                        } else if (va.type == DataType::DECIMAL && vb.type == DataType::DECIMAL) {
                            return desc ? (va.data.decimal_val > vb.data.decimal_val) : (va.data.decimal_val < vb.data.decimal_val);
                        } else {
                            std::string sa = va.toString();
                            std::string sb = vb.toString();
                            return desc ? (sa > sb) : (sa < sb);
                        }
                    });
            }
        }

        // Project columns
        const auto& schema = table->getSchema();
        std::string cache_payload;

        for (const auto& row : matching_rows) {
            ResultRow res_row;
            if (stmt.columns.empty()) {
                for (size_t i = 0; i < row.values.size() && i < schema.size(); i++) {
                    res_row.column_names.push_back(schema[i].name);
                    res_row.values.push_back(row.values[i].toString());
                }
            } else {
                for (const auto& col_name : stmt.columns) {
                    int idx = table->getColumnIndex(col_name);
                    if (idx >= 0 && idx < static_cast<int>(row.values.size())) {
                        res_row.column_names.push_back(col_name);
                        res_row.values.push_back(row.values[idx].toString());
                    }
                }
            }
            result.rows.push_back(res_row);

            // Build cache payload
            for (size_t i = 0; i < res_row.column_names.size(); i++) {
                cache_payload += res_row.column_names[i];
                if (i + 1 < res_row.column_names.size()) cache_payload += "|";
            }
            cache_payload += "\t";
            for (size_t i = 0; i < res_row.values.size(); i++) {
                cache_payload += res_row.values[i];
                if (i + 1 < res_row.values.size()) cache_payload += "|";
            }
            cache_payload += "\n";
        }

        result.stats.rows_returned = result.rows.size();

        // Store in cache (only if result is not too large)
        if (cache && cache_payload.size() < 1024 * 1024) { // < 1MB
            std::string cache_key = "SELECT:" + stmt.table_name;
            
            cache_key += ":C:";
            if (stmt.columns.empty()) {
                cache_key += "*";
            } else {
                for (const auto& c : stmt.columns) cache_key += c + ",";
            }
            
            if (stmt.has_where)
                cache_key += ":W:" + stmt.where.column_name +
                             std::to_string((int)stmt.where.op) +
                             stmt.where.value.toString();
                             
            if (stmt.has_order_by)
                cache_key += ":O:" + stmt.order_by.column_name + (stmt.order_by.is_desc ? "DESC" : "ASC");
                
            cache->put(cache_key, cache_payload);
        }

    } catch (const std::exception& e) {
        result.success = false;
        result.error_message = std::string("SELECT error: ") + e.what();
    }

    auto end = std::chrono::high_resolution_clock::now();
    result.stats.execution_time_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    return result;
}

// ============ INSERT ============

QueryResult QueryExecutor::executeInsert(const InsertStatement& stmt) {
    auto start = std::chrono::high_resolution_clock::now();

    QueryResult result;
    result.success = true;
    result.stats.cache_hit = false;

    try {
        auto table        = database->getTable(stmt.table_name);
        auto column_store = database->getColumnStore(stmt.table_name);

        // Cast away const to steal values safely
        std::vector<Value>& flat_values = const_cast<std::vector<Value>&>(stmt.flat_values);
        size_t total_rows = 0;
        if (stmt.num_columns > 0) {
            total_rows = flat_values.size() / stmt.num_columns;
        }

        time_t expiry = (stmt.ttl_seconds > 0) ? std::time(nullptr) + stmt.ttl_seconds : 0;

        // Single-lock batch insert + single write directly from flat array
        column_store->insertBatchFlat(flat_values, stmt.num_columns, expiry);

        // Explicit batch commit
        column_store->flush();
        result.stats.rows_returned = static_cast<int>(total_rows);

        // Invalidate cache entries for this table
        if (cache) cache->invalidate(stmt.table_name);

    } catch (const std::exception& e) {
        result.success = false;
        result.error_message = std::string("INSERT error: ") + e.what();
    }

    auto end = std::chrono::high_resolution_clock::now();
    result.stats.execution_time_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    return result;
}

// ============ CREATE TABLE ============

QueryResult QueryExecutor::executeCreateTable(const CreateTableStatement& stmt) {
    auto start = std::chrono::high_resolution_clock::now();

    QueryResult result;
    result.success = true;
    result.stats.cache_hit = false;

    try {
        database->createTable(stmt.table_name, stmt.columns);
        result.stats.rows_returned = 1;
    } catch (const std::exception& e) {
        result.success = false;
        result.error_message = std::string("CREATE TABLE error: ") + e.what();
    }

    auto end = std::chrono::high_resolution_clock::now();
    result.stats.execution_time_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    return result;
}

// ============ DELETE ============

QueryResult QueryExecutor::executeDelete(const DeleteStatement& stmt) {
    auto start = std::chrono::high_resolution_clock::now();

    QueryResult result;
    result.success = true;
    result.stats.cache_hit = false;
    result.stats.rows_scanned = 0;
    result.stats.rows_returned = 0;

    try {
        auto table        = database->getTable(stmt.table_name);
        auto column_store = database->getColumnStore(stmt.table_name);

        if (stmt.has_where) {
            int col_idx = table->getColumnIndex(stmt.where.column_name);
            if (col_idx < 0) {
                result.success = false;
                result.error_message = "Column not found: " + stmt.where.column_name;
                return result;
            }

            // --- B-Tree fast path for PK equality delete ---
            bool used_index = false;
            if (column_store->hasPKIndex() && col_idx == column_store->getPKColIdx() &&
                stmt.where.op == ComparisonOp::EQ &&
                stmt.where.value.type == DataType::INT)
            {
                int target = stmt.where.value.data.int_val;
                bool found = false;
                size_t row_id = column_store->getPKRange(target, target).empty()
                                ? SIZE_MAX
                                : column_store->getPKRange(target, target)[0];
                if (row_id != SIZE_MAX) {
                    column_store->deleteRow(row_id);
                    result.stats.rows_returned = 1;
                }
                result.stats.rows_scanned = 1;
                used_index = true;
                (void)found;
            }

            // --- Full scan fallback ---
            if (!used_index) {
                auto all_rows = column_store->getAllRows();
                result.stats.rows_scanned = all_rows.size();
                int deleted_count = 0;
                for (const auto& row : all_rows) {
                    if (!row.deleted && col_idx < static_cast<int>(row.values.size())) {
                        const Value& row_val = row.values[col_idx];
                        bool matches = false;
                        switch (stmt.where.op) {
                            case ComparisonOp::EQ: matches = compareValues(row_val, stmt.where.value); break;
                            case ComparisonOp::NE: matches = notEqual(row_val, stmt.where.value); break;
                            case ComparisonOp::LT: matches = lessThan(row_val, stmt.where.value); break;
                            case ComparisonOp::LE: matches = lessEqual(row_val, stmt.where.value); break;
                            case ComparisonOp::GT: matches = greaterThan(row_val, stmt.where.value); break;
                            case ComparisonOp::GE: matches = greaterEqual(row_val, stmt.where.value); break;
                        }
                        if (matches) { column_store->deleteRow(row.file_offset); deleted_count++; }
                    }
                }
                column_store->flush();
                result.stats.rows_returned = deleted_count;
            }
        } else {
            // DELETE all rows
            auto all_rows = column_store->getAllRows();
            result.stats.rows_scanned = all_rows.size();
            for (const auto& row : all_rows) {
                column_store->deleteRow(row.file_offset);
            }
            column_store->flush();
            result.stats.rows_returned = all_rows.size();
        }

        // Invalidate cache
        if (cache) cache->invalidate(stmt.table_name);

    } catch (const std::exception& e) {
        result.success = false;
        result.error_message = std::string("DELETE error: ") + e.what();
    }

    auto end = std::chrono::high_resolution_clock::now();
    result.stats.execution_time_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    return result;
}

// ============ INNER JOIN (Hash Join) ============

QueryResult QueryExecutor::executeJoin(const JoinStatement& stmt) {
    auto start = std::chrono::high_resolution_clock::now();

    QueryResult result;
    result.success = true;
    result.stats.cache_hit = false;
    result.stats.rows_scanned = 0;
    result.stats.rows_returned = 0;

    try {
        auto left_table   = database->getTable(stmt.left_table);
        auto right_table  = database->getTable(stmt.right_table);
        auto left_store   = database->getColumnStore(stmt.left_table);
        auto right_store  = database->getColumnStore(stmt.right_table);

        int left_join_idx  = left_table->getColumnIndex(stmt.left_join_col);
        int right_join_idx = right_table->getColumnIndex(stmt.right_join_col);

        if (left_join_idx < 0) {
            result.success = false;
            result.error_message = "Column not found in " + stmt.left_table + ": " + stmt.left_join_col;
            return result;
        }
        if (right_join_idx < 0) {
            result.success = false;
            result.error_message = "Column not found in " + stmt.right_table + ": " + stmt.right_join_col;
            return result;
        }

        auto left_rows  = left_store->getAllRows();
        auto right_rows = right_store->getAllRows();
        result.stats.rows_scanned = left_rows.size() + right_rows.size();

        const auto& left_schema  = left_table->getSchema();
        const auto& right_schema = right_table->getSchema();

        // Build hash map on right table: join_key_string -> list of right rows
        std::unordered_map<std::string, std::vector<const Row*>> hash_map;
        for (const auto& rrow : right_rows) {
            if (right_join_idx < static_cast<int>(rrow.values.size())) {
                std::string key = rrow.values[right_join_idx].toString();
                hash_map[key].push_back(&rrow);
            }
        }

        // WHERE filter helpers
        int where_col_left  = -1;
        int where_col_right = -1;
        if (stmt.has_where) {
            where_col_left  = left_table->getColumnIndex(stmt.where.column_name);
            where_col_right = right_table->getColumnIndex(stmt.where.column_name);
        }

        // Probe phase: for each left row, look up matching right rows
        for (const auto& lrow : left_rows) {
            if (left_join_idx >= static_cast<int>(lrow.values.size())) continue;
            std::string probe_key = lrow.values[left_join_idx].toString();

            auto it = hash_map.find(probe_key);
            if (it == hash_map.end()) continue;

            for (const Row* rrow_ptr : it->second) {
                const Row& rrow = *rrow_ptr;

                // Apply optional WHERE filter
                if (stmt.has_where) {
                    bool matches = false;
                    // Check left table first, then right
                    if (where_col_left >= 0 && where_col_left < static_cast<int>(lrow.values.size())) {
                        const Value& v = lrow.values[where_col_left];
                        switch (stmt.where.op) {
                            case ComparisonOp::EQ: matches = compareValues(v, stmt.where.value); break;
                            case ComparisonOp::NE: matches = notEqual(v, stmt.where.value); break;
                            case ComparisonOp::LT: matches = lessThan(v, stmt.where.value); break;
                            case ComparisonOp::LE: matches = lessEqual(v, stmt.where.value); break;
                            case ComparisonOp::GT: matches = greaterThan(v, stmt.where.value); break;
                            case ComparisonOp::GE: matches = greaterEqual(v, stmt.where.value); break;
                        }
                    } else if (where_col_right >= 0 && where_col_right < static_cast<int>(rrow.values.size())) {
                        const Value& v = rrow.values[where_col_right];
                        switch (stmt.where.op) {
                            case ComparisonOp::EQ: matches = compareValues(v, stmt.where.value); break;
                            case ComparisonOp::NE: matches = notEqual(v, stmt.where.value); break;
                            case ComparisonOp::LT: matches = lessThan(v, stmt.where.value); break;
                            case ComparisonOp::LE: matches = lessEqual(v, stmt.where.value); break;
                            case ComparisonOp::GT: matches = greaterThan(v, stmt.where.value); break;
                            case ComparisonOp::GE: matches = greaterEqual(v, stmt.where.value); break;
                        }
                    }
                    if (!matches) continue;
                }

                // Build result row: left columns + right columns
                ResultRow res_row;
                if (stmt.select_cols.empty()) {
                    for (size_t i = 0; i < lrow.values.size() && i < left_schema.size(); i++) {
                        res_row.column_names.push_back(stmt.left_table + "." + left_schema[i].name);
                        res_row.values.push_back(lrow.values[i].toString());
                    }
                    for (size_t i = 0; i < rrow.values.size() && i < right_schema.size(); i++) {
                        res_row.column_names.push_back(stmt.right_table + "." + right_schema[i].name);
                        res_row.values.push_back(rrow.values[i].toString());
                    }
                } else {
                    for (const auto& col_name : stmt.select_cols) {
                        res_row.column_names.push_back(col_name);
                        
                        // Parse table.col
                        std::string t_name = "";
                        std::string c_name = col_name;
                        size_t dot_pos = col_name.find('.');
                        if (dot_pos != std::string::npos) {
                            t_name = col_name.substr(0, dot_pos);
                            c_name = col_name.substr(dot_pos + 1);
                        }
                        
                        std::string val = "NULL";
                        if (t_name.empty() || t_name == stmt.left_table) {
                            int idx = left_table->getColumnIndex(c_name);
                            if (idx >= 0 && idx < static_cast<int>(lrow.values.size())) val = lrow.values[idx].toString();
                        }
                        if (val == "NULL" && (t_name.empty() || t_name == stmt.right_table)) {
                            int idx = right_table->getColumnIndex(c_name);
                            if (idx >= 0 && idx < static_cast<int>(rrow.values.size())) val = rrow.values[idx].toString();
                        }
                        res_row.values.push_back(val);
                    }
                }
                result.rows.push_back(std::move(res_row));
            }
        }

        // Sort ResultRow objects if ORDER BY is present
        if (stmt.has_order_by) {
            int target_idx = -1;
            std::string target_name = stmt.order_table.empty() ? stmt.order_by.column_name : (stmt.order_table + "." + stmt.order_by.column_name);
            
            if (!result.rows.empty()) {
                for (size_t i = 0; i < result.rows[0].column_names.size(); i++) {
                    std::string c_name = result.rows[0].column_names[i];
                    if (stmt.order_table.empty()) {
                        if (c_name == target_name || c_name.substr(c_name.find('.') + 1) == target_name) {
                            target_idx = static_cast<int>(i); break;
                        }
                    } else if (c_name == target_name) {
                        target_idx = static_cast<int>(i); break;
                    }
                }
            }
            
            if (target_idx >= 0) {
                bool desc = stmt.order_by.is_desc;
                std::sort(result.rows.begin(), result.rows.end(),
                    [target_idx, desc](const ResultRow& a, const ResultRow& b) {
                        if (target_idx >= (int)a.values.size() || target_idx >= (int)b.values.size()) return false;
                        std::string va = a.values[target_idx];
                        std::string vb = b.values[target_idx];
                        
                        try {
                            double da = std::stod(va);
                            double db = std::stod(vb);
                            return desc ? (da > db) : (da < db);
                        } catch (...) {
                            return desc ? (va > vb) : (va < vb);
                        }
                    });
            }
        }

        result.stats.rows_returned = result.rows.size();

    } catch (const std::exception& e) {
        result.success = false;
        result.error_message = std::string("INNER JOIN error: ") + e.what();
    }

    auto end = std::chrono::high_resolution_clock::now();
    result.stats.execution_time_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    return result;
}

} // namespace flexql
