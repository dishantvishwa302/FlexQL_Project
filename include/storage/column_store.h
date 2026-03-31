#ifndef FLEXQL_COLUMN_STORE_H
#define FLEXQL_COLUMN_STORE_H

#include "../common/types.h"
#include "table.h"
#include "../index/btree.h"
#include <vector>
#include <memory>
#include <fstream>
#include <string>
#include <mutex>
#include <fcntl.h>
#include <unistd.h>

namespace flexql {

class ColumnStore {
private:
    std::shared_ptr<Table> table;
    std::string data_file_path;
    mutable std::fstream data_file;
    mutable std::recursive_mutex file_mutex;
    size_t row_count;
    int data_fd = -1;         // raw POSIX fd for fdatasync/fadvise
    size_t flush_count;   // how many flush() calls made so far
    size_t flush_interval_batches; // How many batches before forcing fdatasync
    
    static constexpr size_t DEFAULT_FLUSH_INTERVAL_BATCHES = 1000; // Default to fdatasync every 1000 batches
    
    // B-Tree index on INT primary key (nullptr if no INT primary key)
    std::unique_ptr<BTree> pk_index;
    int pk_col_idx; // -1 if none

public:
    ColumnStore(std::shared_ptr<Table> t);
    ~ColumnStore();

    void insertRow(const Row& row);
    void insertBatch(std::vector<Row>& rows);  // batch insert with single lock
    void insertBatchFlat(const std::vector<Value>& flat_values, size_t num_columns, time_t expiry);
    Row getRow(size_t row_id) const;
    std::vector<Row> getAllRows() const;
    std::vector<Row> getRowsByColumnValue(size_t col_idx, const Value& v) const;
    std::vector<Row> getRowsByColumnRange(size_t col_idx, const Value& start, const Value& end) const;
    void deleteRow(size_t row_id);
    void deleteExpiredRows();
    size_t getRowCount() const { return row_count; }
    long long getMemoryUsageBytes() const;
    void flush();

    // B-Tree index access
    bool hasPKIndex() const { return pk_index != nullptr && pk_col_idx >= 0; }
    int getPKColIdx() const { return pk_col_idx; }

    // Disk I/O Helpers
    void writeRowAtEnd(const Row& row, size_t& out_offset);
    Row readRowAtOffset(size_t offset) const;

    // Get a single row by primary key using B-Tree index (returns deleted row if not found)
    Row getRowByPK(int pk_value) const;

    // Get matching row_offsets for a pk range using B-Tree index
    std::vector<size_t> getPKRange(int lo, int hi) const;
};

} // namespace flexql

#endif // FLEXQL_COLUMN_STORE_H
