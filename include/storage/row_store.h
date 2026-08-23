#ifndef FLEXQL_ROW_STORE_H
#define FLEXQL_ROW_STORE_H

#include "../common/types.h"
#include "table.h"
#include "../index/btree.h"
#include <vector>
#include <memory>
#include <fstream>
#include <string>
#include <mutex>

namespace flexql {

// Row-major disk store. One .dat file per table.
// Each row: [deleted:1B][expiry:8B][col0][col1]...[colN]
// The B-tree maps INT primary keys -> file offset.
class RowStore {
private:
    std::shared_ptr<Table> table;
    std::string data_file_path;
    mutable std::fstream data_file;
    mutable std::recursive_mutex file_mutex;
    size_t row_count;
    int data_fd = -1;
    size_t flush_count;
    size_t flush_interval_batches;

    static constexpr size_t DEFAULT_FLUSH_INTERVAL_BATCHES = 1000;

    std::unique_ptr<BTree> pk_index;
    int pk_col_idx; // -1 if the table has no INT/DECIMAL primary key

    void skipColumn(const Column& col) const;
    void appendValue(std::vector<char>& buf, const Column& col, const Value* v) const;

public:
    explicit RowStore(std::shared_ptr<Table> t);
    ~RowStore();

    void insertRow(const Row& row);
    void insertBatchFlat(const std::vector<Value>& flat_values, size_t num_columns, time_t expiry);
    Row getRow(size_t offset) const;
    std::vector<Row> getAllRows() const;
    void deleteRow(size_t offset);
    void deleteExpiredRows();
    size_t getRowCount() const { return row_count; }
    long long getMemoryUsageBytes() const;
    void flush();

    bool hasPKIndex() const { return pk_index != nullptr && pk_col_idx >= 0; }
    int getPKColIdx() const { return pk_col_idx; }
    Row getRowByPK(int pk_value) const;
    std::vector<size_t> getPKRange(int lo, int hi) const;

    void writeRowAtEnd(const Row& row, size_t& out_offset);
    Row readRowAtOffset(size_t offset) const;
};

} // namespace flexql

#endif
