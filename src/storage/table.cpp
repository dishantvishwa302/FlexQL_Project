#include "../../include/storage/table.h"
#include "../../include/common/errors.h"

namespace flexql {

Table::Table(const std::string& table_name) 
    : name(table_name), primary_key_idx(-1) {}

void Table::addColumn(const Column& col) {
    size_t idx = schema.size();
    schema.push_back(col);
    column_index[col.name] = idx;
}

const Column& Table::getColumn(size_t idx) const {
    return schema[idx];
}

const Column& Table::getColumn(const std::string& name) const {
    auto it = column_index.find(name);
    if (it == column_index.end()) {
        throw FlexQLException(ErrorCode::COLUMN_NOT_FOUND, "Column not found: " + name);
    }
    return schema[it->second];
}

int Table::getColumnIndex(const std::string& name) const {
    auto it = column_index.find(name);
    return (it != column_index.end()) ? it->second : -1;
}

void Table::setPrimaryKeyColumn(size_t idx) {
    primary_key_idx = idx;
}

}
