#ifndef FLEXQL_TABLE_H
#define FLEXQL_TABLE_H

#include "../common/types.h"
#include <vector>
#include <unordered_map>

namespace flexql {

class Table {
private:
    std::string name;
    std::vector<Column> schema;
    std::unordered_map<std::string, size_t> column_index;
    int primary_key_idx;
    
public:
    Table(const std::string& table_name);
    void addColumn(const Column& col);
    const Column& getColumn(size_t idx) const;
    const Column& getColumn(const std::string& name) const;
    int getColumnIndex(const std::string& name) const;
    const std::vector<Column>& getSchema() const { return schema; }
    void setPrimaryKeyColumn(size_t idx);
    int getPrimaryKeyIndex() const { return primary_key_idx; }
    size_t getColumnCount() const { return schema.size(); }
    const std::string& getName() const { return name; }
};

} // namespace flexql

#endif // FLEXQL_TABLE_H
