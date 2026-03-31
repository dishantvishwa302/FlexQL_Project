#ifndef FLEXQL_DATABASE_H
#define FLEXQL_DATABASE_H

#include "table.h"
#include "column_store.h"
#include "../concurrency/lock.h"
#include <memory>
#include <unordered_map>

namespace flexql {

class Database {
private:
    std::unordered_map<std::string, std::shared_ptr<Table>> tables;
    std::unordered_map<std::string, std::shared_ptr<ColumnStore>> column_stores;
    RWLock lock;
    
public:
    Database() = default;
    void createTable(const std::string& name, const std::vector<Column>& schema);
    std::shared_ptr<Table> getTable(const std::string& name);
    std::shared_ptr<ColumnStore> getColumnStore(const std::string& name);
    bool tableExists(const std::string& name) const;
    std::vector<std::string> getTableNames() const;
    long long getTotalMemoryUsageBytes() const;
    void cleanupExpiredRows();
};

} // namespace flexql

#endif // FLEXQL_DATABASE_H
