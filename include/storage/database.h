#ifndef FLEXQL_DATABASE_H
#define FLEXQL_DATABASE_H

#include "table.h"
#include "row_store.h"
#include "../concurrency/lock.h"
#include <memory>
#include <unordered_map>

namespace flexql {

class Database {
private:
    std::unordered_map<std::string, std::shared_ptr<Table>> tables;
    std::unordered_map<std::string, std::shared_ptr<RowStore>> stores;
    RWLock lock;

    void saveSchema(const std::string& name, const std::vector<Column>& schema) const;
    void loadExistingTables();
    static std::string typeToString(DataType t);
    static DataType stringToType(const std::string& s);

public:
    Database();
    void createTable(const std::string& name, const std::vector<Column>& schema,
                     bool if_not_exists = false);
    std::shared_ptr<Table> getTable(const std::string& name);
    std::shared_ptr<RowStore> getRowStore(const std::string& name);
    bool tableExists(const std::string& name) const;
    std::vector<std::string> getTableNames() const;
    long long getTotalMemoryUsageBytes() const;
    void cleanupExpiredRows();
};

} // namespace flexql

#endif
