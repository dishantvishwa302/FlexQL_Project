#include "../../include/storage/database.h"
#include "../../include/common/errors.h"

namespace flexql {

void Database::createTable(const std::string& name, const std::vector<Column>& schema) {
    auto lock = this->lock.writeLock();
    
    if (tables.find(name) != tables.end()) {
        throw FlexQLException(ErrorCode::GENERIC_ERROR, "Table already exists: " + name);
    }
    
    auto table = std::make_shared<Table>(name);
    for (const auto& col : schema) {
        table->addColumn(col);
    }
    
    tables[name] = table;
    column_stores[name] = std::make_shared<ColumnStore>(table);
}

std::shared_ptr<Table> Database::getTable(const std::string& name) {
    auto lock = this->lock.readLock();
    auto it = tables.find(name);
    if (it == tables.end()) {
        throw FlexQLException(ErrorCode::TABLE_NOT_FOUND, "Table not found: " + name);
    }
    return it->second;
}

std::shared_ptr<ColumnStore> Database::getColumnStore(const std::string& name) {
    auto lock = this->lock.readLock();
    auto it = column_stores.find(name);
    if (it == column_stores.end()) {
        throw FlexQLException(ErrorCode::TABLE_NOT_FOUND, "Table not found: " + name);
    }
    return it->second;
}

bool Database::tableExists(const std::string& name) const {
    auto lock = this->lock.readLock();
    return tables.find(name) != tables.end();
}

std::vector<std::string> Database::getTableNames() const {
    auto lock = this->lock.readLock();
    std::vector<std::string> names;
    for (const auto& p : tables) {
        names.push_back(p.first);
    }
    return names;
}

long long Database::getTotalMemoryUsageBytes() const {
    auto lock = this->lock.readLock();
    long long total = 0;
    for (const auto& p : column_stores) {
        total += p.second->getMemoryUsageBytes();
    }
    return total;
}

void Database::cleanupExpiredRows() {
    auto lock = this->lock.writeLock();
    for (auto& p : column_stores) {
        p.second->deleteExpiredRows();
    }
}

}
