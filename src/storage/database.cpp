#include "../../include/storage/database.h"
#include "../../include/common/errors.h"
#include <filesystem>
#include <fstream>
#include <sstream>

namespace flexql {

namespace fs = std::filesystem;

std::string Database::typeToString(DataType t) {
    switch (t) {
        case DataType::INT: return "INT";
        case DataType::DECIMAL: return "DECIMAL";
        case DataType::VARCHAR: return "VARCHAR";
        case DataType::DATETIME: return "DATETIME";
        default: return "VARCHAR";
    }
}

DataType Database::stringToType(const std::string& s) {
    if (s == "INT") return DataType::INT;
    if (s == "DECIMAL") return DataType::DECIMAL;
    if (s == "VARCHAR") return DataType::VARCHAR;
    if (s == "DATETIME") return DataType::DATETIME;
    return DataType::NULLTYPE;
}

Database::Database() {
    fs::create_directories("data/tables");
    loadExistingTables();
}

void Database::saveSchema(const std::string& name, const std::vector<Column>& schema) const {
    std::ofstream f("data/tables/" + name + ".schema");
    for (const auto& col : schema) {
        f << col.name << " " << typeToString(col.type);
        if (col.is_primary_key) f << " PRIMARY_KEY";
        if (col.is_not_null) f << " NOT_NULL";
        f << "\n";
    }
}

void Database::loadExistingTables() {
    fs::path dir("data/tables");
    if (!fs::exists(dir)) return;

    for (const auto& entry : fs::directory_iterator(dir)) {
        if (entry.path().extension() != ".schema") continue;

        std::string name = entry.path().stem().string();
        std::ifstream in(entry.path());
        std::vector<Column> schema;
        std::string line;
        while (std::getline(in, line)) {
            if (line.empty()) continue;
            std::istringstream ss(line);
            Column col;
            std::string type_name;
            ss >> col.name >> type_name;
            col.type = stringToType(type_name);
            col.is_primary_key = false;
            col.is_not_null = false;
            std::string flag;
            while (ss >> flag) {
                if (flag == "PRIMARY_KEY") col.is_primary_key = true;
                if (flag == "NOT_NULL") col.is_not_null = true;
            }
            schema.push_back(col);
        }
        if (schema.empty()) continue;

        auto table = std::make_shared<Table>(name);
        for (const auto& col : schema) table->addColumn(col);
        tables[name] = table;
        stores[name] = std::make_shared<RowStore>(table);
    }
}

void Database::createTable(const std::string& name, const std::vector<Column>& schema,
                           bool if_not_exists) {
    auto lock_guard = this->lock.writeLock();

    if (tables.find(name) != tables.end()) {
        if (if_not_exists) return;
        throw FlexQLException(ErrorCode::GENERIC_ERROR, "Table already exists: " + name);
    }

    auto table = std::make_shared<Table>(name);
    for (const auto& col : schema) {
        table->addColumn(col);
    }

    tables[name] = table;
    stores[name] = std::make_shared<RowStore>(table);
    saveSchema(name, schema);
}

std::shared_ptr<Table> Database::getTable(const std::string& name) {
    auto lock_guard = this->lock.readLock();
    auto it = tables.find(name);
    if (it == tables.end()) {
        throw FlexQLException(ErrorCode::TABLE_NOT_FOUND, "Table not found: " + name);
    }
    return it->second;
}

std::shared_ptr<RowStore> Database::getRowStore(const std::string& name) {
    auto lock_guard = this->lock.readLock();
    auto it = stores.find(name);
    if (it == stores.end()) {
        throw FlexQLException(ErrorCode::TABLE_NOT_FOUND, "Table not found: " + name);
    }
    return it->second;
}

bool Database::tableExists(const std::string& name) const {
    auto lock_guard = this->lock.readLock();
    return tables.find(name) != tables.end();
}

std::vector<std::string> Database::getTableNames() const {
    auto lock_guard = this->lock.readLock();
    std::vector<std::string> names;
    names.reserve(tables.size());
    for (const auto& p : tables) names.push_back(p.first);
    return names;
}

long long Database::getTotalMemoryUsageBytes() const {
    auto lock_guard = this->lock.readLock();
    long long total = 0;
    for (const auto& p : stores) {
        total += p.second->getMemoryUsageBytes();
    }
    return total;
}

void Database::cleanupExpiredRows() {
    auto lock_guard = this->lock.writeLock();
    for (auto& p : stores) {
        p.second->deleteExpiredRows();
    }
}

} // namespace flexql
