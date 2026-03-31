#ifndef FLEXQL_EXECUTOR_H
#define FLEXQL_EXECUTOR_H

#include "../parser/parser.h"
#include "../storage/database.h"
#include "../cache/lru_cache.h"
#include "../common/types.h"
#include <memory>
#include <vector>

namespace flexql {

struct QueryResult {
    std::vector<ResultRow> rows;
    QueryStats stats;
    bool success;
    std::string error_message;
};

class QueryExecutor {
private:
    std::shared_ptr<Database> database;
    std::shared_ptr<LRUCache> cache;
    
public:
    QueryExecutor(std::shared_ptr<Database> db,
                  std::shared_ptr<LRUCache> lru = nullptr);
    QueryResult execute(const std::string& sql);
    QueryResult executeSelect(const SelectStatement& stmt);
    QueryResult executeInsert(const InsertStatement& stmt);
    QueryResult executeCreateTable(const CreateTableStatement& stmt);
    QueryResult executeDelete(const DeleteStatement& stmt);
    QueryResult executeJoin(const JoinStatement& stmt);
};

} // namespace flexql

#endif // FLEXQL_EXECUTOR_H
