#ifndef FLEXQL_PARSER_H
#define FLEXQL_PARSER_H

#include "../common/types.h"
#include "../common/errors.h"
#include <string>
#include <vector>
#include <memory>
#include <ctime>

namespace flexql {

enum class ComparisonOp { EQ, NE, LT, LE, GT, GE };

struct WhereCondition {
    std::string column_name;
    ComparisonOp op;
    Value value;
};

struct OrderByClause {
    std::string column_name;
    bool is_desc{false};
};

struct SelectStatement {
    std::string table_name;
    std::vector<std::string> columns;
    WhereCondition where;
    bool has_where;
    OrderByClause order_by;
    bool has_order_by{false};
};

struct InsertStatement {
    std::string table_name;
    std::vector<Value> flat_values;
    size_t num_columns = 0;
    time_t ttl_seconds = 0;
};

struct DeleteStatement {
    std::string table_name;
    WhereCondition where;
    bool has_where;
};

struct CreateTableStatement {
    std::string table_name;
    std::vector<Column> columns;
};

struct JoinStatement {
    std::string left_table;
    std::string right_table;
    std::string left_join_col;    // column on left table for ON condition
    std::string right_join_col;   // column on right table for ON condition
    std::vector<std::string> select_cols; // empty = SELECT *
    WhereCondition where;
    bool has_where;
    std::string where_table; // which table the WHERE column belongs to (prefix)
    OrderByClause order_by;
    bool has_order_by{false};
    std::string order_table; // which table the ORDER BY column belongs to (prefix)
};

// Forward declarations
struct Token;
enum class TokenType;
enum class DataType;

class Parser {
private:
    std::string input;
    size_t pos;
    std::string error_msg;
    
public:
    Parser(const std::string& sql);
    std::unique_ptr<SelectStatement> parseSelect();
    std::unique_ptr<InsertStatement> parseInsert();
    std::unique_ptr<CreateTableStatement> parseCreateTable();
    std::unique_ptr<DeleteStatement> parseDelete();
    std::unique_ptr<JoinStatement> parseJoin();
    bool hasError() const { return !error_msg.empty(); }
    const std::string& getError() const { return error_msg; }
    
    // Helper methods for parsing
    DataType parseDataType(const Token& token);
    ComparisonOp getComparisonOpFromToken(const Token& token);
};

} // namespace flexql

#endif // FLEXQL_PARSER_H
