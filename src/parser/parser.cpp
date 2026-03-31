#include "../../include/parser/parser.h"
#include <cctype>
#include <algorithm>
#include <sstream>

namespace flexql {

// ============ LEXER IMPLEMENTATION ============

enum class TokenType {
    TOKEN_EOF = 0,
    TOKEN_SELECT = 1,
    TOKEN_FROM = 2,
    TOKEN_WHERE = 3,
    TOKEN_INSERT = 4,
    TOKEN_INTO = 5,
    TOKEN_VALUES = 6,
    TOKEN_CREATE = 7,
    TOKEN_TABLE = 8,
    TOKEN_DELETE = 9,
    TOKEN_INT = 10,
    TOKEN_VARCHAR = 11,
    TOKEN_DECIMAL = 12,
    TOKEN_DATETIME = 13,
    TOKEN_PRIMARY = 14,
    TOKEN_KEY = 15,
    TOKEN_NOT = 16,
    TOKEN_NULL_KW = 17,
    TOKEN_IDENTIFIER = 20,
    TOKEN_NUMBER = 21,
    TOKEN_STRING = 22,
    TOKEN_COMMA = 23,
    TOKEN_LPAREN = 24,
    TOKEN_RPAREN = 25,
    TOKEN_STAR = 26,
    TOKEN_EQ = 27,
    TOKEN_LT = 28,
    TOKEN_GT = 29,
    TOKEN_LE = 30,
    TOKEN_GE = 31,
    TOKEN_NE = 32,
    TOKEN_WITH = 33,
    TOKEN_TTL = 34,
    TOKEN_DOT = 35,
    TOKEN_SEMICOLON = 36,
    TOKEN_INNER = 37,
    TOKEN_JOIN = 38,
    TOKEN_ON = 39
};

struct Token {
    TokenType type;
    std::string value;
};

TokenType getKeywordType(const std::string& word) {
    std::string upper = word;
    std::transform(upper.begin(), upper.end(), upper.begin(), ::toupper);
    
    if (upper == "SELECT") return TokenType::TOKEN_SELECT;
    if (upper == "FROM") return TokenType::TOKEN_FROM;
    if (upper == "WHERE") return TokenType::TOKEN_WHERE;
    if (upper == "INSERT") return TokenType::TOKEN_INSERT;
    if (upper == "INTO") return TokenType::TOKEN_INTO;
    if (upper == "VALUES") return TokenType::TOKEN_VALUES;
    if (upper == "CREATE") return TokenType::TOKEN_CREATE;
    if (upper == "TABLE") return TokenType::TOKEN_TABLE;
    if (upper == "DELETE") return TokenType::TOKEN_DELETE;
    if (upper == "INT") return TokenType::TOKEN_INT;
    if (upper == "VARCHAR") return TokenType::TOKEN_VARCHAR;
    if (upper == "DECIMAL") return TokenType::TOKEN_DECIMAL;
    if (upper == "DATETIME") return TokenType::TOKEN_DATETIME;
    if (upper == "PRIMARY") return TokenType::TOKEN_PRIMARY;
    if (upper == "KEY") return TokenType::TOKEN_KEY;
    if (upper == "NOT") return TokenType::TOKEN_NOT;
    if (upper == "NULL") return TokenType::TOKEN_NULL_KW;
    if (upper == "WITH") return TokenType::TOKEN_WITH;
    if (upper == "TTL") return TokenType::TOKEN_TTL;
    if (upper == "INNER") return TokenType::TOKEN_INNER;
    if (upper == "JOIN") return TokenType::TOKEN_JOIN;
    if (upper == "ON") return TokenType::TOKEN_ON;
    return TokenType::TOKEN_IDENTIFIER;
}

class Lexer {
private:
    std::string input;
    size_t pos;
    
public:
    Lexer(const std::string& sql) : input(sql), pos(0) {}
    
    std::vector<Token> tokenize() {
        std::vector<Token> tokens;
        
        while (pos < input.length()) {
            // Skip whitespace
            if (std::isspace(input[pos])) {
                pos++;
                continue;
            }
            
            // Single-char tokens
            if (input[pos] == '(') {
                tokens.push_back({TokenType::TOKEN_LPAREN, "("});
                pos++;
            } else if (input[pos] == ')') {
                tokens.push_back({TokenType::TOKEN_RPAREN, ")"});
                pos++;
            } else if (input[pos] == ',') {
                tokens.push_back({TokenType::TOKEN_COMMA, ","});
                pos++;
            } else if (input[pos] == '*') {
                tokens.push_back({TokenType::TOKEN_STAR, "*"});
                pos++;
            } else if (input[pos] == ';') {
                tokens.push_back({TokenType::TOKEN_SEMICOLON, ";"});
                pos++;
            } else if (input[pos] == '.') {
                tokens.push_back({TokenType::TOKEN_DOT, "."});
                pos++;
            } else if (input[pos] == '=') {
                tokens.push_back({TokenType::TOKEN_EQ, "="});
                pos++;
            } else if (input[pos] == '<') {
                if (pos + 1 < input.length()) {
                    if (input[pos + 1] == '=') {
                        tokens.push_back({TokenType::TOKEN_LE, "<="});
                        pos += 2;
                    } else if (input[pos + 1] == '>') {
                        tokens.push_back({TokenType::TOKEN_NE, "<>"});
                        pos += 2;
                    } else {
                        tokens.push_back({TokenType::TOKEN_LT, "<"});
                        pos++;
                    }
                } else {
                    tokens.push_back({TokenType::TOKEN_LT, "<"});
                    pos++;
                }
            } else if (input[pos] == '>') {
                if (pos + 1 < input.length() && input[pos + 1] == '=') {
                    tokens.push_back({TokenType::TOKEN_GE, ">="});
                    pos += 2;
                } else {
                    tokens.push_back({TokenType::TOKEN_GT, ">"});
                    pos++;
                }
            } else if (input[pos] == '!') {
                if (pos + 1 < input.length() && input[pos + 1] == '=') {
                    tokens.push_back({TokenType::TOKEN_NE, "!="});
                    pos += 2;
                } else {
                    pos++;
                }
            } else if (input[pos] == '\'' || input[pos] == '"') {
                // String literal
                char quote = input[pos];
                pos++;
                std::string str;
                while (pos < input.length() && input[pos] != quote) {
                    if (input[pos] == '\\' && pos + 1 < input.length()) {
                        pos++;
                        str += input[pos];
                    } else {
                        str += input[pos];
                    }
                    pos++;
                }
                if (pos < input.length()) pos++;  // Skip closing quote
                tokens.push_back({TokenType::TOKEN_STRING, str});
            } else if (std::isdigit(input[pos])) {
                // Number
                std::string num;
                while (pos < input.length() && (std::isdigit(input[pos]) || input[pos] == '.')) {
                    num += input[pos];
                    pos++;
                }
                tokens.push_back({TokenType::TOKEN_NUMBER, num});
            } else if (std::isalpha(input[pos]) || input[pos] == '_') {
                // Identifier or keyword
                std::string id;
                while (pos < input.length() && (std::isalnum(input[pos]) || input[pos] == '_')) {
                    id += input[pos];
                    pos++;
                }
                TokenType type = getKeywordType(id);
                tokens.push_back({type, id});
            } else {
                pos++;
            }
        }
        tokens.push_back({TokenType::TOKEN_EOF, ""});
        return tokens;
    }
};

// ============ PARSER IMPLEMENTATION ============

Parser::Parser(const std::string& sql) : input(sql), pos(0) {}

std::unique_ptr<SelectStatement> Parser::parseSelect() {
    Lexer lexer(input);
    auto tokens = lexer.tokenize();
    pos = 0;
    
    if (tokens.empty() || tokens[0].type != TokenType::TOKEN_SELECT) {
        error_msg = "Expected SELECT keyword";
        return nullptr;
    }
    
    auto stmt = std::make_unique<SelectStatement>();
    stmt->has_where = false;
    pos = 1;
    
    // Parse column list or *
    if (tokens[pos].type == TokenType::TOKEN_STAR) {
        pos++;
    } else {
        while (pos < tokens.size() && tokens[pos].type != TokenType::TOKEN_FROM) {
            if (tokens[pos].type == TokenType::TOKEN_IDENTIFIER) {
                stmt->columns.push_back(tokens[pos].value);
            }
            pos++;
            if (pos < tokens.size() && tokens[pos].type == TokenType::TOKEN_COMMA) {
                pos++;
            }
        }
    }
    
    // Parse FROM
    if (pos >= tokens.size() || tokens[pos].type != TokenType::TOKEN_FROM) {
        error_msg = "Expected FROM";
        return nullptr;
    }
    pos++;
    
    if (pos >= tokens.size() || tokens[pos].type != TokenType::TOKEN_IDENTIFIER) {
        error_msg = "Expected table name";
        return nullptr;
    }
    stmt->table_name = tokens[pos].value;
    pos++;
    
    // Parse optional WHERE
    if (pos < tokens.size() && tokens[pos].type == TokenType::TOKEN_WHERE) {
        pos++;
        if (pos < tokens.size() && tokens[pos].type == TokenType::TOKEN_IDENTIFIER) {
            stmt->where.column_name = tokens[pos].value;
            pos++;
            
            if (pos < tokens.size()) {
                stmt->where.op = getComparisonOpFromToken(tokens[pos]);
                pos++;
                
                if (pos < tokens.size()) {
                    if (tokens[pos].type == TokenType::TOKEN_NUMBER) {
                        stmt->where.value = Value(std::stod(tokens[pos].value) > 1000 ? 
                            static_cast<int>(std::stod(tokens[pos].value)) : 
                            static_cast<int>(std::stod(tokens[pos].value)));
                    } else if (tokens[pos].type == TokenType::TOKEN_STRING) {
                        stmt->where.value = Value(tokens[pos].value);
                    }
                    stmt->has_where = true;
                    pos++;
                }
            }
        }
    }
    // Parse optional ORDER BY
    auto to_upper = [](const std::string& str) {
        std::string res = str;
        std::transform(res.begin(), res.end(), res.begin(), ::toupper);
        return res;
    };
    if (pos + 1 < tokens.size() && to_upper(tokens[pos].value) == "ORDER" && to_upper(tokens[pos+1].value) == "BY") {
        pos += 2;
        if (pos < tokens.size() && tokens[pos].type == TokenType::TOKEN_IDENTIFIER) {
            stmt->order_by.column_name = tokens[pos].value;
            stmt->has_order_by = true;
            pos++;
            
            if (pos < tokens.size() && tokens[pos].type == TokenType::TOKEN_IDENTIFIER) {
                if (to_upper(tokens[pos].value) == "DESC") {
                    stmt->order_by.is_desc = true;
                    pos++;
                } else if (to_upper(tokens[pos].value) == "ASC") {
                    stmt->order_by.is_desc = false;
                    pos++;
                }
            }
        }
    }
    
    return stmt;
}

std::unique_ptr<InsertStatement> Parser::parseInsert() {
    Lexer lexer(input);
    auto tokens = lexer.tokenize();
    pos = 0;
    
    if (tokens.empty() || tokens[0].type != TokenType::TOKEN_INSERT) {
        error_msg = "Expected INSERT";
        return nullptr;
    }
    
    auto stmt = std::make_unique<InsertStatement>();
    stmt->ttl_seconds = 0;
    pos = 1;
    
    if (pos >= tokens.size() || tokens[pos].type != TokenType::TOKEN_INTO) {
        error_msg = "Expected INTO";
        return nullptr;
    }
    pos++;
    
    if (pos >= tokens.size() || tokens[pos].type != TokenType::TOKEN_IDENTIFIER) {
        error_msg = "Expected table name";
        return nullptr;
    }
    stmt->table_name = tokens[pos].value;
    pos++;
    
    if (pos >= tokens.size() || tokens[pos].type != TokenType::TOKEN_VALUES) {
        error_msg = "Expected VALUES";
        return nullptr;
    }
    pos++;
    
    // Parse multiple tuples: (val1, val2), (val3, val4)
    while (pos < tokens.size() && tokens[pos].type == TokenType::TOKEN_LPAREN) {
        pos++; // Skip (
        
        std::vector<Value> current_row;
        while (pos < tokens.size() && tokens[pos].type != TokenType::TOKEN_RPAREN) {
            if (tokens[pos].type == TokenType::TOKEN_NUMBER) {
                double num = std::stod(tokens[pos].value);
                if (num == static_cast<int>(num)) {
                    current_row.push_back(Value(static_cast<int>(num)));
                } else {
                    current_row.push_back(Value(num));
                }
            } else if (tokens[pos].type == TokenType::TOKEN_STRING) {
                current_row.push_back(Value(tokens[pos].value));
            }
            pos++;
            
            if (pos < tokens.size() && tokens[pos].type == TokenType::TOKEN_COMMA) {
                pos++;
            }
        }
        
        if (pos < tokens.size() && tokens[pos].type == TokenType::TOKEN_RPAREN) {
            pos++; // Skip )
        }
        
        if (stmt->num_columns == 0) {
            stmt->num_columns = current_row.size();
        } else if (current_row.size() != stmt->num_columns) {
            error_msg = "Inconsistent number of values in INSERT tuples";
            return nullptr;
        }

        // Steal values into the flat_values 1D array to avoid heap fragmentation
        for (auto& v : current_row) {
            stmt->flat_values.push_back(std::move(v));
        }
        
        if (pos < tokens.size() && tokens[pos].type == TokenType::TOKEN_COMMA) {
            pos++; // Skip trailing comma before next tuple
        } else {
            break; // No more tuples
        }
    }
    
    // Check for WITH TTL
    if (pos < tokens.size() && tokens[pos].type == TokenType::TOKEN_WITH) {
        pos++;
        if (pos < tokens.size() && tokens[pos].type == TokenType::TOKEN_TTL) {
            pos++;
            if (pos < tokens.size() && tokens[pos].type == TokenType::TOKEN_NUMBER) {
                stmt->ttl_seconds = std::stoi(tokens[pos].value);
                pos++;
            }
        }
    }
    
    return stmt;
}

std::unique_ptr<CreateTableStatement> Parser::parseCreateTable() {
    Lexer lexer(input);
    auto tokens = lexer.tokenize();
    pos = 0;
    
    if (tokens.empty() || tokens[0].type != TokenType::TOKEN_CREATE) {
        error_msg = "Expected CREATE";
        return nullptr;
    }
    
    auto stmt = std::make_unique<CreateTableStatement>();
    pos = 1;
    
    if (pos >= tokens.size() || tokens[pos].type != TokenType::TOKEN_TABLE) {
        error_msg = "Expected TABLE";
        return nullptr;
    }
    pos++;
    
    // Check for IF NOT EXISTS
    auto to_upper = [](const std::string& str) {
        std::string res = str;
        std::transform(res.begin(), res.end(), res.begin(), ::toupper);
        return res;
    };
    if (pos + 2 < tokens.size() && to_upper(tokens[pos].value) == "IF" && 
        tokens[pos+1].type == TokenType::TOKEN_NOT && to_upper(tokens[pos+2].value) == "EXISTS") {
        pos += 3;
    }
    
    if (pos >= tokens.size() || tokens[pos].type != TokenType::TOKEN_IDENTIFIER) {
        error_msg = "Expected table name";
        return nullptr;
    }
    stmt->table_name = tokens[pos].value;
    pos++;
    
    if (pos >= tokens.size() || tokens[pos].type != TokenType::TOKEN_LPAREN) {
        error_msg = "Expected (";
        return nullptr;
    }
    pos++;
    
    // Parse columns
    while (pos < tokens.size() && tokens[pos].type != TokenType::TOKEN_RPAREN) {
        Column col;
        
        if (tokens[pos].type != TokenType::TOKEN_IDENTIFIER) {
            pos++;
            continue;
        }
        col.name = tokens[pos].value;
        pos++;
        
        if (pos < tokens.size()) {
            col.type = parseDataType(tokens[pos]);
            pos++;
            
            // Check for optional size, e.g. VARCHAR(64)
            if (pos + 2 < tokens.size() && tokens[pos].type == TokenType::TOKEN_LPAREN && 
                tokens[pos+1].type == TokenType::TOKEN_NUMBER && tokens[pos+2].type == TokenType::TOKEN_RPAREN) {
                pos += 3; // ignore size for now
            }
        }
        
        col.is_primary_key = false;
        col.is_not_null = false;
        
        while (pos < tokens.size() && tokens[pos].type != TokenType::TOKEN_COMMA && 
               tokens[pos].type != TokenType::TOKEN_RPAREN) {
            if (tokens[pos].type == TokenType::TOKEN_PRIMARY) {
                pos++;
                if (pos < tokens.size() && tokens[pos].type == TokenType::TOKEN_KEY) {
                    col.is_primary_key = true;
                    pos++;
                }
            } else if (tokens[pos].type == TokenType::TOKEN_NOT) {
                pos++;
                if (pos < tokens.size() && tokens[pos].type == TokenType::TOKEN_NULL_KW) {
                    col.is_not_null = true;
                    pos++;
                }
            } else {
                pos++;
            }
        }
        
        stmt->columns.push_back(col);
        
        if (pos < tokens.size() && tokens[pos].type == TokenType::TOKEN_COMMA) {
            pos++;
        }
    }
    
    return stmt;
}

std::unique_ptr<DeleteStatement> Parser::parseDelete() {
    Lexer lexer(input);
    auto tokens = lexer.tokenize();
    pos = 0;
    
    if (tokens.empty() || tokens[0].type != TokenType::TOKEN_DELETE) {
        error_msg = "Expected DELETE";
        return nullptr;
    }
    
    auto stmt = std::make_unique<DeleteStatement>();
    stmt->has_where = false;
    pos = 1;
    
    if (pos >= tokens.size() || tokens[pos].type != TokenType::TOKEN_FROM) {
        error_msg = "Expected FROM";
        return nullptr;
    }
    pos++;
    
    if (pos >= tokens.size() || tokens[pos].type != TokenType::TOKEN_IDENTIFIER) {
        error_msg = "Expected table name";
        return nullptr;
    }
    stmt->table_name = tokens[pos].value;
    pos++;
    
    if (pos < tokens.size() && tokens[pos].type == TokenType::TOKEN_WHERE) {
        pos++;
        if (pos < tokens.size() && tokens[pos].type == TokenType::TOKEN_IDENTIFIER) {
            stmt->where.column_name = tokens[pos].value;
            pos++;
            
            if (pos < tokens.size()) {
                stmt->where.op = getComparisonOpFromToken(tokens[pos]);
                pos++;
                
                if (pos < tokens.size()) {
                    if (tokens[pos].type == TokenType::TOKEN_NUMBER) {
                        stmt->where.value = Value(static_cast<int>(std::stod(tokens[pos].value)));
                    } else if (tokens[pos].type == TokenType::TOKEN_STRING) {
                        stmt->where.value = Value(tokens[pos].value);
                    }
                    stmt->has_where = true;
                    pos++;
                }
            }
        }
    }
    
    return stmt;
}

DataType Parser::parseDataType(const Token& token) {
    if (token.type == TokenType::TOKEN_INT) return DataType::INT;
    if (token.type == TokenType::TOKEN_DECIMAL) return DataType::DECIMAL;
    if (token.type == TokenType::TOKEN_VARCHAR) return DataType::VARCHAR;
    if (token.type == TokenType::TOKEN_DATETIME) return DataType::DATETIME;
    return DataType::NULLTYPE;
}

ComparisonOp Parser::getComparisonOpFromToken(const Token& token) {
    if (token.type == TokenType::TOKEN_EQ) return ComparisonOp::EQ;
    if (token.type == TokenType::TOKEN_NE) return ComparisonOp::NE;
    if (token.type == TokenType::TOKEN_LT) return ComparisonOp::LT;
    if (token.type == TokenType::TOKEN_LE) return ComparisonOp::LE;
    if (token.type == TokenType::TOKEN_GT) return ComparisonOp::GT;
    if (token.type == TokenType::TOKEN_GE) return ComparisonOp::GE;
    return ComparisonOp::EQ;
}

std::unique_ptr<JoinStatement> Parser::parseJoin() {
    Lexer lexer(input);
    auto tokens = lexer.tokenize();
    pos = 0;

    // Expect: SELECT cols FROM left_table INNER JOIN right_table ON left.col = right.col [WHERE ...]
    if (tokens.empty() || tokens[pos].type != TokenType::TOKEN_SELECT) {
        error_msg = "Expected SELECT";
        return nullptr;
    }

    auto stmt = std::make_unique<JoinStatement>();
    stmt->has_where = false;
    pos++;  // skip SELECT

    // Parse selected columns or *
    if (tokens[pos].type == TokenType::TOKEN_STAR) {
        pos++;  // SELECT * - leave select_cols empty
    } else {
        // Collect column names (possibly table.col or just col)
        while (pos < tokens.size() && tokens[pos].type != TokenType::TOKEN_FROM) {
            if (tokens[pos].type == TokenType::TOKEN_IDENTIFIER) {
                std::string col_name = tokens[pos].value;
                pos++;
                // Handle table.col prefix
                if (pos < tokens.size() && tokens[pos].type == TokenType::TOKEN_DOT) {
                    pos++;
                    if (pos < tokens.size() && tokens[pos].type == TokenType::TOKEN_IDENTIFIER) {
                        col_name = col_name + "." + tokens[pos].value;
                        pos++;
                    }
                }
                stmt->select_cols.push_back(col_name);
            } else if (tokens[pos].type == TokenType::TOKEN_COMMA) {
                pos++;
            } else {
                break;
            }
        }
    }

    // FROM
    if (pos >= tokens.size() || tokens[pos].type != TokenType::TOKEN_FROM) {
        error_msg = "Expected FROM";
        return nullptr;
    }
    pos++;

    // left table
    if (pos >= tokens.size() || tokens[pos].type != TokenType::TOKEN_IDENTIFIER) {
        error_msg = "Expected table name after FROM";
        return nullptr;
    }
    stmt->left_table = tokens[pos++].value;

    // INNER
    if (pos >= tokens.size() || tokens[pos].type != TokenType::TOKEN_INNER) {
        error_msg = "Expected INNER";
        return nullptr;
    }
    pos++;

    // JOIN
    if (pos >= tokens.size() || tokens[pos].type != TokenType::TOKEN_JOIN) {
        error_msg = "Expected JOIN";
        return nullptr;
    }
    pos++;

    // right table
    if (pos >= tokens.size() || tokens[pos].type != TokenType::TOKEN_IDENTIFIER) {
        error_msg = "Expected right table name";
        return nullptr;
    }
    stmt->right_table = tokens[pos++].value;

    // ON
    if (pos >= tokens.size() || tokens[pos].type != TokenType::TOKEN_ON) {
        error_msg = "Expected ON";
        return nullptr;
    }
    pos++;

    // left_table.col = right_table.col
    // Could be identifier DOT identifier
    auto parseTableCol = [&](std::string& table_name, std::string& col_name) -> bool {
        if (pos >= tokens.size() || tokens[pos].type != TokenType::TOKEN_IDENTIFIER) return false;
        std::string first = tokens[pos++].value;
        if (pos < tokens.size() && tokens[pos].type == TokenType::TOKEN_DOT) {
            pos++;
            if (pos >= tokens.size() || tokens[pos].type != TokenType::TOKEN_IDENTIFIER) return false;
            table_name = first;
            col_name = tokens[pos++].value;
        } else {
            col_name = first;
        }
        return true;
    };

    std::string left_tbl, right_tbl;
    if (!parseTableCol(left_tbl, stmt->left_join_col)) {
        error_msg = "Expected left join column";
        return nullptr;
    }

    // '='
    if (pos >= tokens.size() || tokens[pos].type != TokenType::TOKEN_EQ) {
        error_msg = "Expected = in ON clause";
        return nullptr;
    }
    pos++;

    if (!parseTableCol(right_tbl, stmt->right_join_col)) {
        error_msg = "Expected right join column";
        return nullptr;
    }

    // Optional WHERE
    if (pos < tokens.size() && tokens[pos].type == TokenType::TOKEN_WHERE) {
        pos++;
        // Could be table.col or just col
        if (pos < tokens.size() && tokens[pos].type == TokenType::TOKEN_IDENTIFIER) {
            std::string first = tokens[pos++].value;
            if (pos < tokens.size() && tokens[pos].type == TokenType::TOKEN_DOT) {
                pos++;
                stmt->where_table = first;
                if (pos < tokens.size() && tokens[pos].type == TokenType::TOKEN_IDENTIFIER) {
                    stmt->where.column_name = tokens[pos++].value;
                }
            } else {
                stmt->where.column_name = first;
            }

            if (pos < tokens.size()) {
                stmt->where.op = getComparisonOpFromToken(tokens[pos++]);
            }
            if (pos < tokens.size()) {
                if (tokens[pos].type == TokenType::TOKEN_NUMBER) {
                    stmt->where.value = Value(static_cast<int>(std::stod(tokens[pos].value)));
                } else if (tokens[pos].type == TokenType::TOKEN_STRING) {
                    stmt->where.value = Value(tokens[pos].value);
                }
                stmt->has_where = true;
                pos++;
            }
        }
    }
    // Parse optional ORDER BY
    auto to_upper = [](const std::string& str) {
        std::string res = str;
        std::transform(res.begin(), res.end(), res.begin(), ::toupper);
        return res;
    };
    if (pos + 1 < tokens.size() && to_upper(tokens[pos].value) == "ORDER" && to_upper(tokens[pos+1].value) == "BY") {
        pos += 2;
        if (pos < tokens.size() && tokens[pos].type == TokenType::TOKEN_IDENTIFIER) {
            std::string first = tokens[pos++].value;
            if (pos < tokens.size() && tokens[pos].type == TokenType::TOKEN_DOT) {
                pos++;
                stmt->order_table = first;
                if (pos < tokens.size() && tokens[pos].type == TokenType::TOKEN_IDENTIFIER) {
                    stmt->order_by.column_name = tokens[pos++].value;
                }
            } else {
                stmt->order_by.column_name = first;
            }
            stmt->has_order_by = true;
            
            if (pos < tokens.size() && tokens[pos].type == TokenType::TOKEN_IDENTIFIER) {
                if (to_upper(tokens[pos].value) == "DESC") {
                    stmt->order_by.is_desc = true;
                    pos++;
                } else if (to_upper(tokens[pos].value) == "ASC") {
                    stmt->order_by.is_desc = false;
                    pos++;
                }
            }
        }
    }

    return stmt;
}

}  // namespace flexql
