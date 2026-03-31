#ifndef FLEXQL_ERRORS_H
#define FLEXQL_ERRORS_H

#include <exception>
#include <string>

namespace flexql {

enum class ErrorCode {
    OK = 0,
    GENERIC_ERROR = 1,
    TABLE_NOT_FOUND = 2,
    COLUMN_NOT_FOUND = 3,
    SYNTAX_ERROR = 4,
    INVALID_TYPE = 5,
    PRIMARY_KEY_VIOLATION = 6,
    NOT_NULL_VIOLATION = 7,
    MEMORY_ERROR = 8,
    IO_ERROR = 9,
    NETWORK_ERROR = 10
};

class FlexQLException : public std::exception {
private:
    ErrorCode code;
    std::string message;
    
public:
    FlexQLException(ErrorCode c, const std::string& msg) : code(c), message(msg) {}
    const char* what() const noexcept override { return message.c_str(); }
    ErrorCode getCode() const { return code; }
};

} // namespace flexql

#endif // FLEXQL_ERRORS_H
