#ifndef FLEXQL_MESSAGE_H
#define FLEXQL_MESSAGE_H

#include <string>
#include <cstdint>
#include <vector>

namespace flexql {

enum class MessageType : uint8_t {
    QUERY = 1,
    RESULT = 2,
    ERROR = 3,
    CONNECT = 4,
    DISCONNECT = 5
};

struct Message {
    MessageType type;
    std::string data;
    
    virtual std::string serialize() const = 0;
    virtual ~Message() = default;
};

struct QueryMessage : public Message {
    std::string query;
    
    QueryMessage(const std::string& q = "") : query(q) { type = MessageType::QUERY; }
    std::string serialize() const override;
};

struct ResultMessage : public Message {
    std::vector<std::vector<std::string>> rows;
    std::vector<std::string> column_names;
    
    ResultMessage() { type = MessageType::RESULT; }
    std::string serialize() const override;
};

struct ErrorMessage : public Message {
    std::string error;
    
    ErrorMessage(const std::string& e = "") : error(e) { type = MessageType::ERROR; }
    std::string serialize() const override;
};

} // namespace flexql

#endif // FLEXQL_MESSAGE_H
