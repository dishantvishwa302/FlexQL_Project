#include "../../include/network/message.h"
#include <cstring>

namespace flexql {

std::string QueryMessage::serialize() const {
    std::string result;
    result += (char)type;
    uint32_t len = query.length();
    result += (char)((len >> 24) & 0xFF);
    result += (char)((len >> 16) & 0xFF);
    result += (char)((len >> 8) & 0xFF);
    result += (char)(len & 0xFF);
    result += query;
    return result;
}

std::string ResultMessage::serialize() const {
    std::string result;
    result += (char)type;
    uint32_t len = 4;
    for (const auto& row : rows) {
        for (const auto& val : row) {
            len += 4 + val.length();
        }
    }
    result += (char)((len >> 24) & 0xFF);
    result += (char)((len >> 16) & 0xFF);
    result += (char)((len >> 8) & 0xFF);
    result += (char)(len & 0xFF);
    return result;
}

std::string ErrorMessage::serialize() const {
    std::string result;
    result += (char)type;
    uint32_t len = error.length();
    result += (char)((len >> 24) & 0xFF);
    result += (char)((len >> 16) & 0xFF);
    result += (char)((len >> 8) & 0xFF);
    result += (char)(len & 0xFF);
    result += error;
    return result;
}

}
