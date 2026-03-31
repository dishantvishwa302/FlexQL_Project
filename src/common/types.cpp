#include "../../include/common/types.h"
#include <cstring>
#include <ctime>
#include <sstream>
#include <iomanip>
#include <cmath>

namespace flexql {

Value::Value(const std::string& v) : type(DataType::VARCHAR) {
    data.varchar_val = new char[v.length() + 1];
    std::strcpy(data.varchar_val, v.c_str());
}

// Copy constructor — deep copy varchar
Value::Value(const Value& other) : type(other.type), data{} {
    data.varchar_val = nullptr;
    if (type == DataType::VARCHAR) {
        if (other.data.varchar_val) {
            data.varchar_val = new char[std::strlen(other.data.varchar_val) + 1];
            std::strcpy(data.varchar_val, other.data.varchar_val);
        }
    } else {
        data = other.data;
    }
}

// Copy assignment
Value& Value::operator=(const Value& other) {
    if (this == &other) return *this;
    clear();
    type = other.type;
    data.varchar_val = nullptr;
    if (type == DataType::VARCHAR) {
        if (other.data.varchar_val) {
            data.varchar_val = new char[std::strlen(other.data.varchar_val) + 1];
            std::strcpy(data.varchar_val, other.data.varchar_val);
        }
    } else {
        data = other.data;
    }
    return *this;
}

// Move constructor
Value::Value(Value&& other) noexcept : type(other.type), data(other.data) {
    // Steal the pointer; null out source so it won't delete
    if (type == DataType::VARCHAR) {
        other.data.varchar_val = nullptr;
    }
    other.type = DataType::NULLTYPE;
}

// Move assignment
Value& Value::operator=(Value&& other) noexcept {
    if (this == &other) return *this;
    clear();
    type = other.type;
    data = other.data;
    if (type == DataType::VARCHAR) {
        other.data.varchar_val = nullptr;
    }
    other.type = DataType::NULLTYPE;
    return *this;
}

Value::~Value() {
    clear();
}

void Value::clear() {
    if (type == DataType::VARCHAR && data.varchar_val != nullptr) {
        delete[] data.varchar_val;
        data.varchar_val = nullptr;
    }
}

std::string Value::toString() const {
    std::ostringstream oss;
    
    switch (type) {
        case DataType::INT:
            oss << data.int_val;
            break;
        case DataType::DECIMAL:
            if (std::floor(data.decimal_val) == data.decimal_val) {
                oss << static_cast<long long>(data.decimal_val);
            } else {
                oss << std::fixed << std::setprecision(2) << data.decimal_val;
            }
            break;
        case DataType::VARCHAR:
            if (data.varchar_val) oss << data.varchar_val;
            break;
        case DataType::DATETIME: {
            char time_buf[32];
            struct tm* timeinfo = std::localtime(&data.datetime_val);
            std::strftime(time_buf, sizeof(time_buf), "%Y-%m-%d %H:%M:%S", timeinfo);
            oss << time_buf;
            break;
        }
        default:
            oss << "NULL";
            break;
    }
    return oss.str();
}

Value Value::clone() const {
    Value cloned;
    cloned.type = type;
    
    switch (type) {
        case DataType::INT:
            cloned.data.int_val = data.int_val;
            break;
        case DataType::DECIMAL:
            cloned.data.decimal_val = data.decimal_val;
            break;
        case DataType::VARCHAR:
            if (data.varchar_val) {
                cloned.data.varchar_val = new char[std::strlen(data.varchar_val) + 1];
                std::strcpy(cloned.data.varchar_val, data.varchar_val);
            }
            break;
        case DataType::DATETIME:
            cloned.data.datetime_val = data.datetime_val;
            break;
        default:
            cloned.data.varchar_val = nullptr;
            break;
    }
    return cloned;
}

bool Row::isExpired() const {
    if (expiry_time == 0) return false;
    return std::time(nullptr) >= expiry_time;
}

bool compareValues(const Value& a, const Value& b) {
    if (a.type != b.type) {
        if (a.type == DataType::INT && b.type == DataType::DECIMAL) {
            return std::fabs(a.data.int_val - b.data.decimal_val) < 1e-9;
        } else if (a.type == DataType::DECIMAL && b.type == DataType::INT) {
            return std::fabs(a.data.decimal_val - b.data.int_val) < 1e-9;
        }
        return false;
    }
    
    switch (a.type) {
        case DataType::INT:
            return a.data.int_val == b.data.int_val;
        case DataType::DECIMAL:
            return std::fabs(a.data.decimal_val - b.data.decimal_val) < 1e-9;
        case DataType::VARCHAR:
            if (!a.data.varchar_val || !b.data.varchar_val) 
                return a.data.varchar_val == b.data.varchar_val;
            return std::strcmp(a.data.varchar_val, b.data.varchar_val) == 0;
        case DataType::DATETIME:
            return a.data.datetime_val == b.data.datetime_val;
        default:
            return false;
    }
}

bool lessThan(const Value& a, const Value& b) {
    if (a.type != b.type) {
        if (a.type == DataType::INT && b.type == DataType::DECIMAL) {
            return a.data.int_val < b.data.decimal_val;
        } else if (a.type == DataType::DECIMAL && b.type == DataType::INT) {
            return a.data.decimal_val < b.data.int_val;
        }
        return false;
    }
    
    switch (a.type) {
        case DataType::INT:
            return a.data.int_val < b.data.int_val;
        case DataType::DECIMAL:
            return a.data.decimal_val < b.data.decimal_val;
        case DataType::VARCHAR:
            if (!a.data.varchar_val || !b.data.varchar_val) return false;
            return std::strcmp(a.data.varchar_val, b.data.varchar_val) < 0;
        case DataType::DATETIME:
            return a.data.datetime_val < b.data.datetime_val;
        default:
            return false;
    }
}

bool greaterThan(const Value& a, const Value& b) {
    if (a.type != b.type) {
        if (a.type == DataType::INT && b.type == DataType::DECIMAL) {
            return a.data.int_val > b.data.decimal_val;
        } else if (a.type == DataType::DECIMAL && b.type == DataType::INT) {
            return a.data.decimal_val > b.data.int_val;
        }
        return false;
    }
    
    switch (a.type) {
        case DataType::INT:
            return a.data.int_val > b.data.int_val;
        case DataType::DECIMAL:
            return a.data.decimal_val > b.data.decimal_val;
        case DataType::VARCHAR:
            if (!a.data.varchar_val || !b.data.varchar_val) return false;
            return std::strcmp(a.data.varchar_val, b.data.varchar_val) > 0;
        case DataType::DATETIME:
            return a.data.datetime_val > b.data.datetime_val;
        default:
            return false;
    }
}

bool lessEqual(const Value& a, const Value& b) {
    return lessThan(a, b) || compareValues(a, b);
}

bool greaterEqual(const Value& a, const Value& b) {
    return greaterThan(a, b) || compareValues(a, b);
}

bool notEqual(const Value& a, const Value& b) {
    return !compareValues(a, b);
}

}
