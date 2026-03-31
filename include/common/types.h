#ifndef FLEXQL_TYPES_H
#define FLEXQL_TYPES_H

#include <string>
#include <vector>
#include <ctime>
#include <unordered_map>

namespace flexql {

enum class DataType {
    INT,
    DECIMAL,
    VARCHAR,
    DATETIME,
    NULLTYPE
};

union DataValue {
    int int_val;
    double decimal_val;
    char* varchar_val;
    time_t datetime_val;
};

struct Value {
    DataType type;
    DataValue data;
    
    Value() : type(DataType::NULLTYPE), data{} { data.varchar_val = nullptr; }
    Value(int v) : type(DataType::INT) { data.int_val = v; }
    Value(double v) : type(DataType::DECIMAL) { data.decimal_val = v; }
    Value(const std::string& v);
    Value(time_t v) : type(DataType::DATETIME) { data.datetime_val = v; }
    
    // Copy constructor — deep copies varchar
    Value(const Value& other);
    // Copy assignment
    Value& operator=(const Value& other);
    // Move constructor
    Value(Value&& other) noexcept;
    // Move assignment
    Value& operator=(Value&& other) noexcept;
    
    ~Value();
    void clear();
    std::string toString() const;
    Value clone() const;
};

struct Column {
    std::string name;
    DataType type;
    bool is_primary_key;
    bool is_not_null;
};

struct Row {
    std::vector<Value> values;
    time_t expiry_time;
    bool deleted;
    size_t file_offset;
    
    Row() : expiry_time(0), deleted(false), file_offset(0) {}
    bool isExpired() const;
};

struct ResultRow {
    std::vector<std::string> values;
    std::vector<std::string> column_names;
};

struct QueryStats {
    long long execution_time_ms;
    int rows_scanned;
    int rows_returned;
    bool cache_hit;
};

// Comparison functions
bool compareValues(const Value& a, const Value& b);
bool lessThan(const Value& a, const Value& b);
bool greaterThan(const Value& a, const Value& b);
bool lessEqual(const Value& a, const Value& b);
bool greaterEqual(const Value& a, const Value& b);
bool notEqual(const Value& a, const Value& b);

} // namespace flexql

#endif // FLEXQL_TYPES_H
