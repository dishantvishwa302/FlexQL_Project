#ifndef FLEXQL_LRU_CACHE_H
#define FLEXQL_LRU_CACHE_H

#include <unordered_map>
#include <list>
#include <string>
#include <memory>
#include <mutex>

namespace flexql {

struct CacheEntry {
    std::string key;
    std::string value;
};

class LRUCache {
private:
    int max_entries;
    std::list<CacheEntry> cache_list;
    std::unordered_map<std::string, std::list<CacheEntry>::iterator> cache_map;
    int hits;
    int misses;
    mutable std::mutex cache_mutex;
    
public:
    LRUCache(int max_size = 1000);
    
    bool get(const std::string& key, std::string& value);
    void put(const std::string& key, const std::string& value);
    void clear();

    // Remove all cache entries whose key contains table_name (for invalidation)
    void invalidate(const std::string& table_name);
    
    int getHitRate() const;
    long long getMemoryUsageBytes() const;
};

} // namespace flexql

#endif // FLEXQL_LRU_CACHE_H
