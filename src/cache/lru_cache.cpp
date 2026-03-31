#include "../../include/cache/lru_cache.h"

namespace flexql {

LRUCache::LRUCache(int max_size) 
    : max_entries(max_size), hits(0), misses(0) {}

bool LRUCache::get(const std::string& key, std::string& value) {
    std::lock_guard<std::mutex> lock(cache_mutex);
    auto it = cache_map.find(key);
    
    if (it == cache_map.end()) {
        misses++;
        return false;
    }
    
    // Move to front (most recently used)
    auto entry = *it->second;
    cache_list.erase(it->second);
    cache_list.push_front(entry);
    cache_map[key] = cache_list.begin();
    
    value = entry.value;
    hits++;
    return true;
}

void LRUCache::put(const std::string& key, const std::string& value) {
    std::lock_guard<std::mutex> lock(cache_mutex);
    auto it = cache_map.find(key);
    
    if (it != cache_map.end()) {
        cache_list.erase(it->second);
    }
    
    CacheEntry entry{key, value};
    cache_list.push_front(entry);
    cache_map[key] = cache_list.begin();
    
    // Evict LRU if needed
    if (static_cast<int>(cache_list.size()) > max_entries) {
        auto last = cache_list.back();
        cache_map.erase(last.key);
        cache_list.pop_back();
    }
}

void LRUCache::clear() {
    std::lock_guard<std::mutex> lock(cache_mutex);
    cache_list.clear();
    cache_map.clear();
    hits = 0;
    misses = 0;
}

void LRUCache::invalidate(const std::string& table_name) {
    std::lock_guard<std::mutex> lock(cache_mutex);
    // Remove all entries whose key contains the table name
    auto it = cache_list.begin();
    while (it != cache_list.end()) {
        if (it->key.find(table_name) != std::string::npos) {
            cache_map.erase(it->key);
            it = cache_list.erase(it);
        } else {
            ++it;
        }
    }
}

int LRUCache::getHitRate() const {
    std::lock_guard<std::mutex> lock(cache_mutex);
    int total = hits + misses;
    if (total == 0) return 0;
    return (hits * 100) / total;
}

long long LRUCache::getMemoryUsageBytes() const {
    std::lock_guard<std::mutex> lock(cache_mutex);
    long long size = 0;
    for (const auto& entry : cache_list) {
        size += entry.key.length() + entry.value.length();
    }
    return size;
}

} // namespace flexql
