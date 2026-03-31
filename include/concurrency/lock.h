#ifndef FLEXQL_LOCK_H
#define FLEXQL_LOCK_H

#include <shared_mutex>
#include <mutex>

namespace flexql {

class RWLock {
private:
    mutable std::shared_mutex mutex;
    
public:
    RWLock() = default;
    std::shared_lock<std::shared_mutex> readLock() const { 
        return std::shared_lock<std::shared_mutex>(mutex);
    }
    std::unique_lock<std::shared_mutex> writeLock() {
        return std::unique_lock<std::shared_mutex>(mutex);
    }
};

} // namespace flexql

#endif // FLEXQL_LOCK_H
