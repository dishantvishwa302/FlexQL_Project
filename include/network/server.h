#ifndef FLEXQL_SERVER_H
#define FLEXQL_SERVER_H

#include "../storage/database.h"
#include "../query/executor.h"
#include "../cache/lru_cache.h"
#include <memory>

namespace flexql {

// Forward declarations
struct QueryResult;

class Server {
private:
    int port;
    int max_connections;
    int server_socket;
    bool running;
    
    std::shared_ptr<Database> database;
    std::shared_ptr<LRUCache> cache;
    std::shared_ptr<QueryExecutor> executor;
    
public:
    Server(int port, int max_connections = 100);
    ~Server();
    
    bool start();
    void stop();
    void acceptConnections();
    void handleClient(int client_socket);
    std::string formatResponse(const QueryResult& result);
    
    int getPort() const { return port; }
    bool isRunning() const { return running; }
    std::shared_ptr<Database> getDatabase() { return database; }
};

} // namespace flexql

#endif // FLEXQL_SERVER_H
