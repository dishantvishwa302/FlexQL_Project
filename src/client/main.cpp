#include <iostream>
#include <string>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <cstring>
#include <unistd.h>

class FlexQLClient {
private:
    int socket_fd;
    std::string host;
    int port;
    bool connected;
    
public:
    FlexQLClient(const std::string& h, int p) 
        : socket_fd(-1), host(h), port(p), connected(false) {}
    
    ~FlexQLClient() {
        disconnect();
    }
    
    bool connect() {
        // Create socket
        socket_fd = socket(AF_INET, SOCK_STREAM, 0);
        if (socket_fd < 0) {
            std::cerr << "✗ Error: Could not create socket" << std::endl;
            return false;
        }
        
        // Connect to server
        struct sockaddr_in server_addr;
        server_addr.sin_family = AF_INET;
        server_addr.sin_port = htons(port);
        
        if (inet_pton(AF_INET, host.c_str(), &server_addr.sin_addr) <= 0) {
            std::cerr << "✗ Error: Invalid server address" << std::endl;
            close(socket_fd);
            return false;
        }
        
        if (::connect(socket_fd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
            std::cerr << "✗ Error: Could not connect to server at " << host << ":" << port << std::endl;
            std::cerr << "  Make sure the server is running: ./bin/flexql_server" << std::endl;
            close(socket_fd);
            return false;
        }
        
        connected = true;
        return true;
    }
    
    void disconnect() {
        if (socket_fd >= 0) {
            close(socket_fd);
            socket_fd = -1;
            connected = false;
        }
    }
    
    bool isConnected() const {
        return connected;
    }
    
    std::string executeQuery(const std::string& query) {
        if (!connected) {
            return "✗ Not connected to server. Please start the server first.";
        }
        
        // Send query to server
        std::string payload = query + "\n<EOF>\n";
        if (send(socket_fd, payload.c_str(), payload.length(), 0) < 0) {
            std::cerr << "✗ Error: Failed to send query to server" << std::endl;
            connected = false;
            return "ERROR: Connection lost";
        }
        
        // Receive response
        char buffer[8192];
        memset(buffer, 0, sizeof(buffer));
        
        int n = recv(socket_fd, buffer, sizeof(buffer) - 1, 0);
        if (n <= 0) {
            std::cerr << "✗ Error: Server disconnected" << std::endl;
            connected = false;
            return "ERROR: Connection lost";
        }
        
        buffer[n] = '\0';
        return std::string(buffer);
    }
};

void displayHeader() {
    std::cout << "\n╔════════════════════════════════════════════════════════════╗" << std::endl;
    std::cout << "║           FlexQL Client v1.0                              ║" << std::endl;
    std::cout << "╚════════════════════════════════════════════════════════════╝\n" << std::endl;
}

void displayHelp() {
    std::cout << "╔══════════════════════════════════════════════════════════════════╗" << std::endl;
    std::cout << "║ Available Commands:                                              ║" << std::endl;
    std::cout << "║                                                                  ║" << std::endl;
    std::cout << "║  CREATE TABLE name (col1 TYPE, col2 TYPE)                       ║" << std::endl;
    std::cout << "║  INSERT INTO table VALUES (val1, val2, ...)                     ║" << std::endl;
    std::cout << "║  SELECT * FROM table [WHERE condition]                         ║" << std::endl;
    std::cout << "║  DELETE FROM table WHERE condition                             ║" << std::endl;
    std::cout << "║  help       - Show this message                                ║" << std::endl;
    std::cout << "║  exit       - Exit the client                                  ║" << std::endl;
    std::cout << "║                                                                  ║" << std::endl;
    std::cout << "║ Example:                                                         ║" << std::endl;
    std::cout << "║  CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR, age INT) ║" << std::endl;
    std::cout << "║  INSERT INTO users VALUES (1, 'John', 25)                       ║" << std::endl;
    std::cout << "║  SELECT * FROM users                                            ║" << std::endl;
    std::cout << "╚══════════════════════════════════════════════════════════════════╝\n" << std::endl;
}

int main(int argc, char** argv) {
    std::string host = "127.0.0.1";
    int port = 9000;
    
    if (argc > 1) {
        host = argv[1];
    }
    if (argc > 2) {
        try {
            port = std::stoi(argv[2]);
        } catch (...) {
            std::cerr << "Usage: " << argv[0] << " [host] [port]" << std::endl;
            return 1;
        }
    }
    
    displayHeader();
    
    FlexQLClient client(host, port);
    
    std::cout << "Connecting to server at " << host << ":" << port << "..." << std::endl;
    if (!client.connect()) {
        return 1;
    }
    
    std::cout << "✓ Successfully connected to server\n" << std::endl;
    displayHelp();
    
    std::string input;
    std::string query;
    
    while (true) {
        std::cout << "FlexQL> ";
        std::getline(std::cin, input);
        
        // Trim whitespace
        input.erase(0, input.find_first_not_of(" \t\r\n"));
        input.erase(input.find_last_not_of(" \t\r\n") + 1);
        
        if (input.empty()) {
            continue;
        }
        
        // Handle special commands
        if (input == "exit" || input == "quit") {
            std::cout << "\n✓ Disconnecting from server..." << std::endl;
            client.disconnect();
            std::cout << "✓ Goodbye!" << std::endl;
            break;
        }
        
        if (input == "help") {
            displayHelp();
            continue;
        }
        
        if (input == "clear") {
            system("clear");
            displayHeader();
            continue;
        }
        
        // Execute query
        std::cout << "\nExecuting query...\n" << std::endl;
        std::string response = client.executeQuery(input);
        
        std::cout << "Response:" << std::endl;
        std::cout << "─────────────────────────────────────────────────────────────" << std::endl;
        std::cout << response << std::endl;
        std::cout << "─────────────────────────────────────────────────────────────\n" << std::endl;
    }
    
    return 0;
}
