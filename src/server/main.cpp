#include "../../include/network/server.h"
#include <iostream>
#include <thread>
#include <chrono>

using namespace flexql;

int main(int argc, char** argv) {
    int port = 9000; // Default PostgreSQL port
    
    if (argc > 1) {
        try {
            port = std::stoi(argv[1]);
        } catch (...) {
            std::cerr << "Usage: " << argv[0] << " [port]" << std::endl;
            return 1;
        }
    }
    
    std::cout << "\n╔════════════════════════════════════════════════════════════╗" << std::endl;
    std::cout << "║           FlexQL Server v1.0                              ║" << std::endl;
    std::cout << "╚════════════════════════════════════════════════════════════╝" << std::endl;
    
    Server server(port);
    
    if (!server.start()) {
        std::cerr << "✗ Failed to start server" << std::endl;
        return 1;
    }
    
    std::cout << "\n✓ Server started successfully" << std::endl;
    std::cout << "✓ Listening for client connections on port " << port << std::endl;
    std::cout << "✓ Press Ctrl+C to stop\n" << std::endl;
    
    // Keep server running
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    
    server.stop();
    std::cout << "\n✓ Server stopped" << std::endl;
    
    return 0;
}
