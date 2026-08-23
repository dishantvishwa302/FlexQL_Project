#include "../../include/network/server.h"
#include <iostream>
#include <thread>
#include <chrono>
#include <csignal>

using namespace flexql;

static Server* g_server = nullptr;

static void on_sigint(int) {
    if (g_server) g_server->stop();
}

int main(int argc, char** argv) {
    int port = 9000;

    if (argc > 1) {
        try {
            port = std::stoi(argv[1]);
        } catch (...) {
            std::cerr << "Usage: " << argv[0] << " [port]\n";
            return 1;
        }
    }

    std::cout << "\n╔════════════════════════════════════════════════════════════╗\n";
    std::cout << "║           FlexQL Server v1.0                              ║\n";
    std::cout << "╚════════════════════════════════════════════════════════════╝\n";

    Server server(port);
    g_server = &server;
    std::signal(SIGINT, on_sigint);

    if (!server.start()) {
        std::cerr << "Failed to start server\n";
        return 1;
    }

    std::cout << "\nListening on port " << port << ". Ctrl+C to stop.\n\n";

    while (server.isRunning()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    std::cout << "\nServer stopped.\n";
    return 0;
}
