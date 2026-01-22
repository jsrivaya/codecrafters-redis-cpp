#include "logger.hpp"
#include "server.hpp"

int main(int argc, char** argv) {
    // Flush after every std::cout / std::cerr
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;

    redis::Logger::getInstance().set_level(redis::Logger::DEBUG);

    redis::RedisServer::GetTheServer().run();

    return 0;
}
