#include "server.hpp"

#include <arpa/inet.h>
#include <iostream>
#include <netdb.h>
#include <string>
#include <sys/epoll.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/fcntl.h>
#include <unistd.h>

int make_socket_non_blocking(int socket_fd) {
    int flags = fcntl(socket_fd, F_GETFL, 0);
    if (flags == -1)
        return -1;
    flags |= O_NONBLOCK;
    return fcntl(socket_fd, F_SETFL, flags);
}

int main(int argc, char** argv) {
    // Flush after every std::cout / std::cerr
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;

    redis::Logger::getInstance().set_level(redis::Logger::INFO);

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        std::cerr << "Failed to create server socket\n";
        return 1;
    }
    make_socket_non_blocking(server_fd);

    // Since the tester restarts your program quite often, setting SO_REUSEADDR
    // ensures that we don't run into 'Address already in use' errors
    int reuse = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
        std::cerr << "setsockopt failed\n";
        return 1;
    }

    struct sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(6379);

    if (bind(server_fd, reinterpret_cast<struct sockaddr*>(&server_addr), sizeof(server_addr)) != 0) {
        std::cerr << "Failed to bind to port 6379\n";
        return 1;
    }

    int connection_backlog = 5;
    if (listen(server_fd, connection_backlog) != 0) {
        std::cerr << "listen failed\n";
        return 1;
    }

    int epoll_fd = epoll_create1(0); // Creates an epoll instance
    epoll_event event;
    event.data.fd = server_fd;
    event.events = EPOLLIN; // Monitor for incoming connections
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &event);

    epoll_event events[MAX_EVENTS];

    while (true) {
        /**
         * @brief timeout of -1 causes epoll_wait() to block indefinitely, while
           specifying a timeout equal to zero causes epoll_wait() to return
           immediately, even if no events are available
         */
        int n_fds = epoll_wait(epoll_fd, events, MAX_EVENTS, -1);
        for (int i = 0; i < n_fds; ++i) {
            if (events[i].data.fd == server_fd) {
                // Accept new connection
                int client_fd = accept(server_fd, NULL, NULL);
                make_socket_non_blocking(client_fd);
                event.data.fd = client_fd;
                event.events = EPOLLIN | EPOLLET;
                epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &event);
            } else {
                // Handle data from client
                char buf[2048];
                ssize_t count = read(events[i].data.fd, buf, sizeof(buf));
                if (count == 0) { // EOF
                    close(events[i].data.fd);
                } else if (count > 0) {
                    try {
                        auto resp_array = redis::get_resp_array(buf);
                        auto response = redis::get_response(resp_array);
                        write(events[i].data.fd, response.c_str(), response.size()); // Respond
                    } catch (const std::exception& e) {
                        std::cerr << e.what() << std::endl;
                    }
                } else {
                    std::cerr << "error on read: " << errno << std::endl;
                }
            }
        }
    }

    close(server_fd);

    return 0;
}
