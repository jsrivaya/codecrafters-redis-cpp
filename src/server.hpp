#pragma once

#include "logger.hpp"
#include "parser.hpp"

#include <arpa/inet.h>
#include <sys/epoll.h>
#include <sys/fcntl.h>
#include <sys/socket.h>

namespace redis {
#define MAX_EVENTS 10
#define PORT 8080

    class RedisServer {
      public:
        static RedisServer& GetTheServer() {
            static RedisServer the_server;
            return the_server;
        }

        void run() {
            int epoll_fd = epoll_create1(0); // Creates an epoll instance
            epoll_event event;
            event.data.fd = server_fd;
            event.events = EPOLLIN; // Monitor for incoming connections
            epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &event);

            epoll_event events[MAX_EVENTS];
            while (true) {
                /**
                 * @brief timeout of -1 causes epoll_wait() to block indefinitely, while
                 * specifying a timeout equal to zero causes epoll_wait() to return
                 * immediately, even if no events are available
                 **/
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
                                auto resp_array = get_resp_array(buf);
                                auto response = get_response(resp_array);
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
        }

      protected:
        int server_fd;
        struct sockaddr_in server_addr;
        struct sockaddr_in client_addr;

      private:
        std::list<Data> store{};
        std::unordered_map<std::string, std::list<Data>::iterator> lookup_table{};

        RedisServer() {
            server_fd = socket(AF_INET, SOCK_STREAM, 0);
            if (server_fd < 0) {
                throw std::runtime_error("redis server: failed to create server socket");
            }

            make_socket_non_blocking(server_fd);

            // Since the tester restarts your program quite often, setting SO_REUSEADDR
            // ensures that we don't run into 'Address already in use' errors
            int reuse = 1;
            if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
                std::cerr << "setsockopt failed\n";
                throw std::runtime_error("redis server: failed to set socket options");
            }

            server_addr.sin_family = AF_INET;
            server_addr.sin_addr.s_addr = INADDR_ANY;
            server_addr.sin_port = htons(6379);
            if (bind(server_fd, reinterpret_cast<struct sockaddr*>(&server_addr), sizeof(server_addr)) != 0) {
                std::cerr << "Failed to bind to port 6379\n";
                throw std::runtime_error("redis server: bind failed");
            }

            int connection_backlog = 5;
            if (listen(server_fd, connection_backlog) != 0) {
                std::cerr << "listen failed\n";
                throw std::runtime_error("redis server: listen failed");
            }
        };

        RedisServer(const RedisServer&) = delete;
        RedisServer& operator=(const RedisServer&) = delete;
        ~RedisServer() {
            close(server_fd);
        }

        int make_socket_non_blocking(int socket_fd) {
            int flags = fcntl(socket_fd, F_GETFL, 0);
            if (flags == -1)
                return -1;
            flags |= O_NONBLOCK;
            return fcntl(socket_fd, F_SETFL, flags);
        }

        std::string ping() {
            return "+PONG\r\n";
        }

        std::string get_nil_bulk_string() {
            return "$-1\r\n";
        }
        std::string get_bulk_string(const std::string& s) {
            return "$" + std::to_string(s.length()) + "\r\n" + s + "\r\n";
        }

        std::string get_simple_string(const std::string& s) {
            return "+" + s + "\r\n";
        }

        std::string echo(std::queue<std::string>& args) {
            std::string response{};
            while (!args.empty()) {
                response += get_bulk_string(args.front());
                args.pop();
            }
            return response;
        }

        std::string set(std::queue<std::string>& args) {
            auto key = args.front();
            args.pop();
            auto value = args.front();
            args.pop();

            Data data{value, std::chrono::steady_clock::now(), 0};
            auto lookup_itr = lookup_table.find(key);
            if (lookup_itr == lookup_table.end()) {
                // new data: insert in store and lookup table
                store.emplace_front(data);
                lookup_table.emplace(key, store.begin());
            } else {
                // move list element to the front
                auto data_itr = lookup_table.at(key);
                data_itr->value = value;
                store.splice(store.begin(), store, data_itr);
            }
            // parse options
            while (!args.empty()) {
                auto arg = args.front();
                args.pop();
                if (arg == "PX" && !args.empty()) {
                    auto px_ms = args.front();
                    args.pop();
                    int px_ms_int = 0;
                    std::from_chars(px_ms.data(), px_ms.data() + px_ms.size(), px_ms_int);
                    store.begin()->expiry_ms = px_ms_int;
                }
            }

            return get_simple_string("OK");
        }

        std::string get(const std::queue<std::string>& args) {
            auto lookup_itr = lookup_table.find(args.front());
            if (lookup_itr != lookup_table.end()) {
                auto data_entry = lookup_table.at(args.front());
                auto now = std::chrono::steady_clock::now();
                auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
                auto entry_expire_time = data_entry->timestamp + std::chrono::milliseconds(data_entry->expiry_ms);
                auto entry_expire_time_ms =
                    std::chrono::duration_cast<std::chrono::milliseconds>(entry_expire_time.time_since_epoch()).count();

                if (data_entry->expiry_ms > 0 && now >= entry_expire_time)
                    return get_nil_bulk_string();

                return get_bulk_string(data_entry->value);
            } else {
                std::cerr << "Key doesnt exists: " << args.front() << std::endl;
            }
            return get_nil_bulk_string();
        }

        std::string get_response(std::queue<std::string>& resp_array) {
            auto command = resp_array.front();
            resp_array.pop();
            if (command == "PING")
                return ping();
            if (command == "ECHO")
                return echo(resp_array);
            if (command == "SET")
                return set(resp_array);
            if (command == "GET")
                return get(resp_array);

            throw std::runtime_error("unknown_command");
        }
    };

} // namespace redis