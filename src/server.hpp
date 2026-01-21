#pragma once

#include "logger.hpp"
#include "parser.hpp"
#include "lru.hpp"
#include "utils.hpp"

#include <arpa/inet.h>
#include <functional>
#include <sys/epoll.h>
#include <sys/fcntl.h>
#include <sys/socket.h>
#include <variant>
#include <vector>

namespace redis {
#define MAX_EVENTS 10

    enum DataType {
        STRING,     // Basic type for text or binary data.
        HASH,       // Field-value pairs, useful for representing objects.
        LIST,       // Ordered collection of strings, implemented as a linked list.
        SET,        // Unordered collection of unique strings.
        ZSET,       // Set ordered by a floating-point score.
        STREAM,     // Append-only log for event streaming.
        BITMAP,     // Bit-level operations on strings.
        BITFIELD,   // Efficient encoding of multiple integer fields.
        GEOSPATIAL, // Indexes for geographic data.
        JSON,       // Native support for JSON documents.
        VSET,       // Vector Set: For similarity search with high-dimensional vectors.
        PROB,       // Probabilistic types: Including HyperLogLog, Bloom filters, and more.,
    };

    using RedisValue = std::variant<std::string, std::vector<std::string>>;

    struct DataPoint {
        RedisValue value;
        std::chrono::steady_clock::time_point timestamp;
        unsigned expiry_ms;
    };

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
                    auto event_fd = events[i].data.fd;
                    if (event_fd == server_fd) { // Accept new client connection
                        auto client_fd = accept(server_fd, NULL, NULL);
                        if (client_fd < 0) {
                            throw std::runtime_error("redis server: failed to accept connection");
                        }
                        make_socket_non_blocking(client_fd);
                        event.data.fd = client_fd;
                        event.events = EPOLLIN | EPOLLET;
                        epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &event);
                        clients[client_fd] = ClientConnection{client_fd, ""};
                    } else { // Handle data from client
                        char buf[2048];
                        ssize_t count = read(event_fd, buf, sizeof(buf));
                        if (count == 0) { // EOF - client disconnected
                            close(event_fd);
                            clients.erase(event_fd);
                        } else if (count > 0) {
                            clients[event_fd].buffer += std::string(buf, count);
                            try { // Try to parse from buffer
                                if (has_complete_message(clients[event_fd].buffer)) {
                                    cmd_pipeline = get_resp_array(clients[event_fd].buffer);
                                    // ... process ...
                                    auto response = get_response(cmd_pipeline);
                                    write(event_fd, response.c_str(), response.size()); // Respond
                                    // We do not bulk messages yet: clear buffer
                                    clients[event_fd].buffer.clear();
                                }
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

      private:
        struct ClientConnection {
            int client_fd;
            std::string buffer;
        };
        int server_fd;
        struct sockaddr_in server_addr;
        std::queue<std::string> cmd_pipeline{};
        looneytools::LRUCache<std::string, DataPoint> cache{10};
        std::unordered_map<int, ClientConnection> clients;

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
                std::cerr << "Failed to bind to 6379 \n";
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

        std::string ping() {
            return "+PONG\r\n";
        }

        std::string echo(std::queue<std::string>& args) {
            std::string response{};
            while (!args.empty()) {
                response += get_bulk_string(args.front());
                args.pop();
            }
            return response;
        }

        std::string get(const std::queue<std::string>& args) {
            auto key = args.front();

            if (cache.exists(key)) {
                auto data_ref = cache.get(key);
                auto now = std::chrono::steady_clock::now();
                auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
                auto entry_expire_time = data_ref->get().timestamp + std::chrono::milliseconds(data_ref->get().expiry_ms);
                auto entry_expire_time_ms =
                    std::chrono::duration_cast<std::chrono::milliseconds>(entry_expire_time.time_since_epoch()).count();                
                if (data_ref->get().expiry_ms > 0 && now >= entry_expire_time)
                    return get_nil_bulk_string();

                if (!std::holds_alternative<std::string>(data_ref->get().value)) {
                    return get_simple_string("-WRONGTYPE Operation against a key holding the wrong kind of value");
                }
                return get_bulk_string(std::get<std::string>(data_ref->get().value));
            }

            return get_nil_bulk_string();
        }

        // SET key value
        std::string set(std::queue<std::string>& args) {
            auto key = args.front();
            args.pop();
            auto value = args.front();
            args.pop();

            unsigned expiry_ms = 0; // default value

            auto data_ref = cache.get(key);
            if (data_ref && !std::holds_alternative<std::string>(data_ref->get().value)) {
                return get_simple_string("-WRONGTYPE Operation against a key holding the wrong kind of value");
            }

            // parse options: essentially expiry time
            while (!args.empty()) {
                auto arg = args.front();
                args.pop();
                if (arg == "PX" && !args.empty()) {
                    auto px_ms = args.front();
                    args.pop();
                    int px_ms_int = 0;
                    std::from_chars(px_ms.data(), px_ms.data() + px_ms.size(), px_ms_int);
                    expiry_ms = px_ms_int;
                }
            }

            DataPoint data{value, std::chrono::steady_clock::now(), expiry_ms};
            cache.put(key, data);

            return get_simple_string("+OK");
        }

        std::string push_w_func(
            std::queue<std::string>& args,
            std::function<void(std::vector<std::string>&, const std::string&)> insert_fn) {
            if(args.size() < 2)
                return get_simple_string("-ERR wrong number of arguments for 'rpush' command");

            auto key = args.front();
            args.pop();

            while (!args.empty()) {
                auto value = args.front();
                args.pop();
                unsigned expiry_ms = 0; // default value

                auto data_ref = cache.get(key);
                if(data_ref) {
                    auto list = std::get_if<std::vector<std::string>>(&data_ref->get().value);
                    if (list == nullptr) {
                        return get_simple_string("-WRONGTYPE Operation against a key holding the wrong kind of value");
                    }
                    insert_fn(*list, value);
                } else {
                    DataPoint data{std::vector<std::string>{value}, std::chrono::steady_clock::now(), expiry_ms};
                    cache.put(key, data);
                }
            }

            auto list = std::get_if<std::vector<std::string>>(&cache.get(key)->get().value);
            return get_resp_int(std::to_string(list->size()));
        }
        // RPUSH key value_1 ... value_n
        std::string rpush(std::queue<std::string>& args) {
            return push_w_func(args,
                [](std::vector<std::string>& list, const std::string& value) {
                    list.emplace_back(value);
              });
        }
        // LPUSH key_list value_1 ... value_n (value_n ... value_1)
        std::string lpush(std::queue<std::string>& args) {
            return push_w_func(args,
                [](std::vector<std::string>& list, const std::string& value) {
                    list.insert(list.begin(), value);
              });
        }
        // LRANGE key start stop
        std::string lrange(std::queue<std::string>& args) {
            if(args.size() != 3)
                return get_simple_string("-ERR wrong number of arguments for 'lrange' command");

            auto key = args.front();
            args.pop();

            auto data_ref = cache.get(key);
            if(!data_ref) return get_empty_resp_array();

            auto start = args.front();
            args.pop();
            auto end = args.front();
            args.pop();
            if (!is_integer(start) || !is_integer(end))
                return get_simple_string("-ERR value is not an integer or out of range");
            
            auto start_i = std::stoi(start);
            auto end_i = std::stoi(end);

            auto list = std::get_if<std::vector<std::string>>(&data_ref->get().value);
            if (list == nullptr) {
                return get_simple_string("-WRONGTYPE Operation against a key holding the wrong kind of value");
            }
            // list[0,1,2,3]: size() = 4
            // lrange list 2 4: "*2\r\n1\r\n2\r\n$1\r\n3\r\n"
            start_i = start_i < 0 ? list->size() + start_i : start_i;
            end_i = end_i < 0 ? list->size() + end_i : end_i;

            // Check if range is valid
            if (start_i > end_i || start_i >= (int)list->size() || end_i < 0) {
                return get_empty_resp_array();
            }
            // Clamp to list bounds
            start_i = std::max(0, start_i);
            end_i = std::min(end_i, (int)list->size() - 1);

            auto result = "*" + std::to_string(end_i - start_i + 1) + "\r\n";
            for (auto i = start_i; i<=end_i; ++i) {
                result += "$" + std::to_string(list->at(i).size()) + "\r\n";
                result += list->at(i) + "\r\n";
            }
            return result;
        }

        std::string get_response(std::queue<std::string>& resp_array) {
            auto command = resp_array.front();
            resp_array.pop();
            if (command == "ECHO")
                return echo(resp_array);
            if (command == "PING")
                return ping();
            if (command == "GET")
                return get(resp_array);
            if (command == "LPUSH")
                return lpush(resp_array);
            if (command == "LRANGE")
                return lrange(resp_array);
            if (command == "SET")
                return set(resp_array);
            if (command == "RPUSH")
                return rpush(resp_array);

            throw std::runtime_error("unknown_command");
        }

        std::string get_nil_bulk_string() {
            return "$-1\r\n";
        }
        std::string get_bulk_string(const std::string& s) {
            return "$" + std::to_string(s.length()) + "\r\n" + s + "\r\n";
        }
        std::string get_simple_string(const std::string& s) {
            return s + "\r\n";
        }
        std::string get_resp_int(const std::string& s) {
            return ":" + s + "\r\n";
        }
        std::string get_empty_resp_array() {
            return "*0\r\n";
        }
        int make_socket_non_blocking(int socket_fd) {
            int flags = fcntl(socket_fd, F_GETFL, 0);
            if (flags == -1)
                return -1;
            flags |= O_NONBLOCK;
            return fcntl(socket_fd, F_SETFL, flags);
        }
        std::optional<std::string> has_expected_args(std::queue<std::string>& args, const size_t expected) {
          if(args.size() < expected)
              return "-ERR wrong number of arguments";
          return std::nullopt;
      }
    };

} // namespace redis