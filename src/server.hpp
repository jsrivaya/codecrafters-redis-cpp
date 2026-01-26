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

#include <queue>
#include <string>
#include <string_view>
#include <unordered_set>
#include <variant>
#include <vector>

namespace redis {
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
            while (true) {
                cleanup_timeout_clients();
                const auto n_fds = wait_for_event();
                for (size_t i = 0; i < n_fds; ++i) {
                    auto event_fd = events[i].data.fd;
                    if (event_fd == server_fd) {
                        handle_new_connection();
                    } else {
                        process_client_event(event_fd);
                    }
                }
            }
        }

      private:
        static constexpr size_t MAX_EVENTS = 10;
        static constexpr size_t MAX_CLIENTS = 10000;
        static constexpr size_t MAX_BUFFER_SIZE = 512 * 1024;  // 512KB limit
        static constexpr size_t CACHE_SIZE = 10000;

        struct ClientConnection {
            int client_fd;
            std::string buffer;
        };

        int server_fd;
        int epoll_fd;
        epoll_event events[MAX_EVENTS];
        struct sockaddr_in server_addr;
        looneytools::LRUCache<std::string, DataPoint> cache{CACHE_SIZE};
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

            // initialize epoll events
            init_epoll();
        };

        RedisServer(const RedisServer&) = delete;
        RedisServer& operator=(const RedisServer&) = delete;
        ~RedisServer() {
            if (epoll_fd != -1) close(epoll_fd);
            if (server_fd != -1) close(server_fd);
        }

        void init_epoll() {
            epoll_fd = epoll_create1(0); // Creates an epoll instance
            epoll_event event;
            event.data.fd = server_fd;
            event.events = EPOLLIN; // Monitor for incoming connections only
            epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &event);
        }

        int wait_for_event() {
            // Wait until earliest blocked client timeout
            auto timeout_ms = -1;
            if (!blocked_clients_pq.empty()) {
                auto now = std::chrono::steady_clock::now();
                auto earliest_timeout = blocked_clients_pq.top().timeout_point;
                auto duration = earliest_timeout - now;
                timeout_ms = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
                timeout_ms = std::max(0, timeout_ms);  // Clamp to 0 if negative
            }
            return epoll_wait(epoll_fd, events, MAX_EVENTS, timeout_ms);
        }

        void register_client(const int fd) {
            epoll_event event;
            event.data.fd = fd;
            event.events = EPOLLIN | EPOLLET;
            epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &event);
            clients[fd] = ClientConnection{fd, ""};
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
            const auto key = args.front();

            if (cache.exists(key)) {
                auto data_ref = cache.get(key);
                auto& data = data_ref->get();
                auto now = std::chrono::steady_clock::now();
                auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
                auto entry_expire_time = data.timestamp + std::chrono::milliseconds(data.expiry_ms);             
                if (data.expiry_ms > 0 && now >= entry_expire_time)
                    return get_null_bulk_string();

                if (!std::holds_alternative<std::string>(data.value)) {
                    return get_simple_string("-WRONGTYPE Operation against a key holding the wrong kind of value");
                }
                return get_bulk_string(std::get<std::string>(data.value));
            }

            return get_null_bulk_string();
        }

        // SET key value
        std::string set(std::queue<std::string>& args) {
            const auto key = args.front();
            args.pop();
            auto value = args.front();
            args.pop();

            unsigned expiry_ms = 0; // default value

            if(auto data_ref = cache.get(key);
                data_ref && !std::holds_alternative<std::string>(data_ref->get().value)) {
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
            cache.put(key, DataPoint{value, std::chrono::steady_clock::now(), expiry_ms});

            return get_simple_string("+OK");
        }

        std::string push_w_func(
            std::queue<std::string>& args,
            std::function<void(std::vector<std::string>&, const std::string&)> insert_fn) {
            if(args.size() < 2) {
                return get_simple_string("-ERR wrong number of arguments for 'push' command");
            }

            const auto key = args.front();
            args.pop();

            while (!args.empty()) {
                auto value = args.front();
                args.pop();

                if (key_to_blocked_clients.contains(key)) {
                    unblock_and_handoff_value(key, value);
                    continue;
                }

                unsigned expiry_ms = 0; // default value
                if(auto data_ref = cache.get(key); data_ref) {
                    auto& data = data_ref->get();
                    auto list = std::get_if<std::vector<std::string>>(&data.value);
                    if (list == nullptr) {
                        return get_simple_string("-WRONGTYPE Operation against a key holding the wrong kind of value");
                    }
                    insert_fn(*list, value);
                } else {
                    cache.put(key, DataPoint{std::vector<std::string>{value}, std::chrono::steady_clock::now(), expiry_ms});
                }
            }

            if(auto data_ref = cache.get(key); data_ref) {
                auto list = std::get_if<std::vector<std::string>>(&cache.get(key)->get().value);
                return get_resp_int(std::to_string(list->size()));
            }
            return get_resp_int("1");
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

        // LLEN key
        std::string llen(std::queue<std::string>& args) {
            if (args.size() != 1) {
                return get_simple_string("-ERR wrong number of arguments for 'llen' command");
            }

            const auto key = args.front();
            if (auto data_ref = cache.get(key); data_ref) {
                auto& data = data_ref->get();
                auto list = std::get_if<std::vector<std::string>>(&data.value);
                if (list == nullptr) {
                    return get_simple_string("-WRONGTYPE Operation against a key holding the wrong kind of value");
                }
                return get_resp_int(std::to_string(list->size())); 
            }
            return get_resp_int("0");
        }

        // BLPOP key1 key2 ... timeout
        std::optional<std::string> blpop(const int fd, std::queue<std::string>& args) {
            std::vector<std::string> keys;
            while (args.size() > 1) {
                keys.emplace_back(std::move(args.front()));
                args.pop();
            }
            if (keys.empty()) {
                return get_simple_string("-ERR wrong number of arguments for 'blpop' command");
            }

            size_t size;
            auto arg = args.front();
            args.pop();

            auto timeout_s = std::stod(arg, &size);
            if (size != arg.length()) {
                return get_simple_string("-ERR timeout is not a valid float");
            }
            std::chrono::steady_clock::time_point timeout_point;
            if (timeout_s == 0) {
                // Block forever - use maximum time_point
                timeout_point = std::chrono::steady_clock::time_point::max();
            } else {
                // Block for specified duration
                auto timeout_ms = std::chrono::milliseconds(static_cast<long long>(timeout_s * 1000));
                timeout_point = std::chrono::steady_clock::now() + timeout_ms;
            }

            for (const auto& key : keys) {
                // pop and return the first value available in any of the keys
                if (auto data_ref = cache.get(key); data_ref) {
                    auto& data = data_ref->get();
                    auto list = std::get_if<std::vector<std::string>>(&data.value);
                    if (!list) {
                        return get_simple_string("-WRONGTYPE Operation against a key holding the wrong kind of value");
                    }
                    if (!list->empty()) {
                        const auto value = std::move(list->front());
                        list->erase(list->begin());
                        return get_resp_array_string({key,value});
                    }
                }
            }
            // none of the lists has values - block
            block_client(fd, keys, timeout_point);

            return "";  // Don't send response yet  
        }

        // LPOP key
        std::string lpop(std::queue<std::string>& args) {
            if (args.size() > 2) {
                return get_simple_string("-ERR wrong number of arguments for 'lpop' command");
            }
            auto npop = 1; // default number of elements to pop

            const auto key = args.front();
            args.pop();

            if (!args.empty()) {
                auto arg = args.front();
                size_t pos;
                npop = std::stoi(arg, &pos);
                if(pos != arg.length()) {
                    return get_simple_string("-WRONGTYPE Operation requesting the wrong kind of argument");
                }
                if (npop <= 0) {
                    return get_simple_string("-ERR value is out of range, must be positive");
                }
            }

            if (auto data_ref = cache.get(key); data_ref) {
                auto& data = data_ref->get();
                auto list = std::get_if<std::vector<std::string>>(&data.value);
                if (list == nullptr) {
                    return get_simple_string("-WRONGTYPE Operation against a key holding the wrong kind of value");
                }
                if (list->empty()) {
                    cache.remove(key);
                    return get_null_bulk_string();
                }

                if (npop > list->size()) {
                    npop = list->size();
                }
                std::vector<std::string> sublist(
                    std::make_move_iterator(list->begin()),
                    std::make_move_iterator(list->begin() + npop)
                );
                list->erase(list->begin(), list->begin() + npop);

                if (sublist.size() > 1) {
                    return get_resp_array_string(sublist);
                }
                return get_bulk_string(sublist.front());
            }
            return get_null_bulk_string();
        }

        // LRANGE key start stop
        std::string lrange(std::queue<std::string>& args) {
            if(args.size() != 3) {
                return get_simple_string("-ERR wrong number of arguments for 'lrange' command");
            }

            const auto key = args.front();
            args.pop();

            auto data_ref = cache.get(key);
            if(!data_ref) { return get_empty_resp_array(); }

            auto start = args.front();
            args.pop();
            auto end = args.front();
            args.pop();
            if (!is_integer(start) || !is_integer(end))
                return get_simple_string("-ERR value is not an integer or out of range");
            
            auto start_i = std::stoi(start);
            auto end_i = std::stoi(end);

            auto& data = data_ref->get();
            auto list = std::get_if<std::vector<std::string>>(&data.value);
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
        int make_socket_non_blocking(int socket_fd) {
            int flags = fcntl(socket_fd, F_GETFL, 0);
            if (flags == -1)
                return -1;
            flags |= O_NONBLOCK;
            return fcntl(socket_fd, F_SETFL, flags);
        }
        void remove_client(const int fd) {
            close(fd);
            clients.erase(fd);
            cleanup_blocked_client(fd);
        }

        void handle_new_connection() {
            if (clients.size() >= MAX_CLIENTS) {
                // consume the request and return error
                return handle_max_connections();
            }
            auto client_fd = accept(server_fd, NULL, NULL);
            if (client_fd < 0) {
                throw std::runtime_error("redis server: failed to accept connection");
            }
            make_socket_non_blocking(client_fd);
            register_client(client_fd);
        }

        void handle_max_connections() {
            if (auto fd = accept(server_fd, NULL, NULL); fd >= 0) {
                constexpr std::string_view error = "-ERR max number of clients reached\r\n";
                write(fd, error.data(), error.size());
                close(fd);
            } else {
                throw std::runtime_error("redis server: failed to handle max connections");
            }
        }

        std::optional<std::string> handle_client_message(const int client_fd, const std::string& message) {
            auto request = get_request(message);

            auto command = request.front();
            request.pop();
            if (command == "BLPOP")
                return blpop(client_fd, request);
            if (command == "ECHO")
                return echo(request);
            if (command == "PING")
                return ping();
            if (command == "GET")
                return get(request);
            if (command == "LLEN")
                return llen(request);
            if (command == "LPOP")
                return lpop(request);
            if (command == "LPUSH")
                return lpush(request);
            if (command == "LRANGE")
                return lrange(request);
            if (command == "SET")
                return set(request);
            if (command == "RPUSH")
                return rpush(request);

            throw std::runtime_error("unknown_command");
        }
        struct BlockedClient {
            int fd;
            std::chrono::steady_clock::time_point timeout_point;
            std::vector<std::string> keys;  // All keys this client is waiting on
            bool operator>(const BlockedClient& other) const {
                return timeout_point > other.timeout_point;
            }
        };
        // 1. Priority queue ordered by timeout (earliest timeout at top)
        std::priority_queue<BlockedClient, std::vector<BlockedClient>, std::greater<BlockedClient>> blocked_clients_pq;
        // 2. Multimap: key -> client_fd (for quick lookup when a key gets data)
        std::unordered_multimap<std::string, int> key_to_blocked_clients;
        // 3. Set of blocked fds (for quick existence checks)
        std::unordered_set<int> blocked_fds;
        // 4. client fd to key for quick find of blocked clients
        std::unordered_map<int, std::vector<std::string>> fd_to_keys;

        void block_client(const int fd, std::vector<std::string>& keys, const std::chrono::steady_clock::time_point timeout_point) {
            blocked_clients_pq.push(BlockedClient{fd, timeout_point, keys});
            blocked_fds.insert(fd);
            fd_to_keys[fd] = keys;

            for (const auto& key : keys) {  // Register for ALL keys, even non-existent
                key_to_blocked_clients.emplace(key, fd);
            }
        }
        void unblock_and_handoff_value(const std::string& key, const std::string& value) {
            auto range = key_to_blocked_clients.equal_range(key);
            for (auto it = range.first; it != range.second; ++it) {
                auto fd = it->second;
                if (blocked_fds.count(fd)) {
                    send_to_client(fd, get_resp_array_string({key, value}));
                    cleanup_blocked_client(fd);
                    break;
                }
            }
        }
        void cleanup_blocked_client(const int fd) {
            // Remove this client from ALL key registrations
            // 1. priority_queue: blocked_clients_pq
            // Lazy clean up for the priority queue

            // 2. unordered_multimap: key -> client fd
            auto it = fd_to_keys.find(fd);
            if (it == fd_to_keys.end()) return;
            for (const auto& key : it->second) {
                auto range = key_to_blocked_clients.equal_range(key);
                for (auto kt = range.first; kt != range.second; ) {
                    if (kt->second == fd) {
                        kt = key_to_blocked_clients.erase(kt);
                        break;
                    }
                    ++kt;
                }
            }
            // 3. unordered_set: blocked fds
            blocked_fds.erase(fd);
            // 4. unordered_map: client fd -> key
            fd_to_keys.erase(fd);
        }
        void cleanup_timeout_clients() {
            while (!blocked_clients_pq.empty()) {
                auto& blocked = blocked_clients_pq.top();

                // Check if already expired
                if (blocked.timeout_point > std::chrono::steady_clock::now()) {
                    break;  // No more timeouts yet
                }

                // Timeout expired - but check if client is still actually blocked
                if (blocked_fds.count(blocked.fd)) {
                    // Client is still blocked - send timeout response
                    send_to_client(blocked.fd, get_null_resp_array());
                    cleanup_blocked_client(blocked.fd);
                }
                // else: Client was already unblocked by data arrival - skip

                blocked_clients_pq.pop();  // Remove (either handled or stale)
            }
        }

        void process_client_event(const int fd) {
            char received_buf[2048];

            ssize_t count = read(fd, received_buf, sizeof(received_buf));
            if (count == 0) { // EOF - client disconnected
                remove_client(fd);
                return;
            }

            if (count > 0) {
                if (clients[fd].buffer.size() + count > MAX_BUFFER_SIZE) {
                    std::cerr << "Buffer limit exceeded for client " << fd << std::endl;
                    remove_client(fd);
                    return;
                }
                clients[fd].buffer += std::string(received_buf, count);
                try {
                    if (has_complete_message(clients[fd].buffer)) {
                        auto response_opt = handle_client_message(fd, clients[fd].buffer);

                        // Only send response if we have one (non-blocking command)
                        if (response_opt.has_value()) {
                            send_to_client(fd, response_opt.value());
                        }
                        clients[fd].buffer.clear();
                    }
                } catch (const std::exception& e) {
                    std::cerr << e.what() << std::endl;
                }
            } else {
                std::cerr << "error on read: " << errno << std::endl;
            }
        }
        void send_to_client(const int& fd, const std::string& message) {
            if (write(fd, message.c_str(), message.size()) < 0) { // Respond back to client
                // Client likely disconnected, clean up
                std::cerr << "Failed to write to client " << fd << std::endl;
                remove_client(fd);
            }
        }
    };

} // namespace redis