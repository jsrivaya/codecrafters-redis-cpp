#include <iostream>
#include <cstdlib>
#include <cctype>
#include <deque>
#include <string>
#include <cstring>
#include <unistd.h>
#include <sys/epoll.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <sys/fcntl.h>

#define MAX_EVENTS 10
#define PORT 8080

int make_socket_non_blocking(int socket_fd) {
    int flags = fcntl(socket_fd, F_GETFL, 0);
    if (flags == -1)
        return -1;
    flags |= O_NONBLOCK;
    return fcntl(socket_fd, F_SETFL, flags);
}

int get_array_size(const std::string& resp, int index) {
    int n = 0;
    while(std::isdigit(resp[index])) {
        int digit = resp[index] - '0';
        n += n * 10 + digit;
        ++index;
    }
    return n;
}

// receives a resp string and an int index pointing to the first character to skip
int skip_crlf(const std::string& resp, int index) {
    if (index < resp.length() && resp[index] == '\r') {
        ++index;
        if (index < resp.length() && resp[index] == '\n') {
            return ++index;
        }
    }
    // TOOD: there is the possibility of this being a parcial read

    throw std::runtime_error("parser_error");    
}

std::pair<std::string, int> get_element(const std::string& resp, int index) {
    // 1. read element identifier {+, -, :, *, $}
    std::string valid = "+-:$*";
    if (index < resp.length() && valid.find(resp[index]) == std::string::npos)
        throw std::runtime_error ("parser_error"); // TODO: this could be a partial read

    ++index;

    // 2. read 'n'
    int n = 0;
    while (std::isdigit(resp[index])) {
        int digit = resp[index] - '0';
        n += n * 10 + digit;
        ++index;
    }

    // 3. skip crlf
    index = skip_crlf(resp, index);

    // 4. read element
    std::string element{};
    while (n > 0) {
        element += resp[index];
        ++index;
        --n;
    }

    // 5. skip crlf
    index = skip_crlf(resp, index);

    return std::pair<std::string, int> {element, index};
}

// process incoming message, convert it from string to vector of strings
// from "*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n"
// to ["ECHO", "hey"];
// RESP strings can include multiple arrays
// each array represent one command and its arguments
std::deque<std::string> get_resp_array(const std::string& resp) {
    // read "*"
    // read n
    // read n bytes
    std::deque<std::string> resp_array{};
    for (int i = 0; i < resp.length(); ++i) {
        if (resp[i] == '*') { // 1. new array: contains 'n' elements
            ++i;
            int n = 0;
            while(std::isdigit(resp[i])) {
                int digit = resp[i] - '0';
                n += n * 10 + digit;
                ++i;
            }

            // 2. skip first crlf
            i = skip_crlf(resp, i); // "\r\n"

            // 3. consume 'n' elements: command and arguments
            while(n > 0 && i < resp.length()) {
                // read element
                auto element = get_element(resp, i);
                resp_array.emplace_back(element.first);
                i = element.second;
            }
        }
    }
    return resp_array;
}

std::string ping() {
    return "+PONG\r\n";
}

std::string get_bulk_string(const std::string& s) {
    return "$" + std::to_string(s.length()) + "\r\n" + s + "\r\n";
}

std::string echo(std::deque<std::string>& command) {
    command.pop_front(); // remove the "echo"
    std::string response{};
    while(!command.empty()) {
        response += get_bulk_string(command.front());
        command.pop_front();
    }
    return response;
}

std::string get_response(std::deque<std::string>& resp_array) {
    if (resp_array.at(0) == "PING") return ping();
    if (resp_array.at(0) == "ECHO") return echo(resp_array);

    throw std::runtime_error("unknown_command");
}

int main(int argc, char** argv) {
    // Flush after every std::cout / std::cerr
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;

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

    if (bind(server_fd, (struct sockaddr*) &server_addr, sizeof(server_addr)) != 0) {
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
                char buf[1024];
                ssize_t count = read(events[i].data.fd, buf, sizeof(buf));
                if (count == 0) { // EOF
                    close(events[i].data.fd);
                } else if (count > 0) {
                    try {
                        auto resp_array = get_resp_array(buf);
                        auto response = get_response(resp_array);
                        write(events[i].data.fd, response.c_str(), response.size()); // Echo back
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
