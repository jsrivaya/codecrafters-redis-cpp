#pragma once

#include "logger.hpp"

#include <iostream>
#include <charconv>
#include <chrono>
#include <queue>
#include <list>
#include <string>
#include <unordered_map>

namespace redis {

#define MAX_EVENTS 10
#define PORT 8080

struct Data {
    std::string value;
    std::chrono::steady_clock::time_point timestamp;
    unsigned expiry_ms;
};

std::list<Data> store{};
std::unordered_map<std::string, std::list<Data>::iterator> lookup_table{};

// receives a resp string and an int index pointing to the first character to skip
void skip_crlf(const std::string& resp, int& index) {
    Logger::getInstance().debug(__func__, "resp: " + resp + "; index: " + std::to_string(index));
    if (index < resp.length() && resp[index] == '\r') {
        Logger::getInstance().debug(__func__, "resp[index]: " + resp[index]);
        ++index;
        if (index < resp.length() && resp[index] == '\n') {
            ++index;
            return;
        }
    }
    // TOOD: there is the possibility of this being a parcial read

    throw std::runtime_error("parser_error: index=" + std::to_string(index));
}

int get_array_size(const std::string& resp, int& index) {
    Logger::getInstance().debug(__func__, "resp: " + resp + "; index: " + std::to_string(index));
    int n = 0;
    if(index < resp.length() && resp[index] == '*') {
        ++index;

        while(index < resp.length() && std::isdigit(resp[index])) {
            int digit = resp[index] - '0';
            n += n * 10 + digit;
            ++index;
        }
        skip_crlf(resp, index);

        return n;
    }

    return -1;
}

std::pair<std::string, int> get_element(const std::string& resp, int index) {
    Logger::getInstance().debug(__func__, "resp: " + resp + "; index: " + std::to_string(index));
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
    skip_crlf(resp, index);

    // 4. read element: read 'n' bytes
    std::string element{};
    while (n > 0) {
        element += resp[index];
        ++index;
        --n;
    }

    // 5. skip crlf
    skip_crlf(resp, index);

    return std::pair<std::string, int> {element, index};
}

// process incoming message, convert it from string to vector of strings
// from "*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n"
// to ["ECHO", "hey"];
// RESP strings can include multiple arrays
// each array represent one command and its arguments
// in the future this should read resp arrays (commands)
std::queue<std::string> get_resp_array(const std::string& resp) {
    // read "*"
    // read n
    // read n bytes
    std::queue<std::string> resp_array{};
    for (int i = 0; i < resp.length(); ++i) {
        // 1. read array size
        auto n = get_array_size(resp, i);

        // 2. consume 'n' array elements: command and arguments
        while(n > 0 && i < resp.length()) {
            // read element
            auto element = get_element(resp, i);
            resp_array.emplace(element.first);
            i = element.second;
            --n;
        }
    }
    return resp_array;
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
    while(!args.empty()) {
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
    while(!args.empty()) {
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
        auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
    now.time_since_epoch()).count();
        auto entry_expire_time = data_entry->timestamp + std::chrono::milliseconds(data_entry->expiry_ms);
        auto entry_expire_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
    entry_expire_time.time_since_epoch()).count();

        if(data_entry->expiry_ms > 0 && now >= entry_expire_time)
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
    if (command == "PING") return ping();
    if (command == "ECHO") return echo(resp_array);
    if (command == "SET") return set(resp_array);
    if (command == "GET") return get(resp_array);

    throw std::runtime_error("unknown_command");
}

} // namespace redis