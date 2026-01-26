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
    // receives a resp string and an int index pointing to the first character to skip
    void skip_crlf(const std::string& resp, int& index) {
        if (index < resp.length() && resp[index] == '\r') {
            ++index;
            if (index < resp.length() && resp[index] == '\n') {
                ++index;
                return;
            }
        }
        // TOOD: there is the possibility of this being a partial read

        throw std::runtime_error("parser_error");
    }

    int get_array_size(const std::string& resp, int& index) {
        constexpr int MAX_ARRAY_SIZE = 1000000;
        if (index < resp.length() && resp[index] == '*') {
            ++index;

            int n = 0;
            while (index < resp.length() && std::isdigit(resp[index])) {
                int digit = resp[index] - '0';
                if (n > MAX_ARRAY_SIZE / 10 ||
                    (n == MAX_ARRAY_SIZE / 10 && digit > MAX_ARRAY_SIZE % 10)) {
                    throw std::runtime_error("Array size exceeds maximum allowed");
                }
                n = n * 10 + digit;
                ++index;
            }
            skip_crlf(resp, index);

            return n;
        }

        return -1;
    }

    std::string get_element(const std::string& resp, int& index) {
        // 1. read element identifier {+, -, :, *, $}
        std::string valid = "+-:$*";
        if (index < resp.length() && valid.find(resp[index]) == std::string::npos)
            throw std::runtime_error("parser_error"); // TODO: this could be a partial read

        ++index;

        // 2. read 'n'
        int n = 0;
        constexpr int MAX_ARRAY_SIZE = 1000000;
        while (index < resp.length() && std::isdigit(resp[index])) {
            int digit = resp[index] - '0';
            if (n > MAX_ARRAY_SIZE / 10 ||
                (n == MAX_ARRAY_SIZE / 10 && digit > MAX_ARRAY_SIZE % 10)) {
                throw std::runtime_error("Array size exceeds maximum allowed");
            }
            n = n * 10 + digit;
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

        return element;
    }

    // process incoming message, convert it from string to vector of strings
    // from "*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n"
    // to ["ECHO", "hey"];
    // RESP strings can include multiple arrays
    // each array represent one command and its arguments
    // in the future this should read resp arrays (commands)
    std::queue<std::string> get_request(const std::string& resp) {
        // read "*"
        // read n
        // read n bytes
        std::queue<std::string> resp_array{};

        int i = 0;
        while (i < resp.length()) {
            // 1. read array size
            auto n = get_array_size(resp, i);

            // 2. consume 'n' array elements: command and arguments
            while (n > 0 && i < resp.length()) {
                // read element
                auto element = get_element(resp, i);
                resp_array.emplace(element);
                --n;
            }
        }
        return resp_array;
    }

    // complete message:
    // "*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n"
    bool has_complete_message(const std::string& buffer) {
        if (buffer.empty() || buffer[0] != '*')
            return false;

        size_t pos = 1;

        size_t array_size = 0;
        constexpr int MAX_ARRAY_SIZE = 1000000;
        while (pos < buffer.size() && std::isdigit(buffer[pos])) {
            if (array_size > MAX_ARRAY_SIZE / 10) {
                return false;  // Invalid message, too large
            }
            array_size = array_size * 10 + (buffer[pos] - '0');
            ++pos;
        }
        if (pos + 2 > buffer.size() || buffer.substr(pos, 2) != "\r\n")
            return false;

        pos += 2;
        // ensure all message parameters are present in the buffer
        for (size_t i = 0; i < array_size; ++i) {
            if (pos >= buffer.size() || buffer[pos] != '$')
                return false;
            ++pos;

            auto param_size = 0;
            while (pos < buffer.size() && std::isdigit(buffer[pos])) {
                if (param_size > MAX_ARRAY_SIZE / 10) {
                    return false;  // Invalid message
                }
                param_size = param_size * 10 + (buffer[pos] - '0');
                ++pos;
            }
            if (pos + 2 > buffer.size() || buffer.substr(pos, 2) != "\r\n")
                return false;

            pos += 2;

            if (pos + param_size + 2 > buffer.size())
                return false;

            pos += param_size + 2;
        }

        return true;
    }
    std::string get_null_bulk_string() {
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
    std::string get_resp_array_string(const std::vector<std::string>& elements) {
        auto resp_array = "*" + std::to_string(elements.size()) + "\r\n";
        for (const auto& e : elements) {
            resp_array += get_bulk_string(e);
        }
        return resp_array;
    }
    std::string get_empty_resp_array() {
        return "*0\r\n";
    }
    std::string get_null_resp_array() {
        return "*-1\r\n";
    }
} // namespace redis