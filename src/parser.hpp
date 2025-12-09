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
        // TOOD: there is the possibility of this being a parcial read

        throw std::runtime_error("parser_error");
    }

    int get_array_size(const std::string& resp, int& index) {
        Logger::getInstance().debug(__func__, "resp: " + resp + "; index: " + std::to_string(index));
        if (index < resp.length() && resp[index] == '*') {
            ++index;

            int n = 0;
            while (index < resp.length() && std::isdigit(resp[index])) {
                int digit = resp[index] - '0';
                n += n * 10 + digit;
                ++index;
            }
            skip_crlf(resp, index);

            return n;
        }

        return -1;
    }

    std::string get_element(const std::string& resp, int& index) {
        Logger::getInstance().debug(__func__, "resp: " + resp + "; index: " + std::to_string(index));
        // 1. read element identifier {+, -, :, *, $}
        std::string valid = "+-:$*";
        if (index < resp.length() && valid.find(resp[index]) == std::string::npos)
            throw std::runtime_error("parser_error"); // TODO: this could be a partial read

        ++index;

        // 2. read 'n'
        int n = 0;
        while (std::isdigit(resp[index])) {
            int digit = resp[index] - '0';
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
    std::queue<std::string> get_resp_array(const std::string& resp) {
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
} // namespace redis