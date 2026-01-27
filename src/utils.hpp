#pragma once

#include <string>

bool is_integer(const std::string& s) {
    if (s.empty()) {
        return false;
    }
    size_t start = 0;
    if (s[0] == '+' || s[0] == '-') {
        if (s.length() == 1) {
            return false; // sign alone is invalid
        }
        start = 1;
    }
    return std::all_of(s.begin() + start, s.end(), ::isdigit);
}