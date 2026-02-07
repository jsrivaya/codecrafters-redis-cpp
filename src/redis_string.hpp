#pragma once

#include <string>
#include <vector>

namespace redis {

std::string get_null_bulk_string() {
    return "$-1\r\n";
}
std::string get_bulk_string(const std::string& s) {
    return "$" + std::to_string(s.length()) + "\r\n" + s + "\r\n";
}
std::string get_simple_string(const std::string& s) {
    return "+" + s + "\r\n";
}
std::string get_error_string(const std::string& s) {
    return "-" + s + "\r\n";
}
std::string get_resp_int(const std::string& s) {
    return ":" + s + "\r\n";
}
std::string get_empty_resp_array() {
    return "*0\r\n";
}
std::string get_resp_array_string(const std::vector<std::string>& elements) {
    if (elements.empty()) {
        return get_empty_resp_array();
    }
    auto resp_array = "*" + std::to_string(elements.size()) + "\r\n";
    for (const auto& e : elements) {
        resp_array += get_bulk_string(e);
    }
    return resp_array;
}
std::string get_null_resp_array() {
    return "*-1\r\n";
}
} // namespace redis