#pragma once

#include <iostream>
#include <mutex>
#include <string>
#include <chrono>
#include <unistd.h>

namespace redis {

    class Logger {
      public:
        enum Level { INFO, DEBUG, WARN, ERROR };

        static Logger& getInstance() {
            static Logger instance;
            return instance;
        }

        void enable(const Level l) {
            enabled = true;
            level = l;
        }
        void disable() {
            enabled = false;
        }
        void set_level(Level l) {
            level = l;
        }
        std::string get_mode() {
            switch (level) {
                case Level::ERROR:
                    return "ERROR";
                    break;
                case Level::INFO:
                    return "INFO";
                    break;
                case Level::WARN:
                    return "WARN";
                    break;
                case Level::DEBUG:
                    return "DEBUG";
                    break;
                default:
                    return "INFO";
                    break;
            }
        }

        void log(const std::string& tag = "", const std::string& str = "") {
            if (level >= Level::INFO) {
                print(tag, str);
            }
        }

        void warn(const std::string& tag = "", const std::string& str = "") {
            if (level >= Level::WARN) {
                print(tag, str);
            }
        }

        void debug(const std::string& tag = "", const std::string& str = "") {
            if (level >= Level::DEBUG) {
                print(tag, str);
            }
        }

        void error(const std::string& tag = "", const std::string& str = "") {
            if (level >= Level::ERROR) {
                print(tag, str);
            }
        }

      private:
        Logger() {
            // // Flush after every std::cout / std:cerr
            std::cout << std::unitbuf;
            std::cerr << std::unitbuf;
        };
        Logger& operator=(const Logger) = delete;

        bool enabled = false;
        Level level = Level::INFO;

        std::mutex print_mtx;
        void print(const std::string& tag = "", const std::string& str = "") {
            std::lock_guard<std::mutex> lock(print_mtx);
            auto pid = std::to_string(getpid());
            auto output = "[" + pid + "]\t" + get_mode() + ": " + tag + "\t" + str;
            std::cerr << output << std::endl;
        }
    };

} // namespace redis