#pragma once

#include "logger.hpp"
#include "redis_list.hpp"

#include <string>
#include <variant>

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

    using RedisValue = std::variant<std::string, RedisList<std::string>>;

    struct DataPoint {
        RedisValue value;
        DataType datatype;
        std::chrono::steady_clock::time_point timestamp;
        unsigned expiry_ms;
    };

} // namespace redis