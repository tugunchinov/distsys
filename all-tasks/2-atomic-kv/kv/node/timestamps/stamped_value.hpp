#pragma once

#include <node/timestamps/write_timestamp.hpp>

// Replicas store versioned (stamped) values

using Value = std::string;

struct StampedValue {
  Value value;
  WriteTimestamp timestamp;

  MUESLI_SERIALIZABLE(value, timestamp)
};

inline std::ostream& operator<<(std::ostream& out,
                                const StampedValue& stamped_value) {
  out << "{" << stamped_value.value << ", ts: " << stamped_value.timestamp
      << "}";
  return out;
}
