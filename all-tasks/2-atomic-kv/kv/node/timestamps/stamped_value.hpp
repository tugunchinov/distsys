#pragma once

#include <muesli/serializable.hpp>
#include <cereal/types/string.hpp>

#include <kv/node/timestamps/write_timestamp.hpp>

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
