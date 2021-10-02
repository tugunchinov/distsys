#pragma once

#include <iostream>

#include <muesli/serializable.hpp>
#include <cereal/types/string.hpp>

struct WriteTimestamp {
  uint64_t value;

  static WriteTimestamp Min() {
    return {0};
  }

  bool operator<(const WriteTimestamp& that) const {
    return value < that.value;
  }

  WriteTimestamp operator+(uint64_t other) {
    WriteTimestamp ts = *this;
    ts.value += other;
    return ts;
  }

  MUESLI_SERIALIZABLE(value)
};

inline std::ostream& operator<<(std::ostream& out, const WriteTimestamp& ts) {
  out << ts.value;
  return out;
}