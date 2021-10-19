#pragma once

#include <iostream>

#include <muesli/serializable.hpp>
#include <cereal/types/string.hpp>
#include <whirl/node/time/wall_time.hpp>

struct WriteTimestamp {
  uint64_t value{0};
  int64_t id{0};
  uint64_t local_time{0};

  static WriteTimestamp Min() {
    return {};
  }

  bool operator<(const WriteTimestamp& that) const {
    return std::tie(value, id, local_time) <
           std::tie(that.value, that.id, that.local_time);
  }

  MUESLI_SERIALIZABLE(value, id, local_time)
};

inline std::ostream& operator<<(std::ostream& out, const WriteTimestamp& ts) {
  out << ts.value << ":" << ts.id << ":" << ts.local_time;
  return out;
}