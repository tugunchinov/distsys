#pragma once

#include <iostream>

#include <muesli/serializable.hpp>
#include <cereal/types/string.hpp>
#include <whirl/node/time/wall_time.hpp>

struct WriteTimestamp {
  uint64_t time{0};
  std::string guid{};

  static WriteTimestamp Min() {
    return {};
  }

  bool operator<(const WriteTimestamp& that) const {
    return std::tie(time, guid) < std::tie(that.time, that.guid);
  }

  MUESLI_SERIALIZABLE(time, guid)
};

inline std::ostream& operator<<(std::ostream& out, const WriteTimestamp& ts) {
  out << ts.time << '{' << ts.guid << '}';
  return out;
}