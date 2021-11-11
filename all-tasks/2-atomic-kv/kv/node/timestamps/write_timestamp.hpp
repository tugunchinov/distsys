#pragma once

#include <iostream>

#include <muesli/serializable.hpp>
#include <cereal/types/string.hpp>
#include <whirl/node/time/wall_time.hpp>

struct WriteTimestamp {
  uint64_t e{0};
  uint64_t l{0};
  std::string guid{};

  static WriteTimestamp Min() {
    return {};
  }

  bool operator<(const WriteTimestamp& that) const {
    return std::tie(l, e, guid) < std::tie(that.l, that.e, that.guid);
  }

  MUESLI_SERIALIZABLE(e, l, guid)
};

inline std::ostream& operator<<(std::ostream& out, const WriteTimestamp& ts) {
  out << "[" << ts.e << ", " << ts.l << "]{" << ts.guid << "}";
  return out;
}