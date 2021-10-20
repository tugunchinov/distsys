#pragma once

#include <muesli/serializable.hpp>

// Enable string serialization
#include <cereal/types/string.hpp>

#include <string>
#include <ostream>

namespace paxos {

////////////////////////////////////////////////////////////////////////////////

using Value = std::string;

////////////////////////////////////////////////////////////////////////////////

// Proposal number

struct ProposalNumber {
  uint64_t k{0};
  int64_t id{0};
  uint64_t local_time{0};

  static ProposalNumber Zero() {
    return {};
  }

  ProposalNumber operator+(uint64_t value) {
    return {k + value, id, local_time};
  }

  bool operator<(const ProposalNumber& that) const {
    return std::tie(k, id, local_time) <
           std::tie(that.k, that.id, that.local_time);
  }

  MUESLI_SERIALIZABLE(k, id, local_time)
};

inline std::ostream& operator<<(std::ostream& out, const ProposalNumber& n) {
  out << "{" << n.k << ", " << n.id << ", " << n.local_time << "}";
  return out;
}

////////////////////////////////////////////////////////////////////////////////

// Proposal = Proposal number + Value

struct Proposal {
  ProposalNumber n{0};
  Value value{};

  MUESLI_SERIALIZABLE(n, value)
};

inline std::ostream& operator<<(std::ostream& out, const Proposal& proposal) {
  out << "{" << proposal.n << ", " << proposal.value << "}";
  return out;
}

}  // namespace paxos
