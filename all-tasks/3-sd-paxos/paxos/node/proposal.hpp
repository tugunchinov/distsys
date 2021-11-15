#pragma once

// Enable string serialization
#include <cereal/types/string.hpp>

#include <muesli/serializable.hpp>

#include <whirl/node/runtime/shortcuts.hpp>

#include <string>
#include <ostream>

namespace paxos {

////////////////////////////////////////////////////////////////////////////////

using Value = std::string;

////////////////////////////////////////////////////////////////////////////////

// Proposal number

struct ProposalNumber {
  uint64_t k{0};
  std::string guid{whirl::node::rt::GenerateGuid()};

  static ProposalNumber Zero() {
    return {};
  }

  ProposalNumber operator+(uint64_t value) const {
    return {k + value, whirl::node::rt::GenerateGuid()};
  }

  bool operator<(const ProposalNumber& that) const {
    return std::tie(k, guid) < std::tie(that.k, that.guid);
  }

  MUESLI_SERIALIZABLE(k, guid)
};

inline std::ostream& operator<<(std::ostream& out, const ProposalNumber& n) {
  out << "{" << n.k << ", " << n.guid << "}";
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