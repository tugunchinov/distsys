#pragma once

#include <paxos/node/proposal.hpp>

#include <muesli/serializable.hpp>

#include <cereal/types/optional.hpp>

#include <optional>

namespace paxos {

namespace proto {

////////////////////////////////////////////////////////////////////////////////

// Phase I

struct Prepare {
  // Prepare
  struct Request {
    ProposalNumber n;
    size_t idx{0};

    MUESLI_SERIALIZABLE(n, idx)
  };

  // Promise
  struct Response {
    bool ack;
    ProposalNumber advice;
    std::optional<Proposal> vote;

    MUESLI_SERIALIZABLE(ack, advice, vote)
  };
};

////////////////////////////////////////////////////////////////////////////////

// Phase II

struct Accept {
  // Accept
  struct Request {
    Proposal proposal;
    size_t idx{0};

    MUESLI_SERIALIZABLE(proposal, idx)
  };

  // Accepted
  struct Response {
    bool ack = false;
    ProposalNumber advice;

    MUESLI_SERIALIZABLE(ack, advice)
  };
};

}  // namespace proto

}  // namespace paxos
