#pragma once

#include <rsm/replica/paxos/proposal.hpp>

namespace paxos {

struct AcceptorState {
  ProposalNumber np{ProposalNumber::Zero()};
  std::optional<Proposal> vote{std::nullopt};

  MUESLI_SERIALIZABLE(np, vote)
};

}  // namespace paxos