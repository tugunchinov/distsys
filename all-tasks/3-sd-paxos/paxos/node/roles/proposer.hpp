#pragma once

#include <commute/rpc/service_base.hpp>

#include <paxos/node/backoff.hpp>
#include <paxos/node/proposal.hpp>
#include <paxos/node/proto.hpp>

#include <timber/logger.hpp>

#include <whirl/node/cluster/peer.hpp>

using namespace whirl;

namespace paxos {

// Proposer role / RPC service

class ProposerImpl {
 public:
  explicit ProposerImpl(const Value& input, size_t idx);

 public:
  Value Propose();

 private:
  void UpdateN(const ProposalNumber& advice);

  Backoff GetBackoff() const;

  void Prepare();
  void Accept();
  void Wait();
  void Decide();

  template <typename Phase>
  auto CallAcceptor(const typename Phase::Request& request);

  std::optional<Proposal> GetLatest(
      const std::vector<proto::Prepare::Response>& responses) const;
  Value ChooseValue(
      const std::vector<proto::Prepare::Response>& responses) const;

  uint64_t Majority() const;

 private:
  mutable timber::Logger logger_;

  node::cluster::Peer peer_;

  size_t idx_;
  Backoff backoff_;
  Proposal proposal_;
  const Value& input_;

  bool promised_{false};
  bool accepted_{false};
};

class Proposer : public commute::rpc::ServiceBase<Proposer> {
 protected:
  Value Propose(const Value& input) {
    return ProposerImpl(input, 0).Propose();
  }

  void RegisterMethods() override {
    COMMUTE_RPC_REGISTER_METHOD(Propose);
  }
};

}  // namespace paxos