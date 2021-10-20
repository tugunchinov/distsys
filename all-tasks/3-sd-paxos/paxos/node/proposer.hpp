#pragma once

#include <paxos/node/proposal.hpp>
#include <paxos/node/proto.hpp>
#include <whirl/node/runtime/shortcuts.hpp>

#include <commute/rpc/service_base.hpp>

#include <timber/logger.hpp>

#include <whirl/node/cluster/peer.hpp>

#include <await/fibers/sync/mutex.hpp>

#include <paxos/node/backoff.hpp>

#include <whirl/node/store/kv.hpp>

using namespace whirl;

namespace paxos {

// Proposer role / RPC service

class ProposerImpl : public node::cluster::Peer {
 public:
  ProposerImpl();

 public:
  Value Propose(const Value& input);

 private:
  int64_t GetMyID() const;
  uint64_t GetLocalMonotonicNow() const;
  ProposalNumber ChooseN() const;

  Backoff GetBackoff() const;

  void Start();
  void Retry();

  void Prepare();
  void Accept();

  template <typename Phase>
  auto CallAcceptor(const typename Phase::Request& request);

  Value ChooseValue(
      const std::vector<proto::Prepare::Response>& responses) const;

  uint64_t Majority() const;

 private:
  timber::Logger logger_;

  Backoff backoff_;
  Proposal proposal_;

  Value input_;

  bool chosen_ = false;

  mutable std::atomic<uint64_t> now_;
};

class Proposer : public commute::rpc::ServiceBase<Proposer> {
 protected:
  Value Propose(const Value& input) {
    return ProposerImpl().Propose(input);
  }

  void RegisterMethods() override {
    COMMUTE_RPC_REGISTER_METHOD(Propose);
  }
};

}  // namespace paxos
