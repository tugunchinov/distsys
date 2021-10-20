#pragma once

#include <commute/rpc/service_base.hpp>

#include <paxos/node/backoff.hpp>
#include <paxos/node/proposal.hpp>
#include <paxos/node/proto.hpp>

#include <timber/logger.hpp>

// TODO:

#include <whirl/node/cluster/peer.hpp>

#include <whirl/node/runtime/shortcuts.hpp>

#include <whirl/node/store/kv.hpp>

#include <await/fibers/sync/mutex.hpp>

using namespace whirl;

namespace paxos {

// Proposer role / RPC service

class ProposerImpl : public node::cluster::Peer {
 public:
  explicit ProposerImpl(const Value& input);

 public:
  Value Propose();

 private:
  int64_t GetMyID() const;
  uint64_t GetLocalMonotonicNow();
  ProposalNumber ChooseN();

  Backoff GetBackoff() const;

  void Prepare();
  void Accept();

  void Wait();

  void UpdateN(const ProposalNumber& advice);

  template <typename Phase>
  auto CallAcceptor(const typename Phase::Request& request);

  Value ChooseValue(
      const std::vector<proto::Prepare::Response>& responses) const;

  uint64_t Majority() const;

 private:
  mutable timber::Logger logger_;

  Backoff backoff_;

  Proposal proposal_;

  const Value& input_;

  bool promised_{false};
  bool accepted_{false};
};

class Proposer : public commute::rpc::ServiceBase<Proposer> {
 public:
  Proposer() : local_monotonic_(whirl::node::rt::Database(), "n") {
  }

 protected:
  Value Propose(const Value& input) {
    return ProposerImpl(input).Propose();
  }

  uint64_t GetLocalMonotonicNow() {
    std::lock_guard lock(m_);
    uint64_t latest = local_monotonic_.GetOr("n", 0);
    local_monotonic_.Put("n", latest + 1);
    return latest;
  }

  void RegisterMethods() override {
    COMMUTE_RPC_REGISTER_METHOD(Propose);
    COMMUTE_RPC_REGISTER_METHOD(GetLocalMonotonicNow);
  }

 private:
  whirl::node::store::KVStore<uint64_t> local_monotonic_;
  await::fibers::Mutex m_;
};

}  // namespace paxos
