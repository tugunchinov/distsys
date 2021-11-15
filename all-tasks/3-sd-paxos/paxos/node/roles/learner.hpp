#pragma once

#include <await/fibers/sync/mutex.hpp>

#include <commute/rpc/service_base.hpp>

#include <paxos/node/proposal.hpp>
#include <paxos/node/proto.hpp>
#include <paxos/node/quorum.hpp>

#include <whirl/node/store/kv.hpp>
#include <whirl/node/cluster/peer.hpp>

namespace paxos {

class Learner : public commute::rpc::ServiceBase<Learner> {
 public:
  Learner();

 protected:
  void LearnChosen(Value chosen, size_t idx);

  void RegisterMethods() override;

 private:
  whirl::node::store::KVStore<Value> chosen_;
  await::fibers::Mutex m_;
};

}  // namespace paxos
