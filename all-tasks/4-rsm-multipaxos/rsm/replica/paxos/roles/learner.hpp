#pragma once

#include <await/fibers/sync/channel.hpp>

#include <commute/rpc/service_base.hpp>

#include <rsm/replica/paxos/proposal.hpp>

#include <timber/logger.hpp>

#include <whirl/node/store/kv.hpp>

namespace paxos {

struct Commit {
  size_t idx;
  rsm::Command command;
};

class Learner : public commute::rpc::ServiceBase<Learner> {
 public:
  explicit Learner();

 protected:
  void LearnChosen(Value chosen, size_t idx);
  std::optional<Value> TryGetChosen(size_t idx);

  void RegisterMethods() override;

 private:
  timber::Logger logger_;
  whirl::node::store::KVStore<Value> chosen_store_;
};

}  // namespace paxos