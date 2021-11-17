#pragma once

#include <await/fibers/sync/mutex.hpp>

#include <commute/rpc/service_base.hpp>

#include <paxos/node/proposal.hpp>

#include <timber/logger.hpp>

#include <whirl/node/store/kv.hpp>

namespace paxos {

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
