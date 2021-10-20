#pragma once

#include <commute/rpc/service_base.hpp>
#include <whirl/node/store/kv.hpp>

namespace paxos {

class Learner : public commute::rpc::ServiceBase<Learner> {
 public:
  Learner() {
  }

 protected:
  void RegisterMethods() override {
  }

 private:
  // whirl::node::store::KVStore<Proposal> ;
};

}  // namespace paxos
