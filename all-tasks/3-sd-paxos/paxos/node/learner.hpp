#pragma once

#include <commute/rpc/service_base.hpp>
#include <commute/rpc/call.hpp>

namespace paxos {

class Learner : public commute::rpc::ServiceBase<Learner> {
 public:
  Learner() {
  }

 protected:
  void RegisterMethods() override {
    // COMMUTE_RPC_REGISTER_METHOD(Propose);
  }

 private:
};

}  // namespace paxos
