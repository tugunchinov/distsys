#pragma once

#include <paxos/node/proposal.hpp>
#include <paxos/node/proto.hpp>

#include <commute/rpc/service_base.hpp>

#include <timber/logger.hpp>

namespace paxos {

// Proposer role / RPC service

class Proposer : public commute::rpc::ServiceBase<Proposer> {
 public:
  Proposer();

 protected:
  void RegisterMethods() override {
    COMMUTE_RPC_REGISTER_METHOD(Propose);
  }

  Value Propose(Value input);

 private:
  timber::Logger logger_;
};

}  // namespace paxos
