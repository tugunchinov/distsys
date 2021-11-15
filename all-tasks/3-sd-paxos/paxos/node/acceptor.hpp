#pragma once

#include <paxos/node/proposal.hpp>
#include <paxos/node/proto.hpp>

#include <commute/rpc/service_base.hpp>

#include <timber/logger.hpp>

namespace paxos {

// Acceptor role / RPC service

class Acceptor : public commute::rpc::ServiceBase<Acceptor> {
 public:
  Acceptor();

 protected:
  void RegisterMethods() override {
    COMMUTE_RPC_REGISTER_HANDLER(Prepare);
    COMMUTE_RPC_REGISTER_HANDLER(Accept);
  }

  // Phase 1 (Prepare / Promise)

  void Prepare(const proto::Prepare::Request& request,
               proto::Prepare::Response* response);

  // Phase 2 (Accept / Accepted)

  void Accept(const proto::Accept::Request& request,
              proto::Accept::Response* response);

 private:
  timber::Logger logger_;
};

}  // namespace paxos
