#pragma once

#include <paxos/node/proposal.hpp>
#include <paxos/node/proto.hpp>

#include <commute/rpc/service_base.hpp>

#include <timber/logger.hpp>

#include <await/fibers/sync/mutex.hpp>

#include <whirl/node/store/kv.hpp>

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
  template <typename Phase>
  void Reject(typename Phase::Response* response);

  void Promise(proto::Prepare::Response* response);

  void Vote(proto::Accept::Response* response);

 private:
  timber::Logger logger_;

  whirl::node::store::KVStore<Proposal> kv_store_;

  paxos::ProposalNumber np_{};
  paxos::Proposal vote_{};

  await::fibers::Mutex m_;
};

}  // namespace paxos
