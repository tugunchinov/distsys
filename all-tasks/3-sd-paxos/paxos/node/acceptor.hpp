#pragma once

#include <await/fibers/sync/mutex.hpp>

#include <commute/rpc/service_base.hpp>

#include <paxos/node/proposal.hpp>
#include <paxos/node/proto.hpp>

#include <timber/logger.hpp>

#include <whirl/node/store/kv.hpp>

namespace paxos {

class Acceptor : public commute::rpc::ServiceBase<Acceptor> {
 public:
  Acceptor();

 protected:
  void RegisterMethods() override {
    COMMUTE_RPC_REGISTER_HANDLER(Prepare);
    COMMUTE_RPC_REGISTER_HANDLER(Accept);
  }

  void Prepare(const proto::Prepare::Request& request,
               proto::Prepare::Response* response);

  void Accept(const proto::Accept::Request& request,
              proto::Accept::Response* response);

 private:
  ProposalNumber GetNp() const;
  void UpdateNp(const ProposalNumber& n);

  Proposal GetVote() const;
  void UpdateVote(const Proposal& vote);

  template <typename Phase>
  void Reject(typename Phase::Response* response);

  void Promise(proto::Prepare::Response* response);

  void Vote(proto::Accept::Response* response);

 private:
  timber::Logger logger_;
  whirl::node::store::KVStore<ProposalNumber> np_;
  whirl::node::store::KVStore<Proposal> vote_;
  await::fibers::Mutex m_;
};

}  // namespace paxos
