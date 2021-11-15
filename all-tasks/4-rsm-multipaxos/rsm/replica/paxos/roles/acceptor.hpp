#pragma once

#include <commute/rpc/service_base.hpp>

#include <rsm/replica/paxos/roles/learner.hpp>
#include <rsm/replica/paxos/proto.hpp>

#include <timber/logger.hpp>

#include <whirl/node/runtime/shortcuts.hpp>
#include <whirl/node/store/kv.hpp>

namespace paxos {

struct AcceptorState {
  ProposalNumber np{ProposalNumber::Zero()};
  std::optional<Proposal> vote{std::nullopt};

  MUESLI_SERIALIZABLE(np, vote)
};

class AcceptorImpl {
 public:
  explicit AcceptorImpl(whirl::node::store::KVStore<AcceptorState>& state_store,
                        size_t idx);

  proto::Prepare::Response Prepare(const proto::Prepare::Request& request);
  proto::Accept::Response Accept(const proto::Accept::Request& request);

 private:
  AcceptorState GetState() const;
  void UpdateState();

  template <typename Phase>
  typename Phase::Response Reject() const;
  proto::Prepare::Response Promise() const;
  proto::Accept::Response Vote() const;

 private:
  timber::Logger logger_;
  whirl::node::store::KVStore<AcceptorState>& state_store_;
  std::string idx_;
  AcceptorState state_;
  mutable await::fibers::Mutex m_;
};

class Acceptor : public commute::rpc::ServiceBase<Acceptor> {
 public:
  explicit Acceptor() : state_store_(whirl::node::rt::Database(), "state") {
  }

 protected:
  void Prepare(const proto::Prepare::Request& request,
               proto::Prepare::Response* response) {
    {
      m_.Guard();
      if (!indexed_acceptors_.contains(request.idx)) {
        indexed_acceptors_.try_emplace(request.idx, state_store_, request.idx);
      }
    }

    *response = indexed_acceptors_.at(request.idx).Prepare(request);
  }

  void Accept(const proto::Accept::Request& request,
              proto::Accept::Response* response) {
    *response = indexed_acceptors_.at(request.idx).Accept(request);
  }

  void RegisterMethods() override {
    COMMUTE_RPC_REGISTER_HANDLER(Prepare);
    COMMUTE_RPC_REGISTER_HANDLER(Accept);
  }

 private:
  whirl::node::store::KVStore<AcceptorState> state_store_;
  std::map<size_t, AcceptorImpl> indexed_acceptors_;
  await::fibers::Mutex m_;
};

}  // namespace paxos
