#pragma once

#include <await/fibers/sync/mutex.hpp>

#include <commute/rpc/service_base.hpp>

#include <rsm/replica/paxos/roles/acceptor_state.hpp>
#include <rsm/replica/paxos/roles/learner.hpp>
#include <rsm/replica/paxos/proto.hpp>
#include <rsm/replica/store/log.hpp>

#include <timber/logger.hpp>

#include <whirl/node/runtime/shortcuts.hpp>
#include <whirl/node/store/kv.hpp>

namespace paxos {

class Acceptor : public commute::rpc::ServiceBase<Acceptor> {
 public:
  explicit Acceptor(rsm::Log& log, await::fibers::Mutex& log_lock);

 protected:
  void Prepare(const proto::Prepare::Request& request,
               proto::Prepare::Response* response);
  void Accept(const proto::Accept::Request& request,
              proto::Accept::Response* response);

  void RegisterMethods() override;

 private:
  template <typename Phase>
  typename Phase::Response Reject(const ProposalNumber& np) const;
  proto::Prepare::Response Promise(const std::optional<Proposal>& vote) const;
  proto::Accept::Response Vote() const;

 private:
  mutable timber::Logger logger_;
  rsm::Log& log_;
  await::fibers::Mutex& log_lock_;
};

}  // namespace paxos