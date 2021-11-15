#pragma once

#include <await/fibers/sync/mutex.hpp>

#include <commute/rpc/service_base.hpp>

#include <rsm/replica/paxos/proposal.hpp>
#include <rsm/replica/paxos/proto.hpp>
#include <rsm/replica/paxos/quorum.hpp>
#include <rsm/replica/store/log.hpp>

namespace paxos {

class Learner : public commute::rpc::ServiceBase<Learner> {
 public:
  Learner(rsm::Log& log);

 protected:
  void LearnChosen(Value chosen, size_t idx);

  void RegisterMethods() override;

 private:
  rsm::Log& log_;
  await::fibers::Mutex m_;
};

}  // namespace paxos
