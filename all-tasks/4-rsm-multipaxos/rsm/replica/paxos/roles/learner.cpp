#include <rsm/replica/paxos/roles/learner.hpp>

#include <timber/log.hpp>

#include <whirl/node/runtime/shortcuts.hpp>

using namespace whirl;

namespace paxos {

Learner::Learner(await::fibers::Channel<Commit>& commits)
    : logger_("Paxos.Learner", node::rt::LoggerBackend()),
      chosen_store_(node::rt::Database(), "chosen"),
      commits_(commits) {
}

void Learner::ApproveCommit(Value chosen, size_t idx) {
  commits_.TrySend({idx, chosen});
}

void Learner::LearnChosen(Value chosen, size_t idx) {
  if (!chosen_store_.Has(fmt::to_string(idx))) {
    chosen_store_.Put(fmt::to_string(idx), chosen);
    LOG_INFO("Learnt {} at index {}", chosen, idx);
  }
}

std::optional<Value> Learner::TryGetChosen(size_t idx) {
  return chosen_store_.TryGet(fmt::to_string(idx));
}

void Learner::RegisterMethods() {
  COMMUTE_RPC_REGISTER_METHOD(ApproveCommit);
  COMMUTE_RPC_REGISTER_METHOD(LearnChosen);
  COMMUTE_RPC_REGISTER_METHOD(TryGetChosen);
}

}  // namespace paxos