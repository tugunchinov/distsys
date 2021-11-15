#include <await/fibers/core/await.hpp>

#include <paxos/node/roles/learner.hpp>

#include <whirl/node/runtime/shortcuts.hpp>

namespace paxos {

Learner::Learner() : chosen_(whirl::node::rt::Database(), "chosen") {
}

void Learner::LearnChosen(Value chosen, size_t idx) {
  m_.Guard();
  chosen_.Put(fmt::to_string(idx), std::move(chosen));
}

void Learner::RegisterMethods() {
  COMMUTE_RPC_REGISTER_METHOD(LearnChosen);
}

}  // namespace paxos
