#include <await/fibers/core/await.hpp>

#include <rsm/replica/paxos/roles/learner.hpp>

#include <whirl/node/runtime/shortcuts.hpp>

using namespace whirl;

namespace paxos {

Learner::Learner() : log_(node::rt::Config()->GetString("rsm.store.dir")) {
  log_.Open();
}

void Learner::LearnChosen(Value chosen, size_t idx) {
  m_.Guard();
  log_.Update(idx, {std::move(chosen), true, false});
}

void Learner::RegisterMethods() {
  COMMUTE_RPC_REGISTER_METHOD(LearnChosen);
}

}  // namespace paxos
