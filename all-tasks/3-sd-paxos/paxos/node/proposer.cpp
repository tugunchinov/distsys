#include <paxos/node/proposer.hpp>

#include <whirl/node/runtime/shortcuts.hpp>

#include <timber/log.hpp>

using namespace whirl;

namespace paxos {

Proposer::Proposer() : logger_("Paxos.Proposer", node::rt::LoggerBackend()) {
}

Value Proposer::Propose(Value input) {
  return input;  // Violates agreement
  // return 0;  // Violates validity
}

}  // namespace paxos
