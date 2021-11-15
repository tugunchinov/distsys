#include <paxos/node/acceptor.hpp>

#include <whirl/node/runtime/shortcuts.hpp>

#include <timber/log.hpp>

using namespace whirl;

namespace paxos {

Acceptor::Acceptor() : logger_("Paxos.Acceptor", node::rt::LoggerBackend()) {
}

void Acceptor::Prepare(const proto::Prepare::Request& /*request*/,
                       proto::Prepare::Response* /*response*/) {
  // Your code goes here
}

void Acceptor::Accept(const proto::Accept::Request& /*request*/,
                      proto::Accept::Response* /*response*/) {
  // Your code goes here
}

}  // namespace paxos
