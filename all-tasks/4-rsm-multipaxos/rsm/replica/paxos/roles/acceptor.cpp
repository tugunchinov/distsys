#include <rsm/replica/paxos/roles/acceptor.hpp>

#include <timber/log.hpp>

#include <whirl/node/runtime/shortcuts.hpp>

using namespace whirl;

namespace paxos {

using namespace proto;
using namespace await::fibers;

Acceptor::Acceptor(rsm::Log& log, Mutex& log_lock)
    : logger_("Paxos.Acceptor", node::rt::LoggerBackend()),
      log_(log),
      log_lock_(log_lock) {
}

void Acceptor::Prepare(const proto::Prepare::Request& request,
                       proto::Prepare::Response* response) {
  auto guard = log_lock_.Guard();
  if (!states_.contains(request.idx)) {
    states_[request.idx] =
        log_.Read(request.idx).value_or(AcceptorState::Empty());
  }
  if (request.n < states_[request.idx].np) {
    LOG_INFO("nack P{}", request.n);

    *response = Reject<paxos::Prepare>(states_[request.idx].np);
  } else {
    LOG_INFO("ack P{}", request.n);

    states_[request.idx].np = request.n;
    log_.Update(request.idx, states_[request.idx]);

    *response = Promise(states_[request.idx].vote);
  }
}

void Acceptor::Accept(const proto::Accept::Request& request,
                      proto::Accept::Response* response) {
  auto guard = log_lock_.Guard();
  if (!states_.contains(request.idx)) {
    states_[request.idx] =
        log_.Read(request.idx).value_or(AcceptorState::Empty());
  }
  auto state = states_[request.idx];
  if (request.proposal.n < state.np) {
    LOG_INFO("nack A{}", request.proposal);

    *response = Reject<paxos::Accept>(state.np);
  } else {
    LOG_INFO("ack A{}", request.proposal);

    state.np = request.proposal.n;
    state.vote = request.proposal;
    states_[request.idx] = state;
    log_.Update(request.idx, state);

    *response = Vote();
  }
}

template <typename Phase>
typename Phase::Response Acceptor::Reject(const ProposalNumber& np) const {
  return {.ack = false, .advice = np};
}

proto::Prepare::Response Acceptor::Promise(
    const std::optional<Proposal>& vote) const {
  return {.ack = true, .vote = vote};
}

proto::Accept::Response Acceptor::Vote() const {
  return {.ack = true};
}

void Acceptor::RegisterMethods() {
  COMMUTE_RPC_REGISTER_HANDLER(Prepare);
  COMMUTE_RPC_REGISTER_HANDLER(Accept);
}

}  // namespace paxos