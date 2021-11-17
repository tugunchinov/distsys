#include <paxos/node/roles/acceptor.hpp>

#include <timber/log.hpp>

#include <whirl/node/runtime/shortcuts.hpp>

using namespace whirl;

namespace paxos {

using namespace proto;
using namespace await::fibers;

AcceptorImpl::AcceptorImpl(node::store::KVStore<AcceptorState>& state_store,
                           size_t idx)
    : logger_("Paxos.Acceptor", node::rt::LoggerBackend()),
      state_store_(state_store),
      idx_(fmt::to_string(idx)),
      state_(GetState()) {
}

Prepare::Response AcceptorImpl::Prepare(const Prepare::Request& request) {
  auto guard = m_.Guard();
  if (request.n < state_.np) {
    LOG_INFO("nack P{}", request.n);

    return Reject<paxos::Prepare>();
  } else {
    LOG_INFO("ack P{}", request.n);

    state_.np = request.n;
    UpdateState();

    return Promise();
  }
}

Accept::Response AcceptorImpl::Accept(const Accept::Request& request) {
  auto guard = m_.Guard();
  if (request.proposal.n < state_.np) {
    LOG_INFO("nack A{}", request.proposal);

    return Reject<paxos::Accept>();
  } else {
    LOG_INFO("ack A{}", request.proposal);

    state_.np = request.proposal.n;
    state_.vote = request.proposal;
    UpdateState();

    return Vote();
  }
}

AcceptorState AcceptorImpl::GetState() const {
  return state_store_.GetOr(idx_, {});
}

void AcceptorImpl::UpdateState() {
  state_store_.Put(idx_, state_);
}

template <typename Phase>
typename Phase::Response AcceptorImpl::Reject() const {
  return {.ack = false, .advice = state_.np};
}

proto::Prepare::Response AcceptorImpl::Promise() const {
  return {.ack = true, .vote = state_.vote};
}

proto::Accept::Response AcceptorImpl::Vote() const {
  return {.ack = true};
}

}  // namespace paxos
