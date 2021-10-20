#include <paxos/node/acceptor.hpp>

#include <whirl/node/runtime/shortcuts.hpp>

#include <timber/log.hpp>

using namespace whirl;

namespace paxos {

Acceptor::Acceptor()
    : logger_("Paxos.Acceptor", node::rt::LoggerBackend()),
      kv_store_(node::rt::Database(), "data") {
}

void Acceptor::Prepare(const proto::Prepare::Request& request,
                       proto::Prepare::Response* response) {
  std::lock_guard lock(m_);
  if (request.n < kv_store_.GetOr("np", {}).n) {
    Reject<proto::Prepare>(response);
    LOG_INFO("nP{}", request.n);
  } else {
    LOG_INFO("P{}", request.n);
    kv_store_.Put("np", {request.n});
    np_ = request.n;
    Promise(response);
  }
}

void Acceptor::Accept(const proto::Accept::Request& request,
                      proto::Accept::Response* response) {
  std::lock_guard lock(m_);
  if (request.proposal.n < kv_store_.GetOr("np", {}).n) {
    Reject<proto::Accept>(response);
    LOG_INFO("nA{}", request.proposal);
  } else {
    LOG_INFO("A{}", request.proposal);
    kv_store_.Put("np", {request.proposal.n});
    np_ = request.proposal.n;
    kv_store_.Put("vote", request.proposal);
    vote_ = request.proposal;
    Vote(response);
  }
}

template <typename Phase>
void Acceptor::Reject(typename Phase::Response* response) {
  response->ack = false;
  response->advice = kv_store_.GetOr("np", {}).n;
}

void Acceptor::Promise(proto::Prepare::Response* response) {
  response->ack = true;
  response->vote = kv_store_.GetOr("vote", {});
}

void Acceptor::Vote(proto::Accept::Response* response) {
  response->ack = true;
}

}  // namespace paxos
