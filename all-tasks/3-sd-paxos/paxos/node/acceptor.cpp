#include <paxos/node/acceptor.hpp>

#include <whirl/node/runtime/shortcuts.hpp>

#include <timber/log.hpp>

// TODO: extract learner

using namespace whirl;

namespace paxos {

using namespace proto;

Acceptor::Acceptor()
    : logger_("Paxos.Acceptor", node::rt::LoggerBackend()),
      np_(node::rt::Database(), "vote"),
      vote_(node::rt::Database(), "np") {
}

void Acceptor::Prepare(const Prepare::Request& request,
                       Prepare::Response* response) {
  std::lock_guard lock(m_);
  if (request.n < GetNp()) {
    Reject<proto::Prepare>(response);
    LOG_INFO("nack P{}", request.n);
  } else {
    LOG_INFO("ack P{}", request.n);
    UpdateNp(request.n);
    Promise(response);
  }
}

void Acceptor::Accept(const Accept::Request& request,
                      Accept::Response* response) {
  std::lock_guard lock(m_);
  if (request.proposal.n < GetNp()) {
    Reject<proto::Accept>(response);
    LOG_INFO("nack A{}", request.proposal);
  } else {
    LOG_INFO("ack A{}", request.proposal);
    UpdateNp(request.proposal.n);
    UpdateVote(request.proposal);
    Vote(response);
  }
}

ProposalNumber Acceptor::GetNp() const {
  return np_.GetOr("np", {});
}

void Acceptor::UpdateNp(const ProposalNumber& n) {
  np_.Put("np", n);
}

Proposal Acceptor::GetVote() const {
  return vote_.GetOr("vote", {});
}

void Acceptor::UpdateVote(const Proposal& vote) {
  vote_.Put("vote", vote);
}

template <typename Phase>
void Acceptor::Reject(typename Phase::Response* response) {
  response->ack = false;
  response->advice = GetNp();
}

void Acceptor::Promise(Prepare::Response* response) {
  response->ack = true;
  response->vote = GetVote();
}

void Acceptor::Vote(Accept::Response* response) {
  response->ack = true;
}

}  // namespace paxos
