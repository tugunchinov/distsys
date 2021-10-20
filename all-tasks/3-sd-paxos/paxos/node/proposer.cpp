#include <whirl/node/runtime/shortcuts.hpp>
#include <whirl/node/cluster/peer.hpp>

#include <paxos/node/quorum.hpp>
#include <paxos/node/backoff.hpp>

#include <commute/rpc/call.hpp>

#include <timber/log.hpp>
#include "proposer.hpp"

using namespace whirl;

namespace paxos {

using namespace proto;
using namespace await::futures;
using namespace await::fibers;

ProposerImpl::ProposerImpl()
    : Peer(node::rt::Config()),
      logger_("Paxos.Proposer", node::rt::LoggerBackend()),
      backoff_(GetBackoff()) {
  LOG_INFO("Born");
  now_ = commute::rpc::GenerateRequestId();
}

Value ProposerImpl::Propose(const Value& input) {
  proposal_ = {ChooseN(), input};
  input_ = input;
  Start();
  chosen_ = false;
  return proposal_.value;
}

void ProposerImpl::Start() {
  if (chosen_) {
    return;
  }
  Prepare();
  if (chosen_) {
    return;
  }
  Accept();
}

void ProposerImpl::Retry() {
  Await(node::rt::TimeService()->After(backoff_())).ExpectOk();
  proposal_.value = input_;
  Start();
}

template <typename Phase>
const char* k_phase_call{nullptr};
template <>
const char* k_phase_call<proto::Prepare>{"Acceptor.Prepare"};
template <>
const char* k_phase_call<proto::Accept>{"Acceptor.Accept"};

template <typename Phase>
auto ProposerImpl::CallAcceptor(const typename Phase::Request& request) {
  std::vector<Future<typename Phase::Response>> requests;
  for (const auto& peer : ListPeers().WithMe()) {
    requests.push_back(commute::rpc::Call(k_phase_call<Phase>)
                           .Args(request)
                           .Via(Channel(peer))
                           .Context(await::context::ThisFiber())
                           .AtMostOnce()
                           .Start()
                           .template As<typename Phase::Response>());
  }
  return Await(PaxosQuorum<Phase>(std::move(requests), Majority()));
}

void ProposerImpl::Prepare() {
  LOG_INFO("Choose n = {}", proposal_.n);
  proto::Prepare::Request request{proposal_.n};
  auto verdict = CallAcceptor<proto::Prepare>(request);
  if (verdict.HasError() || !verdict->accepted) {
    if (verdict.HasValue()) {
      proposal_.n = verdict->advice + 1;
      proposal_.n.id = GetMyID();
      proposal_.n.local_time = GetLocalMonotonicNow();
    }
    Retry();
  } else {
    proposal_.value = ChooseValue(verdict->votes);
  }
}

void ProposerImpl::Accept() {
  proto::Accept::Request request{proposal_};
  auto verdict = CallAcceptor<proto::Accept>(request);
  if (verdict.HasError() || !verdict->accepted) {
    if (verdict.HasValue()) {
      proposal_.n = verdict->advice + 1;
      proposal_.n.id = GetMyID();
      proposal_.n.local_time = GetLocalMonotonicNow();
    }
    Retry();
  } else {
    chosen_ = true;
  }
}

uint64_t ProposerImpl::Majority() const {
  return NodeCount() / 2 + 1;
}

ProposalNumber ProposerImpl::ChooseN() const {
  return {0, GetMyID(), GetLocalMonotonicNow()};
}

int64_t ProposerImpl::GetMyID() const {
  return node::rt::Config()->GetInt64("node.id");
}

uint64_t ProposerImpl::GetLocalMonotonicNow() const {
  // return node::rt::TimeService()->MonotonicNow().ToJiffies().Count();
  return now_.fetch_add(1);
}
Backoff ProposerImpl::GetBackoff() const {
  uint64_t init = node::rt::Config()->GetInt<uint64_t>("paxos.backoff.init");
  uint64_t max = node::rt::Config()->GetInt<uint64_t>("paxos.backoff.max");
  uint64_t factor =
      node::rt::Config()->GetInt<uint64_t>("paxos.backoff.factor");
  return Backoff(Backoff::Params{init, max, factor});
}

Value ProposerImpl::ChooseValue(
    const std::vector<proto::Prepare::Response>& responses) const {
  auto latest_vote = std::max_element(responses.begin(), responses.end(),
                                      [](const proto::Prepare::Response& lhs,
                                         const proto::Prepare::Response& rhs) {
                                        return lhs.vote->n < rhs.vote->n;
                                      })
                         ->vote;

  if (!latest_vote) {
    return input_;
  }

  return latest_vote->value.empty() ? input_ : latest_vote->value;
}

}  // namespace paxos
