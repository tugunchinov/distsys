#include <commute/rpc/call.hpp>

#include <paxos/node/quorum.hpp>
#include <paxos/node/proposer.hpp>

#include <timber/log.hpp>

using namespace whirl;

namespace paxos {

using namespace proto;
using namespace await::futures;
using namespace await::fibers;

ProposerImpl::ProposerImpl(const Value& input)
    : Peer(node::rt::Config()),
      logger_("Paxos.Proposer", node::rt::LoggerBackend()),
      backoff_(GetBackoff()),
      proposal_({ChooseN(), input}),
      input_(input) {
}

Value ProposerImpl::Propose() {
  Prepare();
  if (promised_) {
    Accept();
    if (accepted_) {
      return proposal_.value;
    }
  }
  accepted_ = promised_ = false;
  Wait();
  return Propose();
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
  LOG_INFO("Chose n = {}", proposal_.n);
  proto::Prepare::Request request{proposal_.n};
  auto verdict = CallAcceptor<proto::Prepare>(request);
  if (verdict.HasError() || !verdict->accepted) {
    if (verdict.HasValue()) {
      UpdateN(verdict->advice);
    }
  } else {
    proposal_.value = ChooseValue(verdict->votes);
    promised_ = true;
  }
}

void ProposerImpl::Accept() {
  proto::Accept::Request request{proposal_};
  auto verdict = CallAcceptor<proto::Accept>(request);
  if (verdict.HasError() || !verdict->accepted) {
    if (verdict.HasValue()) {
      UpdateN(verdict->advice);
    }
  } else {
    accepted_ = true;
  }
}

uint64_t ProposerImpl::Majority() const {
  return NodeCount() / 2 + 1;
}

ProposalNumber ProposerImpl::ChooseN() {
  return {0, GetMyID(), GetLocalMonotonicNow()};
}

int64_t ProposerImpl::GetMyID() const {
  return node::rt::Config()->GetInt64("node.id");
}

uint64_t ProposerImpl::GetLocalMonotonicNow() {
  // Doesn't work
  // return node::rt::TimeService()->MonotonicNow().ToJiffies().Count();
  return Await(commute::rpc::Call("Proposer.GetLocalMonotonicNow")
                   .Args()
                   .Via(Channel(node::rt::HostName()))
                   .Start()
                   .As<uint64_t>())
      .ValueOrThrow();
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

  return !latest_vote || latest_vote->value.empty() ? input_
                                                    : latest_vote->value;
}

void ProposerImpl::Wait() {
  Await(node::rt::TimeService()->After(backoff_())).ExpectOk();
}

void ProposerImpl::UpdateN(const ProposalNumber& advice) {
  proposal_.n = advice + 1;
  proposal_.n.id = GetMyID();
  proposal_.n.local_time = GetLocalMonotonicNow();
}

}  // namespace paxos
