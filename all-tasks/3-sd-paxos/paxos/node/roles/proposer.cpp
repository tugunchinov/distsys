#include <await/futures/combine/quorum.hpp>

#include <commute/rpc/call.hpp>

#include <paxos/node/quorum.hpp>
#include <paxos/node/roles/proposer.hpp>

#include <timber/log.hpp>

using namespace whirl;

namespace paxos {

using namespace proto;
using namespace await::futures;
using namespace await::fibers;

ProposerImpl::ProposerImpl(const Value& input, size_t idx)
    : logger_("Paxos.Proposer", node::rt::LoggerBackend()),
      peer_(node::rt::Config()),
      idx_(idx),
      backoff_(GetBackoff()),
      proposal_({ProposalNumber::Zero(), input}),
      input_(input) {
}

Value ProposerImpl::Propose() {
  while (true) {
    if (auto maybe_chosen = CheckMaybeChosen()) {
      return *maybe_chosen;
    }

    Prepare();
    if (promised_) {
      Accept();
      if (accepted_) {
        Decide();  // Learn?
        return proposal_.value;
      }
    }
    accepted_ = promised_ = false;
    Wait();
  }
}

template <typename Phase>
const char* k_phase_method{nullptr};
template <>
const char* k_phase_method<proto::Prepare>{"Acceptor.Prepare"};
template <>
const char* k_phase_method<proto::Accept>{"Acceptor.Accept"};

template <typename Phase>
auto ProposerImpl::CallAcceptor(const typename Phase::Request& request) {
  std::vector<Future<typename Phase::Response>> requests;
  for (const auto& peer : peer_.ListPeers().WithMe()) {
    requests.push_back(commute::rpc::Call(k_phase_method<Phase>)
                           .Args(request)
                           .Via(peer_.Channel(peer))
                           .Context(await::context::ThisFiber())
                           .AtMostOnce()
                           .Start()
                           .template As<typename Phase::Response>());
  }
  return Await(PaxosQuorum<Phase>(std::move(requests), Majority()));
}

std::optional<Value> ProposerImpl::CheckMaybeChosen() {
  return Await(commute::rpc::Call("Learner.TryGetChosen")
                   .Args(idx_)
                   .Via(peer_.LoopBack())
                   .Start()
                   .As<std::optional<Value>>())
      .ValueOrThrow();
}

void ProposerImpl::Prepare() {
  LOG_INFO("Chose n = {}", proposal_.n);
  proto::Prepare::Request request{proposal_.n, idx_};
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
  proto::Accept::Request request{proposal_, idx_};
  auto verdict = CallAcceptor<proto::Accept>(request);
  if (verdict.HasError() || !verdict->accepted) {
    if (verdict.HasValue()) {
      UpdateN(verdict->advice);
    }
  } else {
    accepted_ = true;
  }
}

void ProposerImpl::Decide() {
  std::vector<Future<void>> calls;
  for (const auto& peer : peer_.ListPeers().WithMe()) {
    calls.push_back(commute::rpc::Call("Learner.LearnChosen")
                        .Args(proposal_.value, idx_)
                        .Via(peer_.Channel(peer))
                        .AtLeastOnce());
  }
  Await(Quorum(std::move(calls), Majority())).ThrowIfError();
}

uint64_t ProposerImpl::Majority() const {
  return peer_.NodeCount() / 2 + 1;
}

Backoff ProposerImpl::GetBackoff() const {
  uint64_t init = node::rt::Config()->GetInt<uint64_t>("paxos.backoff.init");
  uint64_t max = node::rt::Config()->GetInt<uint64_t>("paxos.backoff.max");
  uint64_t factor =
      node::rt::Config()->GetInt<uint64_t>("paxos.backoff.factor");
  return Backoff(Backoff::Params{init, max, factor});
}

std::optional<Proposal> ProposerImpl::GetLatest(
    const std::vector<proto::Prepare::Response>& responses) const {
  return std::max_element(responses.begin(), responses.end(),
                          [](const proto::Prepare::Response& lhs,
                             const proto::Prepare::Response& rhs) {
                            return !lhs.vote || lhs.vote->n < rhs.vote->n;
                          })
      ->vote;
}

Value ProposerImpl::ChooseValue(
    const std::vector<proto::Prepare::Response>& responses) const {
  auto latest_vote = GetLatest(responses);
  return !latest_vote ? input_ : latest_vote->value;
}

void ProposerImpl::Wait() {
  Await(node::rt::TimeService()->After(backoff_())).ExpectOk();
}

void ProposerImpl::UpdateN(const ProposalNumber& advice) {
  proposal_.n = advice + 1;
}

}  // namespace paxos