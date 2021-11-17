#pragma once

#include <await/futures/core/promise.hpp>
#include <await/futures/combine/detail/combine.hpp>
#include <await/futures/helpers.hpp>

#include <await/support/thread_spinlock.hpp>

#include <rsm/replica/paxos/proposal.hpp>
#include <rsm/replica/paxos/proto.hpp>

#include <optional>
#include <vector>

namespace paxos {

template <typename Phase>
struct Verdict {
  explicit Verdict(ProposalNumber n = ProposalNumber::Zero()) : advice(n) {
  }
  explicit Verdict(std::vector<typename Phase::Response> votes)
      : accepted(true), votes(std::move(votes)) {
  }

  bool accepted{false};

  ProposalNumber advice{};
  std::vector<typename Phase::Response> votes;
};

template <typename Phase>
class PaxosQuorumCombinator {
 public:
  PaxosQuorumCombinator(size_t num_inputs, size_t threshold)
      : num_inputs_(num_inputs), threshold_(threshold) {
    values_.reserve(threshold_);
  }

  void ProcessInput(wheels::Result<typename Phase::Response> result,
                    size_t /*index*/) {
    auto guard = mutex_.Guard();

    if (completed_) {
      return;
    }

    if (result.IsOk()) {
      if (result->ack) {
        ++oks_;
        values_.push_back(std::move(*result));
      } else {
        ++nacks_;
        best_advice_ = std::max(best_advice_, result->advice);
      }
    } else {
      ++errors_;
    }

    if (oks_ == threshold_) {
      completed_ = true;
      std::move(*promise_).Set(MakeVotes());
    } else if (ImpossibleToReachThreshold()) {
      completed_ = true;
      if (nacks_ > 0) {
        std::move(*promise_).Set(MakeAdvice());
      } else {
        std::move(*promise_).SetError(result.GetError());
      }
    }
  }

  await::futures::Future<Verdict<Phase>> MakeFuture() {
    auto [f, p] = await::futures::MakeContract<Verdict<Phase>>();
    promise_.emplace(std::move(p));
    return std::move(f);
  }

 private:
  bool ImpossibleToReachThreshold() const {
    return errors_ + nacks_ + threshold_ > num_inputs_;
  }

  auto MakeAdvice() const {
    return wheels::make_result::Ok(Verdict<Phase>{best_advice_});
  }

  auto MakeVotes() {
    return wheels::make_result::Ok(Verdict<Phase>{std::move(values_)});
  }

 private:
  const size_t num_inputs_;
  const size_t threshold_;

  await::support::ThreadSpinLock mutex_;
  size_t oks_{0};
  size_t errors_{0};
  size_t nacks_{0};
  std::vector<typename Phase::Response> values_;
  ProposalNumber best_advice_;
  std::optional<await::futures::Promise<Verdict<Phase>>> promise_;
  bool completed_{false};
};

template <typename Phase>
auto PaxosQuorum(
    std::vector<await::futures::Future<typename Phase::Response>> inputs,
    size_t threshold) {
  if (threshold == 0) {
    return await::futures::MakeValue(Verdict<Phase>{});
  }
  if (inputs.size() < threshold) {
    WHEELS_PANIC(
        "Number of inputs < required threshold, output future never completes");
  }

  return await::futures::detail::Combine<PaxosQuorumCombinator<Phase>>(
      std::move(inputs), threshold);
}

}  // namespace paxos
