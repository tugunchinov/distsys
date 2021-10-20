#pragma once

#include <paxos/node/proposal.hpp>

#include <await/futures/combine/quorum.hpp>

#include <commute/rpc/call.hpp>

#include <paxos/node/proto.hpp>

#include <await/futures/core/promise.hpp>
#include <await/futures/combine/detail/combine.hpp>
#include <await/futures/combine/detail/traits.hpp>
#include <await/futures/helpers.hpp>

#include <await/support/thread_spinlock.hpp>

#include <twist/stdlike/atomic.hpp>

#include <optional>
#include <vector>

namespace paxos {

using namespace await::futures;
using namespace wheels::make_result;

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
    std::unique_lock lock(mutex_);

    if (completed_) {
      return;
    }

    if (result.IsOk()) {
      if (result->ack) {
        ++oks_;
        values_.push_back(std::move(*result));
        if (oks_ == threshold_) {
          completed_ = true;
          lock.unlock();
          std::move(*promise_).Set(Ok(Verdict<Phase>{std::move(values_)}));
        }
      } else {
        ++nacks_;
        best_advice_ = std::max(best_advice_, result->advice);
        if (ImpossibleToReachThreshold()) {
          completed_ = true;
          lock.unlock();
          std::move(*promise_).Set(Ok(Verdict<Phase>{best_advice_}));
        }
      }
    } else {
      ++errors_;
      if (ImpossibleToReachThreshold()) {
        completed_ = true;
        if (nacks_ > 0) {
          std::move(*promise_).Set(Ok(Verdict<Phase>{best_advice_}));
        } else {
          lock.unlock();
          std::move(*promise_).SetError(result.GetError());
        }
      }
    }
  }

  Future<Verdict<Phase>> MakeFuture() {
    auto [f, p] = MakeContract<Verdict<Phase>>();
    promise_.emplace(std::move(p));
    return std::move(f);
  }

 private:
  bool ImpossibleToReachThreshold() const {
    return errors_ + nacks_ + threshold_ > num_inputs_;
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
  std::optional<Promise<Verdict<Phase>>> promise_;
  bool completed_{false};
};

template <typename Phase>
auto PaxosQuorum(std::vector<Future<typename Phase::Response>> inputs,
                 size_t threshold) {
  if (threshold == 0) {
    return MakeValue(Verdict<Phase>{});
  }
  if (inputs.size() < threshold) {
    WHEELS_PANIC(
        "Number of inputs < required threshold, output future never completes");
  }

  return detail::Combine<PaxosQuorumCombinator<Phase>>(std::move(inputs),
                                                       threshold);
}

}  // namespace paxos
