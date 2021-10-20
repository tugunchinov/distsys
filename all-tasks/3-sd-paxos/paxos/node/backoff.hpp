#pragma once

#include <whirl/node/time/jiffies.hpp>

namespace paxos {

struct Backoff {
  using Millis = whirl::Jiffies;

 public:
  struct Params {
    Millis init;
    Millis max;
    uint64_t factor;
  };

 public:
  explicit Backoff(Params params) : params_(params), next_(params.init) {
  }

  // Returns backoff delay
  Millis operator()() {
    auto curr = next_;
    next_ = ComputeNext(curr);
    return curr;
  }

 private:
  Millis ComputeNext(Millis curr) {
    return std::min(params_.max, curr * params_.factor);
  }

 private:
  const Params params_;
  Millis next_;
};

}  // namespace paxos
