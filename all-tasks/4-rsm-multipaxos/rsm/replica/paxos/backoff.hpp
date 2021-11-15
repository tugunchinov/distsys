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
  explicit Backoff(Params params);

  // Returns backoff delay
  Millis operator()();

 private:
  Millis ComputeNext(Millis curr);

 private:
  const Params params_;
  Millis next_;
};

}  // namespace paxos
