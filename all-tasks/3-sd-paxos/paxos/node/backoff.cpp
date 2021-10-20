#include <paxos/node/backoff.hpp>

namespace paxos {

Backoff::Backoff(Params params) : params_(params), next_(params.init) {
}

Backoff::Millis Backoff::operator()() {
  auto curr = next_;
  next_ = ComputeNext(curr);
  return curr;
}

Backoff::Millis Backoff::ComputeNext(Backoff::Millis curr) {
  return std::min(params_.max, curr * params_.factor);
}

}  // namespace paxos
