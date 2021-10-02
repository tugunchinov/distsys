#include "coordinator.hpp"

using await::fibers::Await;
using await::futures::Future;

Coordinator::Coordinator()
    : Peer(node::rt::Config()),
      logger_("KVNode.Coordinator", node::rt::LoggerBackend()) {
}

void Coordinator::RegisterMethods() {
  COMMUTE_RPC_REGISTER_METHOD(Set);
  COMMUTE_RPC_REGISTER_METHOD(Get);
}

void Coordinator::Set(Key key, Value value) {
  std::lock_guard lock(m_);
  WriteTimestamp write_ts = ChooseWriteTimestamp(key);
  SetStamped(key, {value, write_ts});
}

Value Coordinator::Get(Key key) {
  std::lock_guard lock(m_);
  auto most_recent = GetStamped(key);
  SetStamped(key, most_recent);
  return most_recent.value;
}

WriteTimestamp Coordinator::ChooseWriteTimestamp(Key /*key*/) {
  // return GetStamped(key).timestamp + 1;
  // return {whirl::node::rt::WallTimeNow().ToJiffies().Count()};
  auto tt_now = node::rt::TrueTime()->Now();
  return {tt_now.earliest.ToJiffies().Count() +
          (tt_now.latest.ToJiffies().Count() -
           tt_now.earliest.ToJiffies().Count()) /
              2};
}

StampedValue Coordinator::FindMostRecent(
    const std::vector<StampedValue>& values) const {
  return *std::max_element(
      values.begin(), values.end(),
      [](const StampedValue& lhs, const StampedValue& rhs) {
        return lhs.timestamp < rhs.timestamp;
      });
}

void Coordinator::SetStamped(Key key, StampedValue sv) {
  LOG_INFO("Write timestamp: {}", sv.timestamp);

  std::vector<Future<void>> writes;
  for (const auto& peer : ListPeers().WithMe()) {
    writes.push_back(commute::rpc::Call("Replica.LocalWrite")
                         .Args<Key, StampedValue>(key, {sv.value, sv.timestamp})
                         .Via(Channel(peer))
                         .Context(await::context::ThisFiber())
                         .AtLeastOnce());
  }
  Await(Quorum(std::move(writes), /*threshold=*/Majority())).ThrowIfError();
}

StampedValue Coordinator::GetStamped(Key key) {
  std::vector<Future<StampedValue>> reads;
  for (const auto& peer : ListPeers().WithMe()) {
    reads.push_back(commute::rpc::Call("Replica.LocalRead")
                        .Args(key)
                        .Via(Channel(peer))
                        .Context(await::context::ThisFiber())
                        .AtLeastOnce());
  }
  auto stamped_values =
      Await(Quorum(std::move(reads), /*threshold=*/Majority())).ValueOrThrow();
  auto most_recent = FindMostRecent(stamped_values);
  return most_recent;
}

size_t Coordinator::Majority() const {
  return NodeCount() / 2 + 1;
}