#include <kv/node/roles/coordinator.hpp>

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

void Coordinator::Set(const Key& key, Value value) {
  WriteTimestamp write_ts = ChooseWriteTimestamp();
  SetStamped(key, {value, write_ts});
}

Value Coordinator::Get(const Key& key) {
  auto most_recent = GetStamped(key);
  SetStamped(key, most_recent);
  return most_recent.value;
}

WriteTimestamp Coordinator::ChooseWriteTimestamp() const {
  // TODO: parallel waiting
  auto [e, l] = node::rt::TrueTime()->Now();
  while (!node::rt::TrueTime()->After(l)) {
    await::fibers::Await(node::rt::After(l - e))
        .ExpectOk("ChooseWriteTimestamp");
  }
  return {l.ToJiffies().Count(), node::rt::GenerateGuid()};
}

StampedValue Coordinator::FindMostRecent(
    const std::vector<StampedValue>& values) const {
  return *std::max_element(
      values.begin(), values.end(),
      [](const StampedValue& lhs, const StampedValue& rhs) {
        return lhs.timestamp < rhs.timestamp;
      });
}

void Coordinator::SetStamped(const Key& key, StampedValue sv) {
  LOG_INFO("Write timestamp: {}", sv.timestamp);

  std::vector<Future<void>> writes;
  for (const auto& peer : ListPeers().WithMe()) {
    writes.push_back(commute::rpc::Call("Replica.LocalWrite")
                         .Args<Key, StampedValue>(key, sv)
                         .Via(Channel(peer))
                         .Context(await::context::ThisFiber())
                         .AtLeastOnce());
  }
  Await(Quorum(std::move(writes), /*threshold=*/Majority())).ThrowIfError();
}

StampedValue Coordinator::GetStamped(const Key& key) const {
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
  return FindMostRecent(stamped_values);
}

size_t Coordinator::Majority() const {
  return NodeCount() / 2 + 1;
}
