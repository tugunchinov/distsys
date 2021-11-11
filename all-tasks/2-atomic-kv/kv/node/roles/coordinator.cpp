#include <kv/node/roles/coordinator.hpp>

using await::fibers::Await;
using await::fibers::Go;
using await::fibers::self::SetContext;
using await::futures::Future;

using namespace await::context;
using namespace node;

Coordinator::Coordinator()
    : Peer(rt::Config()), logger_("KVNode.Coordinator", rt::LoggerBackend()) {
}

void Coordinator::RegisterMethods() {
  COMMUTE_RPC_REGISTER_METHOD(Set);
  COMMUTE_RPC_REGISTER_METHOD(Get);
}

void Coordinator::Set(const Key& key, Value value) {
  auto write_ts = ChooseTimestamp();
  LOG_INFO("Write timestamp: {}", write_ts);
  auto f = Commit(key, {value, write_ts});
  WaitTill(IntToWallTime(write_ts.time));
  Await(std::move(f)).ExpectOk();
}

Value Coordinator::Get(const Key& key) {
  auto most_recent = GetStamped(key);
  LOG_INFO("Read {} -> {}", key, most_recent);
  auto f = Commit(key, most_recent);
  WaitTill(IntToWallTime(most_recent.timestamp.time));
  Await(std::move(f)).ExpectOk();
  return most_recent.value;
}

await::futures::Future<void> Coordinator::Commit(const Key& key,
                                                 const StampedValue& sv) {
  auto [f, p] = await::futures::MakeContract<void>();
  Go([this, key, sv, p = std::move(p)]() mutable {
    StopScope stop_scope;
    SetContext(New().StopSource(stop_scope).Done());
    SetStamped(key, sv);
    std::move(p).Set();
  });

  return std::move(f);
}

void Coordinator::SetStamped(const Key& key, const StampedValue& sv) {
  Await(Quorum(Call<void>("Replica.LocalWrite", key, sv), Majority()))
      .ThrowIfError();
}

StampedValue Coordinator::GetStamped(const Key& key) {
  return FindMostRecent(
      Await(Quorum(Call<StampedValue>("Replica.LocalRead", key), Majority()))
          .ValueOrThrow());
}

template <typename T, typename... Args>
std::vector<Future<T>> Coordinator::Call(const std::string& method,
                                         Args&&... args) {
  std::vector<Future<T>> calls;
  for (const auto& peer : ListPeers().WithMe()) {
    calls.push_back(commute::rpc::Call(method)
                        .Args<Args...>(std::forward<Args>(args)...)
                        .Via(Channel(peer))
                        .Context(await::context::ThisFiber())
                        .AtLeastOnce());
  }
  return std::move(calls);
}

void Coordinator::WaitTill(const node::time::WallTime& wt) const {
  while (!rt::TrueTime()->After(wt)) {
    auto e = rt::TrueTime()->Now().earliest;
    if (e < wt) {
      Await(rt::After(wt - e)).ExpectOk();
    }
  }
}

WriteTimestamp Coordinator::ChooseTimestamp() const {
  auto ts = rt::TrueTime()->Now().latest;
  return {WallTimeToInt(ts), rt::GenerateGuid()};
}

StampedValue Coordinator::FindMostRecent(
    const std::vector<StampedValue>& values) const {
  return *std::max_element(
      values.begin(), values.end(),
      [](const StampedValue& lhs, const StampedValue& rhs) {
        return lhs.timestamp < rhs.timestamp;
      });
}

size_t Coordinator::Majority() const {
  return NodeCount() / 2 + 1;
}

time::WallTime Coordinator::IntToWallTime(uint64_t val) const {
  return time::WallTime(val);
}

uint64_t Coordinator::WallTimeToInt(const time::WallTime& wt) const {
  return wt.ToJiffies().Count();
}
