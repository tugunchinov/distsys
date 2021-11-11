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
  // TODO: make better
  auto [e, l] = node::rt::TrueTime()->Now();
  WriteTimestamp write_ts{e.ToJiffies().Count(), l.ToJiffies().Count(),
                          node::rt::GenerateGuid()};

  auto [f, p] = await::futures::MakeContract<void>();
  await::fibers::Go(
      [&key, &value, &write_ts, this, p = std::move(p)]() mutable {
        await::fibers::self::SetContext(
            await::context::New()
                .StopToken(await::context::StopSource().GetToken())
                .Done());
        SetStamped(key, {value, write_ts});
        std::move(p).Set();
      });

  while (!node::rt::TrueTime()->After(l)) {
    Await(node::rt::After(l - e)).ExpectOk();
  }
  Await(std::move(f)).ExpectOk();
}

Value Coordinator::Get(const Key& key) {
  auto most_recent = FindMostRecent(
      Await(Quorum(Call<StampedValue>("Replica.LocalRead", key), Majority()))
          .ValueOrThrow());
  LOG_INFO("Read {} -> {}", key, most_recent);
  auto [f, p] = await::futures::MakeContract<void>();
  await::fibers::Go([&key, &most_recent, this, p = std::move(p)]() mutable {
    await::fibers::self::SetContext(
        await::context::New()
            .StopToken(await::context::StopSource().GetToken())
            .Done());
    SetStamped(key, most_recent);
    std::move(p).Set();
  });
  auto l = node::time::WallTime(most_recent.timestamp.l);
  auto e = node::time::WallTime(most_recent.timestamp.e);
  while (!node::rt::TrueTime()->After(l)) {
    Await(node::rt::After(l - e)).ExpectOk();
  }
  Await(std::move(f)).ExpectOk();
  return most_recent.value;
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
  Await(Quorum(Call<void>("Replica.LocalWrite", key, sv), Majority()))
      .ThrowIfError();
}

size_t Coordinator::Majority() const {
  return NodeCount() / 2 + 1;
}
template <typename T, typename... Args>
std::vector<Future<T>> Coordinator::Call(std::string method, Args... args) {
  std::vector<Future<T>> calls;
  for (const auto& peer : ListPeers().WithMe()) {
    calls.push_back(commute::rpc::Call(method)
                        .Args<Args...>(args...)
                        .Via(Channel(peer))
                        .Context(await::context::ThisFiber())
                        .AtLeastOnce());
  }
  return std::move(calls);
}
