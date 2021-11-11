#pragma once

#include <whirl/node/cluster/peer.hpp>

#include <commute/rpc/call.hpp>

#include <await/futures/combine/quorum.hpp>

#include <kv/node/roles/replica.hpp>
#include <kv/node/timestamps/stamped_value.hpp>

using namespace whirl;

class Coordinator : public commute::rpc::ServiceBase<Coordinator>,
                    public node::cluster::Peer {
 public:
  Coordinator();

  void RegisterMethods() override;

  void Set(const Key& key, Value value);
  Value Get(const Key& key);

 private:
  template <typename T, typename... Args>
  std::vector<await::futures::Future<T>> Call(const std::string& method,
                                              Args&&... args);

  await::futures::Future<void> Commit(const Key& key, const StampedValue& sv);

  void SetStamped(const Key& key, const StampedValue& sv);
  StampedValue GetStamped(const Key& key);

  void WaitTill(const node::time::WallTime& wt) const;

  WriteTimestamp ChooseTimestamp() const;

  StampedValue FindMostRecent(const std::vector<StampedValue>& values) const;

  node::time::WallTime IntToWallTime(uint64_t val) const;
  uint64_t WallTimeToInt(const node::time::WallTime& wt) const;

  size_t Majority() const;

 private:
  mutable timber::Logger logger_;
};
