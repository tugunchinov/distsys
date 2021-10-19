#pragma once

#include <whirl/node/cluster/peer.hpp>

#include <commute/rpc/call.hpp>

#include <await/futures/combine/quorum.hpp>

#include <kv/node/roles/replica.hpp>
#include <kv/node/timestamps/stamped_value.hpp>

#include <await/fibers/sync/mutex.hpp>

using namespace whirl;

class Coordinator : public commute::rpc::ServiceBase<Coordinator>,
                    public node::cluster::Peer {
 public:
  Coordinator();

  void RegisterMethods() override;

  void Set(Key key, Value value);
  Value Get(Key key);

 private:
  WriteTimestamp ChooseWriteTimestamp(Key key);

  int64_t GetMyId() const;
  uint64_t GetLocalMonotonicNow() const;
  uint64_t GetNextTimestamp(Key key) const;

  StampedValue FindMostRecent(const std::vector<StampedValue>& values) const;

  void SetStamped(Key key, StampedValue sv);
  StampedValue GetStamped(Key key) const;

  size_t Majority() const;

 private:
  timber::Logger logger_;
};
