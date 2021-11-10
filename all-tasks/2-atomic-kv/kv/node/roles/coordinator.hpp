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
  WriteTimestamp ChooseWriteTimestamp() const;

  StampedValue FindMostRecent(const std::vector<StampedValue>& values) const;

  void SetStamped(const Key& key, StampedValue sv);
  StampedValue GetStamped(const Key& key) const;

  size_t Majority() const;

 private:
  timber::Logger logger_;
};
