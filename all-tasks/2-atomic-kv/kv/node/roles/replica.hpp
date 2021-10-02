#pragma once

#include <whirl/node/runtime/shortcuts.hpp>
#include <whirl/node/store/kv.hpp>

#include <commute/rpc/service_base.hpp>

#include <timber/log.hpp>

#include <node/timestamps/stamped_value.hpp>

#include <await/fibers/sync/mutex.hpp>

using namespace whirl;

using Key = std::string;
using Value = std::string;

class Replica : public commute::rpc::ServiceBase<Replica> {
 public:
  Replica();

  void RegisterMethods() override;

  void LocalWrite(Key key, StampedValue target_value);
  StampedValue LocalRead(Key key);

 private:
  void Update(Key key, StampedValue target_value);

 private:
  node::store::KVStore<StampedValue> kv_store_;
  timber::Logger logger_;
  await::fibers::Mutex m_;
};
