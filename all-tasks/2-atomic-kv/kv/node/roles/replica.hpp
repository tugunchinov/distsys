#pragma once

#include <commute/rpc/service_base.hpp>

#include <cereal/types/string.hpp>

#include <kv/node/timestamps/stamped_value.hpp>
#include <kv/node/store/versioned_kv_store.hpp>

#include <muesli/serializable.hpp>

#include <timber/log.hpp>

#include <whirl/node/runtime/shortcuts.hpp>

#include <string>

using namespace whirl;

using Key = std::string;
using Value = std::string;

class Replica : public commute::rpc::ServiceBase<Replica> {
 public:
  Replica();

  void RegisterMethods() override;

  void LocalWrite(const Key& key, StampedValue target_value);
  StampedValue LocalRead(const Key& key);

 private:
  VersionedKVStore<WriteTimestamp, StampedValue> kv_store_;
  timber::Logger logger_;
};
