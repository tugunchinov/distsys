#pragma once

#include <commute/rpc/service_base.hpp>

#include <cereal/types/string.hpp>

#include <kv/node/timestamps/stamped_value.hpp>

#include <muesli/serializable.hpp>

#include <timber/log.hpp>

#include <whirl/node/runtime/shortcuts.hpp>
#include <whirl/node/store/kv.hpp>

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
  struct VersionedKey {
    uint64_t version;
    Key key;

    MUESLI_SERIALIZABLE(version, key)
  };

 private:
  uint64_t GetLatestSequenceNumber() const;

 private:
  std::atomic<uint64_t> s_{0};
  node::store::KVStore<StampedValue> kv_store_;
  timber::Logger logger_;
};
