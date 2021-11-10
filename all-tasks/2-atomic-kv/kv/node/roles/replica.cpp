#include <kv/node/roles/replica.hpp>

struct VersionedKey {
  uint64_t version;
  Key key;

  MUESLI_SERIALIZABLE(version, key)
};

Replica::Replica()
    : kv_store_(node::rt::Database()),
      logger_("KVNode.Replica", node::rt::LoggerBackend()) {
}

void Replica::RegisterMethods() {
  COMMUTE_RPC_REGISTER_METHOD(LocalWrite);
  COMMUTE_RPC_REGISTER_METHOD(LocalRead);
}

void Replica::LocalWrite(const Key& key, StampedValue target_value) {
  LOG_INFO("Write '{}' -> {}", key, target_value);
  kv_store_.Put(key, target_value.timestamp, target_value);
}

StampedValue Replica::LocalRead(const Key& key) {
  return kv_store_.GetLatestOr(key, {"", WriteTimestamp::Min()});
}
