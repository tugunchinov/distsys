#include <kv/node/roles/replica.hpp>

Replica::Replica()
    : kv_store_(node::rt::Database(), "data"),
      logger_("KVNode.Replica", node::rt::LoggerBackend()) {
}

void Replica::RegisterMethods() {
  COMMUTE_RPC_REGISTER_METHOD(LocalWrite);
  COMMUTE_RPC_REGISTER_METHOD(LocalRead);
}

void Replica::LocalWrite(Key key, StampedValue target_value) {
  std::lock_guard lock(m_);
  auto local_value = LocalRead(key);
  if (local_value.timestamp < target_value.timestamp) {
    Update(key, target_value);
  }
}

StampedValue Replica::LocalRead(Key key) {
  auto val = kv_store_.GetOr(key, {"", WriteTimestamp::Min()});
  LOG_INFO("Read '{}' -> {}", key, val);
  return val;
}

void Replica::Update(Key key, StampedValue target_value) {
  LOG_INFO("Write '{}' -> {}", key, target_value);
  kv_store_.Put(key, target_value);
}
