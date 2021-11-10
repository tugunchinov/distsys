#include <kv/node/roles/replica.hpp>

Replica::Replica()
    : s_(GetLatestSequenceNumber()),
      kv_store_(node::rt::Database(), "data"),
      logger_("KVNode.Replica", node::rt::LoggerBackend()) {
}

void Replica::RegisterMethods() {
  COMMUTE_RPC_REGISTER_METHOD(LocalWrite);
  COMMUTE_RPC_REGISTER_METHOD(LocalRead);
}

void Replica::LocalWrite(const Key& key, StampedValue target_value) {
  LOG_INFO("Write '{}' -> {}", key, target_value);
  auto versioned_key = VersionedKey{s_.fetch_add(1), key};
  kv_store_.Put(muesli::Serialize(versioned_key), target_value);
}

StampedValue Replica::LocalRead(const Key& key) {
  auto it = node::rt::Database()->MakeSnapshot()->MakeIterator();
  it->SeekToLast();
  while (it->Valid()) {
    std::string s = std::string(it->Key());
    auto versioned_key = muesli::Deserialize<VersionedKey>(s);
    if (key == versioned_key.key) {
      LOG_INFO("Read '{}' -> {}", key, it->Value());
      return muesli::Deserialize<StampedValue>(it->Value().data());
    }
    it->Prev();
  }

  LOG_INFO("Read '{}' -> {}", key, "");
  return {"", WriteTimestamp::Min()};
}

uint64_t Replica::GetLatestSequenceNumber() const {
  auto it = node::rt::Database()->MakeSnapshot()->MakeIterator();
  if (!it->Valid()) {
    return 0;
  }
  it->SeekToLast();
  auto versioned_key = muesli::Deserialize<VersionedKey>(it->Key().data());
  return versioned_key.version;
}
