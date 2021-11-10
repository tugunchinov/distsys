#ifndef VER_VK_STORE_IMPL
#error Do not include this file directly
#endif

#include <fmt/core.h>

#include <kv/node/store/versioned_kv_store.hpp>

#include <muesli/serialize.hpp>
#include <muesli/tuple.hpp>

#include <whirl/node/db/database.hpp>

template <typename Ver, typename Val>
VersionedKVStore<Ver, Val>::VersionedKVStore(whirl::node::db::IDatabase* db)
    : db_(db) {
}

template <typename Ver, typename Val>
void VersionedKVStore<Ver, Val>::Put(const std::string& key, const Ver& version,
                                     const Val& value) {
  auto value_bytes = muesli::Serialize(value);
  auto versioned_key = muesli::SerializeValues(key, version);
  db_->Put(versioned_key, value_bytes);
}

template <typename Ver, typename Val>
std::optional<Val> VersionedKVStore<Ver, Val>::TryGetLatest(
    const std::string& key) const {
  auto it = db_->MakeSnapshot()->MakeIterator();
  std::optional<std::string> value_bytes = std::nullopt;
  Ver latest_version{Ver::Min()};
  while (it->Valid()) {
    auto [cur_key, cur_version] = GetKeyAndVersion(it);
    if (cur_key == key && (!value_bytes || latest_version < cur_version)) {
      latest_version = cur_version;
      value_bytes = it->Value();
    }
    it->Next();
  }

  if (value_bytes.has_value()) {
    return muesli::Deserialize<Val>(*value_bytes);
  } else {
    return std::nullopt;
  }
}

template <typename Ver, typename Val>
Val VersionedKVStore<Ver, Val>::GetLatest(const std::string& key) const {
  std::optional<Val> existing_value = TryGetLatest(key);
  if (existing_value.has_value()) {
    return *existing_value;
  } else {
    throw std::runtime_error(
        fmt::format("Key '{}' not found in local KV storage", key));
  }
}

template <typename Ver, typename Val>
Val VersionedKVStore<Ver, Val>::GetLatestOr(const std::string& key,
                                            Val or_value) const {
  return TryGetLatest(key).value_or(or_value);
}

template <typename Ver, typename Val>
std::tuple<std::string, Ver> VersionedKVStore<Ver, Val>::GetKeyAndVersion(
    whirl::node::db::IIteratorPtr it) const {
  return muesli::DeserializeValues<std::string, Ver>(std::string(it->Key()));
}
