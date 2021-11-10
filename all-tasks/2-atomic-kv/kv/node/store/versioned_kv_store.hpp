#pragma once

#include <whirl/node/db/database.hpp>

#include <atomic>

template <typename Ver, typename Val>
class VersionedKVStore {
 public:
  explicit VersionedKVStore(whirl::node::db::IDatabase* db);

  // Non-copyable
  VersionedKVStore(const VersionedKVStore&) = delete;
  VersionedKVStore& operator=(const VersionedKVStore&) = delete;

  void Put(const std::string& key, const Ver& version, const Val& value);

  std::optional<Val> TryGetLatest(const std::string& key) const;
  Val GetLatest(const std::string& key) const;
  Val GetLatestOr(const std::string& key, Val or_value) const;

 private:
  std::tuple<std::string, Ver> GetKeyAndVersion(
      whirl::node::db::IIteratorPtr it) const;

 private:
  whirl::node::db::IDatabase* db_;
};

#define VER_VK_STORE_IMPL
#include <kv/node/store/versioned_kv_store.ipp>
#undef VER_VK_STORE_IMPL
