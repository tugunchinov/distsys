#include <kv/node/main.hpp>

#include <await/futures/util/never.hpp>

#include <whirl/node/rpc/server.hpp>

#include "roles/coordinator.hpp"

//////////////////////////////////////////////////////////////////////

void KVNodeMain() {
  node::rt::PrintLine("Starting at {}", node::rt::WallTimeNow());

  // Open local database

  auto db_path = node::rt::Config()->GetString("db.path");
  node::rt::Database()->Open(db_path);

  // Start RPC server

  auto rpc_port = node::rt::Config()->GetInt<uint16_t>("rpc.port");
  auto rpc_server = node::rpc::MakeServer(rpc_port);

  rpc_server->RegisterService("KV", std::make_shared<Coordinator>());
  rpc_server->RegisterService("Replica", std::make_shared<Replica>());

  rpc_server->Start();

  // Serving ...

  await::futures::BlockForever();
}
