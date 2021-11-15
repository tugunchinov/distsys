#include <rsm/replica/multipaxos.hpp>

#include <rsm/replica/store/log.hpp>

#include <commute/rpc/call.hpp>

#include <await/fibers/core/api.hpp>
#include <await/fibers/sync/channel.hpp>
#include <await/fibers/sync/mutex.hpp>

#include <timber/log.hpp>

#include <whirl/node/runtime/shortcuts.hpp>
#include <whirl/node/cluster/peer.hpp>

// TODO:
#include <rsm/replica/paxos/roles/proposer.hpp>
#include <rsm/replica/paxos/roles/acceptor.hpp>

#include <whirl/node/store/kv.hpp>

using await::fibers::Channel;
using await::futures::Future;
using await::futures::Promise;

using namespace whirl;

namespace rsm {

//////////////////////////////////////////////////////////////////////

class MultiPaxos : public IReplica, public node::cluster::Peer {
 public:
  MultiPaxos(IStateMachinePtr state_machine, persist::fs::Path store_dir,
             commute::rpc::IServer* server)
      : Peer(node::rt::Config()),
        state_machine_(std::move(state_machine)),
        log_(store_dir),
        logger_("Replica", node::rt::LoggerBackend()) {
    Start(server);
  }

  Future<Response> Execute(Command command) override {
    auto [future, promise] = await::futures::MakeContract<Response>();

    await::fibers::Go([this, p = std::move(promise), command]() mutable {
      uint64_t idx{1};

      while (true) {
        auto current_command = command;
        {
          auto guard = mutex_.Guard();

          while (!log_.IsEmpty(idx)) {
            ++idx;
          }

          log_.Update(idx, LogEntry::Empty());
        }

        Command app_com;
        for (const auto& peer : ListPeers().WithMe()) {
          auto applied = await::fibers::Await(
              commute::rpc::Call("Proposer.Propose")
                  .Args(muesli::Serialize(current_command), idx)
                  .Via(Channel(peer))
                  .AtLeastOnce()
                  .Start()
                  .As<muesli::Bytes>());
          if (applied.IsOk()) {
            app_com = muesli::Deserialize<Command>(applied.ValueOrThrow());
            break;
          }
        }

        {
          auto guard = mutex_.Guard();

          if (app_com == command) {
            LOG_INFO("Executing command {}", app_com);

            auto response = state_machine_->Apply(app_com);
            std::move(p).SetValue(Ack{response});
            break;
          }
        }
      }
    });

    return std::move(future);
  };

  void Start(commute::rpc::IServer* server) {
    // Reset state machine state
    state_machine_->Reset();

    // Open log on disk
    log_.Open();

    // Launch pipeline fibers
    // ...

    // Register RPC services
    auto db_path = node::rt::Config()->GetString("db.path");
    node::rt::Database()->Open(db_path);

    server->RegisterService("Proposer", std::make_shared<paxos::Proposer>());
    server->RegisterService("Acceptor", std::make_shared<paxos::Acceptor>());
    server->RegisterService("Learner", std::make_shared<paxos::Learner>());
  }

 private:
  // Replicated state
  IStateMachinePtr state_machine_;

  // Persistent log
  Log log_;

  await::fibers::Mutex mutex_;

  // Logging
  timber::Logger logger_;
};

//////////////////////////////////////////////////////////////////////

IReplicaPtr MakeMultiPaxosReplica(IStateMachinePtr state_machine,
                                  commute::rpc::IServer* server) {
  auto store_dir =
      node::rt::Fs()->MakePath(node::rt::Config()->GetString("rsm.store.dir"));

  return std::make_shared<MultiPaxos>(std::move(state_machine),
                                      std::move(store_dir), server);
}

}  // namespace rsm
