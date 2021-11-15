#include <rsm/replica/multipaxos.hpp>

#include <rsm/replica/store/log.hpp>

#include <commute/rpc/call.hpp>

#include <await/fibers/core/api.hpp>
#include <await/fibers/sync/channel.hpp>
#include <await/fibers/sync/select.hpp>
#include <await/fibers/sync/mutex.hpp>

#include <await/futures/util/never.hpp>

#include <timber/log.hpp>

#include <whirl/node/runtime/shortcuts.hpp>
#include <whirl/node/cluster/peer.hpp>

// TODO:
#include <rsm/replica/paxos/roles/proposer.hpp>
#include <rsm/replica/paxos/roles/acceptor.hpp>

#include <whirl/node/store/kv.hpp>

#include <set>

using await::fibers::Channel;
using await::futures::Future;
using await::futures::Promise;

using namespace whirl;

namespace rsm {

//////////////////////////////////////////////////////////////////////

struct Commit {
  size_t idx;
  Command command;
};

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
    auto [f, p] = await::futures::MakeContract<Response>();

    ps_.emplace(command.request_id, std::move(p));
    commands_.TrySend(command);

    return std::move(f);
  };

 private:
  void Start(commute::rpc::IServer* server) {
    // Reset state machine state
    state_machine_->Reset();

    // Open log on disk
    log_.Open();

    // Launch pipeline fibers
    await::fibers::Go([this]() {
      while (true) {
        auto next = await::fibers::Select(commands_, commits_);
        if (next.index() == 0) {
          auto command = get<0>(next);
          size_t idx = latest_applied_ + 1;
          while (!log_.IsEmpty(idx)) {
            ++idx;
          }
          log_.Update(idx, LogEntry::Empty());

          commute::rpc::Call("Proposer.Propose")
              .Args(command, idx)
              .Via(LoopBack())
              .Start()
              .As<Command>()
              .Subscribe([this, idx, command](wheels::Result<Command>&& res) {
                if (*res != command) {
                  commands_.TrySend(command);
                }
                commits_.TrySend({idx, *res});
              });
        } else {
          auto commit = get<1>(next);

          log_.Update(commit.idx, {commit.command, true, false});

          to_apply_.emplace(commit.idx);
          if (latest_applied_ + 1 == commit.idx) {
            for (size_t i = commit.idx; to_apply_.contains(i); ++i) {
              applyings_.TrySend(i);
              latest_applied_ = i;
            }
          }
        }
      }
    });

    await::fibers::Go([this]() {
      while (true) {
        size_t idx = applyings_.Receive();
        auto command = log_.Read(idx)->command;
        LOG_INFO("Executing command {}", command);
        auto resp = state_machine_->Apply(command);
        if (ps_.contains(command.request_id)) {
          LOG_INFO("Setting promise for {}", command);
          std::move(ps_.at(command.request_id)).SetValue(Ack{resp});
        }
      }
    });

    // Register RPC services
    auto db_path = node::rt::Config()->GetString("db.path");
    node::rt::Database()->Open(db_path);

    server->RegisterService("Proposer", std::make_shared<paxos::Proposer>());
    server->RegisterService("Acceptor", std::make_shared<paxos::Acceptor>());
  }

 private:
  // Replicated state
  IStateMachinePtr state_machine_;

  // Persistent log
  Log log_;

  await::fibers::Channel<Command> commands_;
  await::fibers::Channel<Commit> commits_;
  await::fibers::Channel<size_t> applyings_;

  std::map<RequestId, await::futures::Promise<Response>> ps_;
  std::set<size_t> to_apply_;
  size_t latest_applied_{0};

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
