#pragma clang diagnostic push
#pragma ide diagnostic ignored "EndlessLoop"

#include <await/fibers/core/api.hpp>
#include <await/fibers/sync/channel.hpp>
#include <await/fibers/sync/select.hpp>
#include <await/futures/util/never.hpp>

#include <cereal/types/map.hpp>

#include <commute/rpc/call.hpp>

#include <rsm/replica/multipaxos.hpp>
#include <rsm/replica/paxos/roles/proposer.hpp>
#include <rsm/replica/paxos/roles/acceptor.hpp>
#include <rsm/replica/store/log.hpp>

#include <timber/log.hpp>

#include <whirl/node/runtime/shortcuts.hpp>
#include <whirl/node/cluster/peer.hpp>
#include <whirl/node/store/kv.hpp>

#include <set>

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
        state_(node::rt::Database(), "RSM state"),
        log_(store_dir),
        logger_("Replica", node::rt::LoggerBackend()) {
    Start(server);
  }

  Future<Response> Execute(Command command) override {
    auto [f, p] = await::futures::MakeContract<Response>();

    promises_.emplace(command.request_id, std::move(p));
    commands_.TrySend(std::move(command));

    return std::move(f);
  };

 private:
  struct RSMState {
    muesli::Bytes sm_snapshot;
    std::map<std::string, size_t> last_applied;
    std::map<std::string, muesli::Bytes> last_response;

    MUESLI_SERIALIZABLE(sm_snapshot, last_applied, last_response)
  };

  struct Commit {
    size_t idx;
    Command command;
  };

 private:
  void Start(commute::rpc::IServer* server) {
    // Open log on disk
    log_.Open();

    // Reset state machine state
    state_machine_->Reset();
    RestoreState();

    // Launch pipeline fibers
    await::fibers::Go([this]() {
      while (true) {
        auto next = await::fibers::Select(commands_, commits_);

        if (const Command* command = std::get_if<Command>(&next)) {
          size_t idx = ChooseIndexForCommand();
          ProposeCommand(std::move(*command), idx);
        } else if (const Commit* commit = std::get_if<Commit>(&next)) {
          MakeCommit(*commit);
          if (last_sent_ + 1 == commit->idx) {
            SendCommitted();
          }
        }
      }
    });

    await::fibers::Go([this]() {
      while (true) {
        auto commands = to_apply_.Receive();
        for (auto command : commands) {
          if (IsOldCommand(command)) {
            continue;
          }
          if (!HaveResponse(command)) {
            ApplyCommand(std::move(command));
          }
        }
        SaveState();
        for (const auto& command : commands) {
          if (HaveResponse(command) && NeedRespond(command)) {
            Respond(command.request_id,
                    std::move(last_response_[command.request_id.client_id]));
          }
        }
      }
    });

    // Register RPC services
    server->RegisterService("Proposer", std::make_shared<paxos::Proposer>());
    server->RegisterService("Acceptor", std::make_shared<paxos::Acceptor>());
    server->RegisterService("Learner", std::make_shared<paxos::Learner>());
  }

  void RestoreState() {
    LOG_INFO("Restoring RSM state");
    if (auto state = state_.TryGet("state")) {
      last_applied_ = state->last_applied;
      last_response_ = state->last_response;
      state_machine_->InstallSnapshot(state->sm_snapshot);
    }

    for (size_t i = 1; !log_.IsEmpty(i); ++i) {
      ProposeCommand(log_.Read(i)->command, i);
    }
  }

  size_t ChooseIndexForCommand() {
    size_t idx = last_sent_ + 1;
    while (!log_.IsEmpty(idx)) {
      ++idx;
    }
    log_.Update(idx, LogEntry::Empty());
    return idx;
  }

  void ProposeCommand(Command command, size_t idx) {
    LOG_INFO("Start proposing {} at {}", command, idx);
    commute::rpc::Call("Proposer.Propose")
        .Args(command, idx)
        .Via(LoopBack())
        .Start()
        .As<Command>()
        .Subscribe([this, command, idx](wheels::Result<Command>&& res) {
          commits_.TrySend({idx, *res});
          if (*res != command) {
            commands_.TrySend(std::move(command));
          }
        });
  }

  void MakeCommit(Commit commit) {
    LOG_INFO("Committing");
    log_.Update(commit.idx, {std::move(commit.command)});
    committed_commands_.emplace(commit.idx);
  }

  void SendCommitted() {
    LOG_INFO("Sending committed commands");
    std::vector<Command> commands;
    for (size_t i = *committed_commands_.begin();
         committed_commands_.contains(i); ++i) {
      commands.push_back(log_.Read(i)->command);
      committed_commands_.erase(i);
      last_sent_ = i;
    }
    to_apply_.TrySend(std::move(commands));
  }

  void ApplyCommand(Command command) {
    if (NoOp(command)) {
      return;
    }

    LOG_INFO("Executing command {}", command);
    last_response_[command.request_id.client_id] =
        state_machine_->Apply(command);
    last_applied_[command.request_id.client_id] = command.request_id.index;
  }

  // TODO: client_id
  void Respond(const RequestId& request_id, muesli::Bytes response) {
    LOG_INFO("Respond to {}", request_id);
    std::move(promises_.at(request_id)).SetValue(Ack{std::move(response)});
    promises_.erase(request_id);
  }

  void SaveState() {
    LOG_INFO("Saving RSM state");
    state_.Put("state",
               {state_machine_->MakeSnapshot(), last_applied_, last_response_});
  }

  [[nodiscard]] bool ClientServedAlready(const std::string& client_id) const {
    return last_applied_.contains(client_id);
  }

  [[nodiscard]] bool IsOldCommand(const Command& command) const {
    return ClientServedAlready(command.request_id.client_id) &&
           last_applied_.at(command.request_id.client_id) >
               command.request_id.index;
  }

  [[nodiscard]] bool NeedRespond(const Command& command) const {
    return promises_.contains(command.request_id);
  }

  [[nodiscard]] bool HaveResponse(const Command& command) const {
    return ClientServedAlready(command.request_id.client_id) &&
           last_applied_.at(command.request_id.client_id) ==
               command.request_id.index;
  }

  [[nodiscard]] bool NoOp(const Command& command) const {
    return command.type.empty();
  }

 private:
  // Replicated state
  IStateMachinePtr state_machine_;

  whirl::node::store::KVStore<RSMState> state_;

  // Persistent log
  Log log_;

  await::fibers::Channel<Command> commands_;
  await::fibers::Channel<Commit> commits_;
  await::fibers::Channel<std::vector<Command>> to_apply_;

  std::map<RequestId, await::futures::Promise<Response>> promises_;
  std::map<std::string, size_t> last_applied_;
  std::map<std::string, muesli::Bytes> last_response_;
  std::set<size_t> committed_commands_;

  size_t last_sent_{0};

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

#pragma clang diagnostic pop