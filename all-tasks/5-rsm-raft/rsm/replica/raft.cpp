#include <rsm/replica/raft.hpp>

#include <rsm/replica/proto/raft.hpp>
#include <rsm/replica/store/log.hpp>

#include <commute/rpc/call.hpp>
#include <commute/rpc/service_base.hpp>

#include <await/fibers/core/api.hpp>
#include <await/fibers/sync/select.hpp>
#include <await/fibers/sync/channel.hpp>
#include <await/fibers/sync/mutex.hpp>
#include <await/futures/combine/quorum.hpp>

#include <timber/log.hpp>

#include <whirl/node/runtime/shortcuts.hpp>
#include <whirl/node/cluster/peer.hpp>
#include <whirl/node/store/kv.hpp>  // TODO: fs?

#include <cereal/types/map.hpp>

#include <mutex>
#include <optional>
#include <set>

using await::fibers::Await;

using await::futures::Future;
using await::futures::Promise;
using await::futures::Quorum;

using rsm::raft::proto::RequestVote;

using namespace whirl;

namespace rsm {

enum class NodeState {
  Candidate = 1,  // Do not format
  Follower = 2,
  Leader = 3
};

class Ticker {
 public:
  explicit Ticker(Jiffies tick_time) : tick_time_(tick_time) {
    RunTicker(tick_time_);
  }

  ~Ticker() {
    stopped_->TrySend({});
  }

  await::fibers::Channel<wheels::Unit>& GetTickChannel() {
    return *ticks_;
  }

  void Reset() {
    // TODO:

    stopped_->TrySend({});

    stopped_ = std::make_shared<await::fibers::Channel<wheels::Unit>>(1);
    ticks_ = std::make_shared<await::fibers::Channel<wheels::Unit>>(1);

    RunTicker(tick_time_);
  }

  void Receive() {
    ticks_->Receive();
  }

 private:
  void RunTicker(Jiffies tick_time) {
    await::fibers::Go([tick_time, ticks = ticks_, stopped = stopped_]() {
      for (;;) {
        if (stopped->TryReceive()) {
          return;
        }
        ticks->TrySend({});
        node::rt::SleepFor(tick_time);
      }
    });
  }

 private:
  std::shared_ptr<await::fibers::Channel<wheels::Unit>> ticks_{
      std::make_shared<await::fibers::Channel<wheels::Unit>>(1)};

  std::shared_ptr<await::fibers::Channel<wheels::Unit>> stopped_{
      std::make_shared<await::fibers::Channel<wheels::Unit>>(1)};

  Jiffies tick_time_;
};

class Raft : public IReplica,
             public commute::rpc::ServiceBase<Raft>,
             public node::cluster::Peer,
             public std::enable_shared_from_this<Raft> {
 private:
  struct State {
    size_t term;
    std::optional<std::string> voted_for;

    MUESLI_SERIALIZABLE(term, voted_for)
  };

 public:
  Raft(IStateMachinePtr state_machine, persist::fs::Path store_dir)
      : Peer(node::rt::Config()),
        state_machine_(std::move(state_machine)),
        log_(node::rt::Fs(), store_dir),
        store_(node::rt::Database(), "raft_state"),
        logger_("Raft", node::rt::LoggerBackend()) {
  }

  Future<proto::Response> Execute(Command command) override {
    auto [f, p] = await::futures::MakeContract<proto::Response>();

    {
      auto guard = mutex_.Guard();

      if (state_ == NodeState::Leader) {
        promises_.emplace(command.request_id, std::move(p));
        log_.Append({LogEntry{command, term_}});
        submit_trigger_.TrySend({});
      } else if (leader_) {
        std::move(p).SetValue(proto::RedirectToLeader{*leader_});
      } else {
        std::move(p).SetValue(proto::NotALeader{});
      }
    }

    return std::move(f);
  };

  void Start(commute::rpc::IServer* rpc_server) {
    state_machine_.Reset();
    RestoreState();

    log_.Open();

    rpc_server->RegisterService("Raft", shared_from_this());

    election_reset_event_ = node::rt::MonotonicNow().ToJiffies();

    RunElectionTimer(term_);
    ApplyCommittedCommands();
  }

 protected:
  // RPC handlers

  void RegisterMethods() override {
    COMMUTE_RPC_REGISTER_METHOD(ReplySuccess);
    COMMUTE_RPC_REGISTER_HANDLER(RequestVote);
    COMMUTE_RPC_REGISTER_HANDLER(AppendEntries);
  }

  bool ReplySuccess() {
    return true;
  }

  // Leader election

  void RequestVote(const raft::proto::RequestVote::Request& request,
                   raft::proto::RequestVote::Response* response) {
    auto guard = mutex_.Guard();

    if (request.term > term_) {
      LOG_INFO("Term out of date in RequestVote");
      BecomeFollower(request.term);
    }

    leader_ = std::nullopt;

    size_t last_log_index = log_.Length();
    size_t last_log_term = log_.LastLogTerm();

    response->vote_granted = false;
    if (request.term == term_ &&
        (!voted_for_ || voted_for_ == request.candidate) &&
        (request.last_log_term > last_log_term ||
         (request.last_log_term == last_log_term &&
          request.last_log_index >= last_log_index))) {
      response->vote_granted = true;
      leader_ = voted_for_ = request.candidate;
      election_reset_event_ = node::rt::MonotonicNow().ToJiffies();

      SaveState();
    }
    response->term = term_;
  }

  // Replication

  void AppendEntries(const raft::proto::AppendEntries::Request& request,
                     raft::proto::AppendEntries::Response* response) {
    auto guard = mutex_.Guard();

    leader_ = request.leader;
    election_reset_event_ = node::rt::MonotonicNow().ToJiffies();

    if (request.term > term_) {
      LOG_INFO("Term out of date in RequestVote");
      BecomeFollower(request.term);
    }

    response->success = false;
    if (request.term == term_) {
      if (state_ != NodeState::Follower) {
        BecomeFollower(request.term);
      }

      if (request.prev_log_index == 0 ||
          (request.prev_log_index <= log_.Length() &&
           request.prev_log_term == log_.Term(request.prev_log_index))) {
        response->success = true;

        size_t log_insert_index = request.prev_log_index + 1;
        size_t new_entries_index = 0;

        for (;;) {
          if (log_insert_index > log_.Length() ||
              new_entries_index >= request.entries.size()) {
            break;
          }

          if (log_.Term(log_insert_index) !=
              request.entries[new_entries_index].term) {
            break;
          }

          ++log_insert_index;
          ++new_entries_index;
        }

        if (new_entries_index < request.entries.size()) {
          LOG_INFO("... inserting entries from index {}", log_insert_index);
          if (log_insert_index <= log_.Length()) {
            log_.TruncateSuffix(log_insert_index);
          }
          log_.Append(request.entries, new_entries_index);
        }

        if (request.leader_commit_index > commit_index_) {
          commit_index_ = std::min(request.leader_commit_index, log_.Length());
          LOG_INFO("Setting commit index {}", commit_index_);
          new_commits_.TrySend({});
        }
      } else {
        if (request.prev_log_index > log_.Length()) {
          response->conflict_index = log_.Length() + 1;
          response->conflict_term = 0;
        } else {
          response->conflict_term = log_.Term(request.prev_log_index);
          response->conflict_index = 1;

          for (size_t i = request.prev_log_index - 1; i > 0; --i) {
            if (log_.Term(i) != response->conflict_term) {
              response->conflict_index = i + 1;
              break;
            }
          }
        }
      }
    }

    response->term = term_;
  }

 private:
  bool IsPreVoteSucceed() {
    std::vector<Future<bool>> requests;
    for (const auto& peer : ListPeers().WithoutMe()) {
      requests.push_back(commute::rpc::Call("Raft.ReplySuccess")
                             .Args()
                             .Via(Channel(peer))
                             .AtLeastOnce()
                             .Start()
                             .As<bool>());
    }

    return Await(Quorum(std::move(requests), Majority() - 1)).IsOk();
  }

 private:
  // Persistence

  void SaveState() {
    store_.Put("state", {term_, voted_for_});
  }

  void RestoreState() {
    if (auto state = store_.TryGet("state")) {
      term_ = state->term;
      voted_for_ = state->voted_for;
    }
  }

 private:
  // State changes

  // With mutex
  void BecomeFollower(size_t term) {
    for (auto it = promises_.begin(); it != promises_.end();
         promises_.erase(it++)) {
      std::move(it->second).SetValue(proto::NotALeader{});
    }

    LOG_INFO("Becomes follower in term {}", term);
    state_ = NodeState::Follower;
    term_ = term;
    voted_for_ = std::nullopt;
    election_reset_event_ = node::rt::MonotonicNow().ToJiffies();

    SaveState();

    RunElectionTimer(term);
  }

  // With mutex
  void BecomeCandidate() {
    state_ = NodeState::Candidate;
    ++term_;
    election_reset_event_ = node::rt::MonotonicNow().ToJiffies();
    voted_for_ = node::rt::HostName();
    votes_received_ = {*voted_for_};
    leader_ = std::nullopt;
    LOG_INFO("Becomes Candidate (current term = {})", term_);

    SaveState();

    for (const auto& peer : ListPeers().WithoutMe()) {
      RunRequestVote(peer, term_);
    }

    RunElectionTimer(term_);
  }

  // With mutex
  void BecomeLeader() {
    state_ = NodeState::Leader;

    for (const auto& peer : ListPeers().WithoutMe()) {
      next_index_[peer] = log_.Length() + 1;
      match_index_[peer] = 0;
    }

    LOG_INFO("Becomes leader in term {}", term_);

    await::fibers::Go([this]() {
      for (const auto& peer : ListPeers().WithoutMe()) {
        RunAppendEntries(peer, term_);
      }

      uint64_t rtt = node::rt::Config()->GetInt<uint64_t>("net.rtt");
      Ticker ticker(rtt / 2);

      for (;;) {
        await::fibers::Select(ticker.GetTickChannel(), submit_trigger_);

        auto guard = mutex_.Guard();

        if (state_ != NodeState::Leader) {
          LOG_INFO("Not a leader anymore");
          return;
        }

        LOG_INFO("Sending heartbeats");
        for (const auto& peer : ListPeers().WithoutMe()) {
          RunAppendEntries(peer, term_);
        }
      }
    });
  }

 private:
  // Fibers

  void ApplyCommittedCommands() {
    await::fibers::Go([this]() {
      for (;;) {
        new_commits_.Receive();

        auto guard = mutex_.Guard();
        if (commit_index_ > last_applied_) {
          for (size_t i = last_applied_ + 1; i <= commit_index_; ++i) {
            Command command = log_.Read(i).command;
            LOG_INFO("Executing command {}", command);
            auto response = state_machine_.Apply(command);

            if (state_ == NodeState::Leader &&
                promises_.contains(command.request_id)) {
              std::move(promises_.at(command.request_id))
                  .SetValue(proto::Ack{response});
              LOG_INFO("Responded to {}", command.request_id);
              promises_.erase(command.request_id);
            }
          }

          last_applied_ = commit_index_;
        }
      }
    });
  }

  void RunElectionTimer(size_t term) {
    await::fibers::Go([this, term]() {
      Jiffies timeout_duration = ElectionTimeout();

      Ticker ticker(timeout_duration);
      for (;;) {
        ticker.Receive();
        auto guard = mutex_.Guard();

        if (state_ == NodeState::Leader) {
          LOG_INFO("In election timer state = {}, bailing out", state_);
          return;
        }

        if (term != term_) {
          LOG_INFO("In election timer term changed from {} to {}, bailing out",
                   term, term_);
          return;
        }

        if (Jiffies elapsed = Since(election_reset_event_);
            elapsed >= timeout_duration) {
          if (IsPreVoteSucceed()) {
            BecomeCandidate();
            return;
          }
        }
      }
    });
  }

  void RunRequestVote(std::string peer, size_t term) {
    size_t last_log_index = log_.Length();
    size_t last_log_term = log_.LastLogTerm();
    RequestVote::Request request{term, *voted_for_, last_log_index,
                                 last_log_term};

    await::fibers::Go([this, peer, term, request]() {
      auto response =
          await::fibers::Await(commute::rpc::Call("Raft.RequestVote")
                                   .Args(request)
                                   .Via(Channel(peer))
                                   .AtMostOnce()
                                   .Start()
                                   .As<RequestVote::Response>());

      if (response.IsOk()) {
        auto guard = mutex_.Guard();

        if (state_ != NodeState::Candidate) {
          LOG_INFO("while waiting for reply, state = {}", state_);
          return;
        }

        if (response->term > term_) {
          LOG_INFO("term out of date in RequestVoteReply");
          BecomeFollower(response->term);
        } else if (response->term == term) {
          if (response->vote_granted) {
            votes_received_.insert(peer);
            if (votes_received_.size() >= Majority()) {
              LOG_INFO("Wins election with {} votes", votes_received_.size());
              BecomeLeader();
            }
          }
        }
      }
    });
  }

  void RunAppendEntries(std::string peer, size_t term) {
    size_t ni = next_index_[peer];
    size_t prev_log_index = ni - 1;
    size_t prev_log_term = prev_log_index > 0 ? log_.Term(prev_log_index) : 0;
    LogEntries entries;
    for (size_t i = ni; i <= log_.Length(); ++i) {
      entries.push_back(log_.Read(i));
    }
    raft::proto::AppendEntries::Request request{
        term,    node::rt::HostName(), prev_log_index, prev_log_term,
        entries, commit_index_};

    await::fibers::Go([this, peer, term, ni, request]() {
      auto response =
          await::fibers::Await(commute::rpc::Call("Raft.AppendEntries")
                                   .Args(request)
                                   .Via(Channel(peer))
                                   .AtMostOnce()
                                   .Start()
                                   .As<raft::proto::AppendEntries::Response>());

      if (response.IsOk()) {
        auto guard = mutex_.Guard();

        if (response->term > term_) {
          LOG_INFO("Term out of date in heartbeat reply");
          BecomeFollower(response->term);
        } else if (state_ == NodeState::Leader && term == response->term) {
          if (response->success) {
            next_index_[peer] = ni + request.entries.size();
            match_index_[peer] = next_index_[peer] - 1;

            LOG_INFO(
                "AppendEntries reply from {} success: next_index = {}, "
                "match_index = {}",
                peer, next_index_[peer], match_index_[peer]);

            size_t saved_commit_index = commit_index_;
            for (size_t i = commit_index_ + 1; i <= log_.Length(); ++i) {
              if (log_.Term(i) == term_) {
                size_t match_count = 1;
                for (const auto& [_, match_index] : match_index_) {
                  if (match_index >= i) {
                    ++match_count;
                  }
                }
                if (match_count >= Majority()) {
                  commit_index_ = i;
                }
              }
            }

            if (commit_index_ != saved_commit_index) {
              LOG_INFO("Leader sets commit index {}", commit_index_);
              new_commits_.TrySend({});
              submit_trigger_.TrySend({});
            }
          } else {
            next_index_[peer] = response->conflict_index;

            if (response->conflict_term > 0) {
              for (size_t i = log_.Length(); i > 0; --i) {
                if (log_.Term(i) == response->conflict_term) {
                  next_index_[peer] = i + 1;
                  break;
                }
              }
            }

            LOG_INFO("AppendEntries reply from {} !success: nextIndex = {}",
                     peer, next_index_[peer]);
          }
        }
      }
    });
  }

  void Replicate(size_t /*term*/) {
    // Your code goes here
  }

 private:
  Jiffies ElectionTimeout() const {
    uint64_t rtt = node::rt::Config()->GetInt<uint64_t>("net.rtt");
    return node::rt::RandomNumber(rtt, 5 * rtt);
  }

  Jiffies Since(Jiffies time) const {
    return node::rt::MonotonicNow() - time;
  }

  size_t Majority() const {
    return NodeCount() / 2 + 1;
  }

 private:
  await::fibers::Mutex mutex_;

  ExactlyOnceApplier state_machine_;

  Log log_;

  Jiffies election_reset_event_{0_jfs};
  // size_t votes_received_{0}; // TODO: ???
  std::set<std::string> votes_received_;

  await::fibers::Channel<wheels::Unit> new_commits_{1};
  await::fibers::Channel<wheels::Unit> submit_trigger_{1};

  size_t term_{0};
  NodeState state_{NodeState::Follower};

  std::optional<std::string> leader_;
  std::optional<std::string> voted_for_;

  // Peer -> next index id
  std::map<std::string, size_t> next_index_;
  // Peer -> match index id
  std::map<std::string, size_t> match_index_;

  size_t commit_index_{0};
  size_t last_applied_{0};

  std::map<RequestId, await::futures::Promise<proto::Response>> promises_;

  node::store::KVStore<State> store_;

  timber::Logger logger_;
};

//////////////////////////////////////////////////////////////////////

IReplicaPtr MakeRaftReplica(IStateMachinePtr state_machine,
                            commute::rpc::IServer* server) {
  auto store_dir =
      node::rt::Fs()->MakePath(node::rt::Config()->GetString("rsm.store.dir"));

  auto replica =
      std::make_shared<Raft>(std::move(state_machine), std::move(store_dir));
  replica->Start(server);
  return replica;
}

}  // namespace rsm
