#pragma once

#include <rsm/client/command.hpp>

#include <muesli/bytes.hpp>

#include <string>
#include <map>
#include <memory>

namespace rsm {

// NOT thread-safe!

struct IStateMachine {
  virtual ~IStateMachine() = default;

  // Move state machine to initial state
  virtual void Reset() = 0;

  // Applies command
  // Returns serialized operation response
  virtual muesli::Bytes Apply(Command command) = 0;

  // Snapshots

  virtual muesli::Bytes MakeSnapshot() = 0;
  virtual void InstallSnapshot(muesli::Bytes snapshot) = 0;
};

using IStateMachinePtr = std::shared_ptr<IStateMachine>;

class ExactlyOnceApplier {
 public:
  explicit ExactlyOnceApplier(IStateMachinePtr state_machine)
      : state_machine_(state_machine) {
  }

  void Reset() {
    state_machine_->Reset();
  };

  muesli::Bytes Apply(Command command) {
    if (!last_applied_.contains(command.request_id.client_id) ||
        last_applied_[command.request_id.client_id] <
            command.request_id.index) {
      last_applied_[command.request_id.client_id] = command.request_id.index;
      last_response_[command.request_id.client_id] =
          state_machine_->Apply(command);
    }
    return last_response_[command.request_id.client_id];
  }

 private:
  IStateMachinePtr state_machine_;

  std::map<std::string, muesli::Bytes> last_response_;
  std::map<std::string, size_t> last_applied_;
};

}  // namespace rsm
