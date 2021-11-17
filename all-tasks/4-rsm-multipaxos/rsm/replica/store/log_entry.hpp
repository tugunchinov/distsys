#pragma once

#include <rsm/client/command.hpp>
#include <rsm/replica/paxos/roles/acceptor_state.hpp>

#include <muesli/serializable.hpp>

namespace rsm {

struct LogEntry {
  Command command;

  // Make empty log entry
  static LogEntry Empty() {
    return {};
  }

  MUESLI_SERIALIZABLE(command);
};

}  // namespace rsm
