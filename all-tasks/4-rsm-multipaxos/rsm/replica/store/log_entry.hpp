#pragma once

#include <rsm/client/command.hpp>
#include <rsm/replica/paxos/roles/acceptor.hpp>

#include <muesli/serializable.hpp>

namespace rsm {

struct LogEntry {
  Command command{};
  bool committed{false};
  bool applied{false};

  // Make empty log entry
  static LogEntry Empty() {
    return {};
  }

  MUESLI_SERIALIZABLE(command, committed, applied)
};

}  // namespace rsm
