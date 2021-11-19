#pragma once

#include <rsm/client/command.hpp>
#include <rsm/replica/paxos/roles/acceptor_state.hpp>

#include <muesli/serializable.hpp>

namespace rsm {

using LogEntry = paxos::AcceptorState;

}  // namespace rsm
