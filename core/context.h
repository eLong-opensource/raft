/*
 * context.h
 *
 *  Created on: Jan 20, 2014
 *      Author: fan
 */

#ifndef RAFT_CORE_LEADERCONTEXT_H_
#define RAFT_CORE_LEADERCONTEXT_H_

#include <boost/shared_ptr.hpp>
#include <raft/core/message.h>

namespace raft {

class ServerContext
{
 public:
  uint64_t Term;
  uint64_t LastLogIndex;
  AppendChannelPtr Appendch;
  DisconnectChannelPtr Disconnectch;

  ServerContext(uint64_t term, uint64_t lastLogIndex,
                const AppendChannelPtr& appendch, const DisconnectChannelPtr& disconnectch)
      : Term(term),
        LastLogIndex(lastLogIndex),
        Appendch(appendch),
        Disconnectch(disconnectch)
  {
  }
};

}


#endif /* RAFT_CORE_LEADERCONTEXT_H_ */
