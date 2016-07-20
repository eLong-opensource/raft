/*
 * transporter.h
 *
 *  Created on: Dec 22, 2013
 *      Author: fan
 */

#ifndef RAFT_CORE_TRANSPORTER_H_
#define RAFT_CORE_TRANSPORTER_H_

#include <raft/core/message.h>

namespace raft {

class Transporter {
 public:
  virtual ~Transporter()
  {
  }
  virtual bool SendRequestVote(const std::string& peer,
                               RequestVoteRequestPtr req, VoteChannelPtr repch) = 0;
  virtual bool SendAppendEntry(const std::string& peer,
                               AppendEntryRequestPtr req, AppendChannelPtr repch) = 0;
  virtual bool SendQueryLeader(const std::string& peer,
                               QueryLeaderRequestPtr req, QueryChannelPtr repch) = 0;
};

} /* namespace raft */
#endif /* RAFT_CORE_TRANSPORTER_H_ */
