/*
 * logentry.h
 *
 *  Created on: Dec 24, 2013
 *      Author: fan
 */

#ifndef RAFT_CORE_LOGENTRY_H_
#define RAFT_CORE_LOGENTRY_H_

#include <boost/shared_ptr.hpp>
#include <raft/core/raft.pb.h>

namespace raft {

using proto::EntryType;
using proto::LOG_ENTRY;
using proto::NEW_LEADER;

class LogEntry {
 public:

  LogEntry(proto::EntryType type, uint64_t term, uint64_t index, const std::string& body)
      : proto_()
  {
    proto_.set_type(type);
    proto_.set_term(term);
    proto_.set_index(index);
    proto_.set_body(body);
  }

  LogEntry(const proto::LogEntry& entry)
      : proto_(entry)
  {

  }

  LogEntry()
      : proto_()
  {
    proto_.set_type(LOG_ENTRY);
    proto_.set_term(0);
    proto_.set_index(0);
    proto_.set_body("");
  }

  EntryType GetType()
  {
    return proto_.type();
  }

  uint64_t GetTerm()
  {
    return proto_.term();
  }

  uint64_t GetIndex()
  {
    return proto_.index();
  }

  const std::string& GetBody()
  {
    return proto_.body();
  }

  const proto::LogEntry& GetProto()
  {
    return proto_;
  }

  void CopyFrom(const proto::LogEntry& entry)
  {
    proto_.CopyFrom(entry);
  }

 private:
  proto::LogEntry proto_;
};

typedef boost::shared_ptr<LogEntry> LogEntryPtr;

}
#endif /* RAFT_CORE_LOGENTRY_H_ */
