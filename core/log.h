/*
 * log.h
 *
 *  Created on: Dec 24, 2013
 *      Author: fan
 */

#ifndef RAFT_CORE_LOG_H_
#define RAFT_CORE_LOG_H_

#include <deque>
#include <boost/shared_ptr.hpp>
#include <muduo/base/Mutex.h>
#include <raft/core/message.h>
#include <raft/core/logentry.h>
#include <raft/core/persist.h>
#include <raft/core/options.h>

namespace raft {

class Log {
 public:

  typedef std::deque<LogEntryPtr> LogEntryList;
  typedef LogPersist::WalkCallback WalkCallback;

  Log(Options* options);

  // used by master
  void Load();

  // used by master
  bool IsAccepted(uint64_t index, uint64_t term);

  // used by master
  // [index:]
  uint64_t Trunc(uint64_t index);

  // used by master
  void Append(const AppendEntryRequest& entries);
  void Append(const LogEntryPtr& entry);
  void appendWithoutLock(const LogEntryPtr& entry);

  bool GetTerm(uint64_t index, uint64_t* term);
  uint64_t LastIndex();
  uint64_t LastTerm();

  // walk throught [start, end)
  bool Walk(WalkCallback callback, uint64_t start, uint64_t end);

 private:
  Options* options_;
  LogEntryList entries_;
  // [startIndex_, nextIndex_)
  uint64_t startIndex_;
  uint64_t nextIndex_;
  LogPersist persist_;
  muduo::MutexLock mutex_;

};

bool loadEntriesFunc(LogEntryList* list, uint64_t* curr, uint64_t end, const proto::LogEntry& entry);

} /* namespace raft */
#endif /* RAFT_CORE_LOG_H_ */
