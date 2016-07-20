/*
 * log.cc
 *
 *  Created on: Dec 24, 2013
 *      Author: fan
 */

#include <boost/bind.hpp>
#include <raft/core/log.h>

namespace raft {

Log::Log(Options* options)
    : options_(options),
      entries_(),
      startIndex_(0),
      nextIndex_(0),
      persist_(options),
      mutex_()
{
}

void Log::Load()
{
  persist_.Init();
  startIndex_ = 0;
  nextIndex_ = persist_.NextIndex();
  if (nextIndex_ == 0) {
    // 这里预插入一条数据，这样大家都有一条相同的日志，方便第一次同步
    LogEntryPtr initEntry(new LogEntry);
    Append(initEntry);
  } else {
    //在内存日志列表插入最后一条日志 方便获取startIndex和nextIndex
    proto::LogEntry protoEntry;
    LogEntryPtr lastEntry(new LogEntry);
    CHECK(persist_.GetEntry(nextIndex_ - 1, &protoEntry));
    CHECK(protoEntry.index() == nextIndex_ - 1);
    lastEntry->CopyFrom(protoEntry);
    startIndex_ = nextIndex_ - 1;
    entries_.push_back(lastEntry);
  }

  LOG(INFO) << "Log Init: startIndex:" << startIndex_ << " nextIndex:" << nextIndex_;
}

bool Log::IsAccepted(uint64_t index, uint64_t term)
{
  if (index < startIndex_) {
    LOG(WARNING) << "IsAccept hits persist";
    uint64_t t;
    if (!persist_.GetTerm(index, &t)) {
      LOG(ERROR) << "IsAccept hits log bottom. No more log files";
      return false;
    }
    return t == term;
  }

  if (index >= nextIndex_) {
    return false;
  }

  uint64_t i = index - startIndex_;
  return entries_[i]->GetTerm() == term;
}

uint64_t Log::Trunc(uint64_t index)
{
  muduo::MutexLockGuard guard(mutex_);
  if (index >= nextIndex_) {
    return 0;
  }

  uint64_t reserve = 0;
  if (index < startIndex_) {
    LOG(INFO) << "Truncate clear memory.";
    reserve = 0;
    startIndex_ = index;
  } else {
    reserve = index - startIndex_;
  }

  entries_.resize(reserve);
  persist_.Trunc(index);

  uint64_t truncated = nextIndex_ - index;
  LOG(INFO) << "truncate " << truncated << " logs.";
  nextIndex_ = index;

  return truncated;
}

void Log::Append(const AppendEntryRequest& req)
{
  muduo::MutexLockGuard guard(mutex_);
  for (int i=0; i<req.entries_size(); i++) {
    persist_.Append(req.entries(i));
    LogEntryPtr entry(new LogEntry(req.entries(i)));
    appendWithoutLock(entry);
  }
}

void Log::Append(const LogEntryPtr& entry)
{
  persist_.Append(entry->GetProto());
  muduo::MutexLockGuard guard(mutex_);
  appendWithoutLock(entry);
}

void Log::appendWithoutLock(const LogEntryPtr& entry)
{
  uint64_t entryIndex = entry->GetIndex();
  CHECK(nextIndex_ == entryIndex);
  if (entries_.size() >= options_->MemoryLogSize) {
    startIndex_ += entries_.size() / 2;
    entries_.erase(entries_.begin(), entries_.begin() + entries_.size() / 2);
    LOG(INFO) << "Shrink memory log entries. New index:" << startIndex_;
  }
  entries_.push_back(entry);
  nextIndex_++;
}

bool Log::GetTerm(uint64_t index, uint64_t* term)
{
  muduo::MutexLockGuard guard(mutex_);
  if (index < startIndex_) {
    LOG(WARNING) << "GetTerm hits persist.";
    return persist_.GetTerm(index, term);
  }
  uint64_t i = index - startIndex_;
  //assert(i < entries_.size());
  if (i >= entries_.size()) {
    return false;
  }
  *term = entries_[i]->GetTerm();
  return true;
}

// used by master in vote
uint64_t Log::LastIndex()
{
  muduo::MutexLockGuard guard(mutex_);
  CHECK(nextIndex_ > 0);
  return nextIndex_ - 1;
}

// used by master in vote
uint64_t Log::LastTerm()
{
  muduo::MutexLockGuard guard(mutex_);
  if (entries_.size() > 0) {
    return entries_.back()->GetTerm();
  }
  uint64_t term = 0;
  persist_.GetTerm(LastIndex(), &term);
  return term;
}

bool Log::Walk(WalkCallback callback, uint64_t start, uint64_t end)
{
  uint64_t startIndex = 0;
  {
    muduo::MutexLockGuard guard(mutex_);
    startIndex = startIndex_;
  }
  if (start < startIndex) {
    //LOG(WARNING) << "Log Walk hits persist log.";
    return persist_.Walk(callback, start, end);
  }

  LogEntryList list;
  uint64_t i = 0;
  {
    muduo::MutexLockGuard guard(mutex_);
    i = start - startIndex_;
    uint64_t j = end - startIndex_;
    if (j >= entries_.size()) {
      list.assign(entries_.begin() + i, entries_.end());
    } else {
      list.assign(entries_.begin() + i, entries_.begin() + j);
    }
  }

  for (i=0; i<list.size(); i++) {
    if (!callback(list[i]->GetProto())) {
      return true;
    }
  }
  return true;
}

bool loadEntriesFunc(LogEntryList* list, const proto::LogEntry& entry)
{
  LogEntryPtr newEntry(new LogEntry(entry));
  list->push_back(newEntry);
  return true;
}

} /* namespace raft */
