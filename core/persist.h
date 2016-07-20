/*
 * logfile.h
 *
 *  Created on: Dec 28, 2013
 *      Author: fan
 */

#ifndef RAFT_CORE_PERSIST_H_
#define RAFT_CORE_PERSIST_H_

#include <boost/function.hpp>
#include <muduo/base/Mutex.h>
#include <raft/core/logentry.h>
#include <raft/core/message.h>
#include <raft/core/options.h>

namespace raft {

const uint64_t kNpos = 0xFFFFFFFFFFFFFFFF;

typedef std::deque<LogEntryPtr> LogEntryList;

class LogFile {
 public:
  explicit LogFile(const std::string& indexName);
  explicit LogFile(uint64_t startIndex);
  bool operator > (const LogFile& rhs) const;
  std::string IndexName;
  std::string LogName;
  uint64_t StartIndex;
};


class LogPersist {
 public:

  typedef std::deque<LogFile> FileList;
  typedef boost::function<bool (const proto::LogEntry&)> WalkCallback;

  LogPersist(Options* options);
  ~LogPersist();

  void Init();

  void Append(const proto::LogEntry& entry);

  // truncate start with index
  void Trunc(uint64_t index);

  // walk through [start, end)
  bool Walk(WalkCallback callback, uint64_t start, uint64_t end);

  // create a new log file with startIndex == index
  void RollFile(uint64_t index);

  // 最早的index，在所有的log file中
  uint64_t StartIndex();

  uint64_t NextIndex();

  bool GetTerm(uint64_t index, uint64_t* term);

  bool GetEntry(uint64_t index, proto::LogEntry* entry);

 private:
  void scanLogDir(FileList* list);
  bool isValidIndexFile(const std::string& name);
  int openFile(const std::string& name);

  // 获取指定index的logentry在日志文件中的位置
  uint32_t getLogOffset(int indexFile, uint64_t startIndex, uint64_t index);
  uint32_t getIndexOffset(uint64_t startIndex, uint64_t index);

  void repair();

  bool entryFunc(proto::LogEntry* toReturn, const proto::LogEntry& entry);

  Options* options_;
  int currentLogFile_;
  int currentIndexFile_;
  // [startIndex_, nextIndex_)
  uint64_t startIndex_;
  uint64_t nextIndex_;

  FileList logFiles_;
  uint64_t currentLogSize_;
  bool inited_;
  muduo::MutexLock mutex_;
};

class MetaPersist {
 public:
  MetaPersist(const std::string& logdir);
  ~MetaPersist();
  void Load(uint64_t* term, int32_t* voteFor, uint64_t* commitIndex);
  void UpdateTerm(uint64_t term);
  void UpdateVoteFor(int32_t voteFor);
  void UpdateCommitIndex(uint64_t commitIndex);
  void Dump();

 private:
  int fd_;
  std::string logdir_;
  uint64_t term_;
  int32_t voteFor_;
  uint64_t commitIndex_;
};

namespace fs {

class CloseGuard : boost::noncopyable
{
 public:
  CloseGuard(int fd) {fd_ = fd;}
  ~CloseGuard() {::close(fd_);}
 private:
  int fd_;
};

std::string Join(const std::string& p1, const std::string& p2);
int Writev(int fd, struct iovec* iov, int iovcnt);
int Write(int fd, void* buff, size_t len);
int Read(int fd, void* buff, size_t len);
int Pread(int fd, void* buff, size_t len, off_t offset);
int Rename(const std::string& oldpath, const std::string& newpath);
int Remove(const std::string& path);
off_t Size(const std::string& path);
bool Exists(const std::string& path);
}

} /* namespace raft */
#endif /* RAFT_CORE_LOGFILE_H_ */
