/*
 * logfile.cc
 *
 *  Created on: Dec 28, 2013
 *      Author: fan
 */

#include <ctype.h>
#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/uio.h>
#include <stdio.h>

#include <sstream>

#include <boost/bind.hpp>

#include <raft/core/persist.h>

namespace raft {

LogPersist::LogPersist(Options* options)
    : options_(options),
      currentLogFile_(0),
      currentIndexFile_(0),
      startIndex_(0),
      nextIndex_(0),
      logFiles_(),
      currentLogSize_(0),
      inited_(false),
      mutex_()
{
  LOG(INFO) << "log dir:" << options_->RaftLogDir;
  LOG(INFO) << "file roll size:" << options_->LogRollSize;
}

LogPersist::~LogPersist()
{
  if (currentLogFile_) {
    ::close(currentLogFile_);
  }
  if (currentIndexFile_) {
    ::close(currentIndexFile_);
  }
}

void LogPersist::Init()
{
  inited_ = true;
  scanLogDir(&logFiles_);
  if (logFiles_.empty()) {
    LOG(INFO) << "log dir empty. create new log file";
    RollFile(0);
    return;
  }

  repair();

  const LogFile& lastFile = logFiles_[0];
  currentLogFile_ = openFile(lastFile.LogName);
  currentIndexFile_ = openFile(lastFile.IndexName);

  off_t size = fs::Size(fs::Join(options_->RaftLogDir, lastFile.IndexName));

  startIndex_ = lastFile.StartIndex;
  nextIndex_ = startIndex_ + size / sizeof(uint32_t);
  currentLogSize_ = nextIndex_ - startIndex_;

  LOG(INFO) << "last log file: " << lastFile.LogName;
  LOG(INFO) << "last index file: " << lastFile.IndexName;
  LOG(INFO) << "index file size: " << size;
  LOG(INFO) << "start index:" << startIndex_;
  LOG(INFO) << "next index:" << nextIndex_;
}

bool LogPersist::Walk(WalkCallback callback, uint64_t start, uint64_t end)
{
  CHECK(inited_);
  FileList logFiles;
  {
    muduo::MutexLockGuard guard(mutex_);
    logFiles.assign(logFiles_.begin(), logFiles_.end());
  }

  FileList::iterator it;
  for (it=logFiles.begin(); it!=logFiles.end(); it++) {
    if (start >= it->StartIndex) {
      break;
    }
  }

  if (it == logFiles.end()) {
    LOG(ERROR) << "No more file to find an log file match index:" << start;
    return false;
  }

  // 倒序开始遍历文件
  uint64_t currentIndex = start;
  while (true) {
    // seek current file
    int indexFile = openFile(it->IndexName);
    int logFile = openFile(it->LogName);
    fs::CloseGuard indexGuard(indexFile);
    fs::CloseGuard LogGuard(logFile);

    CHECK(currentIndex>=it->StartIndex);
    uint64_t offset = getLogOffset(indexFile, it->StartIndex, currentIndex);
    PCHECK(::lseek(logFile, offset, SEEK_SET) >= 0) << "Seek offset: " << offset << " log name: " << it->LogName;

    // read current file
    bool reachEnd = false;
    while (!reachEnd) {
      if (currentIndex >= end) {
        return true;
      }
      uint32_t header;
      std::string data;
      int n = fs::Read(logFile, &header, sizeof(header));
      PCHECK(n >=0) << "read log header error.";
      if (n == 0) {
        reachEnd = true;
        break;
      }

      data.resize(header);
      n = fs::Read(logFile, &data[0], header);
      PCHECK(n >=0) << "read log body error.";

      proto::LogEntry protoEntry;
      PCHECK(protoEntry.ParseFromString(data)) << "parse log body error.";
      if (!callback(protoEntry)) {
        return true;
      }
      CHECK(protoEntry.index() == currentIndex);
      currentIndex++;
    } // End of read log file

    // Already read up to current log file
    if (it == logFiles.begin()) {
      return true;
    } else {
      // switch to next file
      it--;
    }
  }

  return true;
}

// not safe. Only called by master thread
void LogPersist::Append(const proto::LogEntry& entry)
{
  CHECK(inited_);

  if (currentLogSize_ >= options_->LogRollSize) {
    LOG(INFO) << "roll log file at index:" << entry.index();
    RollFile(entry.index());
  }

  CHECK(nextIndex_ == entry.index());

  off_t offset = ::lseek(currentLogFile_, 0, SEEK_END);
  PCHECK(offset >= 0) << "get log file offset.";

  uint32_t header = entry.ByteSize();
  std::string body;
  entry.SerializeToString(&body);
  CHECK(header == body.size());
  struct iovec iov[2] = {{&header, sizeof(header)}, {&body[0], body.size()}};
  int ret = fs::Writev(currentLogFile_, iov, 2);
  PCHECK(ret >=0 ) << "append entry error.";

  uint32_t offset32 = offset;
  ret = fs::Write(currentIndexFile_, &offset32, sizeof(offset32));
  PCHECK(ret >= 0) << "write log file index error.";

  currentLogSize_++;
  nextIndex_++;

}

void LogPersist::Trunc(uint64_t index)
{
  CHECK(inited_);
  // need not truncate
  if (index >= nextIndex_) {
    return;
  }

  // Find first log file whose startIndex is less than the truncated index
  FileList::iterator it;
  for (it=logFiles_.begin(); it!=logFiles_.end(); it++) {
    if (index >= it->StartIndex) {
      break;
    }
  }

  if (it == logFiles_.end()) {
    LOG(FATAL) << "No more file to find an log file match index:" << index;
  }

  // TODO may give an error
  if ((it - logFiles_.begin()) >= 2) {
    LOG(WARNING) << "trunc too much files:" << (it-logFiles_.begin());
  }

  if (it != logFiles_.begin()) {
    // switch new log and index file
    std::string currentLogName = it->LogName;
    std::string currentIndexName = it->IndexName;

    ::close(currentIndexFile_);
    ::close(currentLogFile_);
    currentLogFile_ = openFile(currentLogName);
    currentIndexFile_ = openFile(currentIndexName);
    startIndex_ = it->StartIndex;
  }


  // truncate current log and index file
  long offset = getIndexOffset(startIndex_, index);
  uint32_t pos = getLogOffset(currentIndexFile_, startIndex_, index);

  // delete the log and index file after index
  FileList::iterator delIt = logFiles_.begin();
  for (; delIt!=it; delIt++) {
    PCHECK(fs::Remove(fs::Join(options_->RaftLogDir, delIt->IndexName)) == 0) << "delete index file error.";
    PCHECK(fs::Remove(fs::Join(options_->RaftLogDir, delIt->LogName)) == 0) << "delete index file error.";
    LOG(INFO) << "Delete file " << delIt->LogName << " and " << delIt->IndexName << " for trunc.";
  }

  PCHECK(::ftruncate(currentIndexFile_, offset) == 0) << "truncate index file error.";

  // 程序在这里可能会被中断，恢复的时候用index文件判断日志是否被删除

  PCHECK(::ftruncate(currentLogFile_, pos) == 0) << "truncate log file error.";

  logFiles_.erase(logFiles_.begin(), it);
  nextIndex_ = index;
  currentLogSize_ = nextIndex_ - startIndex_;
}

void LogPersist::RollFile(uint64_t index)
{
  CHECK(inited_);
  CHECK(index == nextIndex_);

  if (currentIndexFile_ != 0) {
    ::close(currentLogFile_);
    ::close(currentIndexFile_);
  }

  LogFile logFile(index);
  currentIndexFile_ = openFile(logFile.IndexName);
  currentLogFile_ = openFile(logFile.LogName);

  startIndex_ = index;
  // lastIndex_ = startIndex_
  currentLogSize_ = 0;
  // insert into logFiles_, we need it later
  muduo::MutexLockGuard guard(mutex_);
  logFiles_.push_front(logFile);
}

uint64_t LogPersist::StartIndex()
{
  CHECK(inited_);
  return logFiles_.back().StartIndex;
}

uint64_t LogPersist::NextIndex()
{
  CHECK(inited_);
  return nextIndex_;
}

bool LogPersist::GetEntry(uint64_t index, proto::LogEntry* entry)
{
  CHECK(entry);
  if (index >= nextIndex_) {
    return false;
  }

  if (!Walk(boost::bind(&LogPersist::entryFunc, this, entry, _1), index, index + 1)) {
    return false;
  }
  return true;
}

bool LogPersist::GetTerm(uint64_t index, uint64_t* term)
{
  CHECK(term);
  proto::LogEntry entry;
  if (!GetEntry(index, &entry)) {
    return false;
  }
  *term = entry.term();
  return true;
}

void LogPersist::scanLogDir(FileList* list)
{
  DIR* dir = ::opendir(options_->RaftLogDir.c_str());
  PCHECK(dir != NULL) << "open log dir failed: " << options_->RaftLogDir;

  struct dirent* ent = NULL;
  struct stat buf;
  while ((ent = ::readdir(dir))) {
    std::string path = fs::Join(options_->RaftLogDir, ent->d_name);
    PCHECK(lstat(path.c_str(), &buf) == 0) << "lstat " << ent->d_name;
    // isValidIndexFile 不能使用path, 要使用文件名
    if (S_ISREG(buf.st_mode) && isValidIndexFile(ent->d_name)) {
      LOG(INFO) << "found index file" << path;
      list->push_back(LogFile(ent->d_name));
    }
  }
  std::sort(list->begin(), list->end(), std::greater<LogFile>());
  ::closedir(dir);
}

// 以index文件为准，truncate掉写失败的log文件
void LogPersist::repair()
{
  // 删除最后的空文件
  std::string indexName = fs::Join(options_->RaftLogDir,
                                   logFiles_[0].IndexName);
  std::string logName = fs::Join(options_->RaftLogDir,
                                 logFiles_[0].LogName);
  uint64_t startIndex = logFiles_[0].StartIndex;
  if (fs::Size(indexName) == 0) {
    fs::Remove(indexName);
    if (fs::Exists(logName)) {
      fs::Remove(logName);
    }
    LOG(INFO) << "Remove empty indexFile and logFile" << indexName << " - " << logName;
    logFiles_.pop_front();

    if (logFiles_.empty()) {
      return;
    }

    indexName = fs::Join(options_->RaftLogDir, logFiles_[1].IndexName);
    logName = fs::Join(options_->RaftLogDir, logFiles_[1].LogName);
    startIndex = logFiles_[1].StartIndex;
  }

  int indexFile = ::open(indexName.c_str(), O_RDWR|O_CLOEXEC, 0644);
  PCHECK(indexFile >= 0);
  int logFile = ::open(logName.c_str(), O_RDWR|O_CLOEXEC, 0644);

  // 删除没写完的index file内容
  off_t size = fs::Size(indexName);
  PCHECK(::ftruncate(indexFile, size / sizeof(uint32_t) * sizeof(uint32_t)) == 0);
  off_t endOffset = ::lseek(indexFile, 0, SEEK_END);
  PCHECK(endOffset >= 0);
  // Index file is empty
  if (endOffset == 0) {
    PCHECK(::ftruncate(logFile, 0) == 0);
    return;
  }

  // 删除没写完的log file内容
  CHECK(endOffset % 4 == 0);
  uint32_t lastEntryOffset = 0;
  PCHECK(fs::Pread(indexFile, &lastEntryOffset, sizeof(lastEntryOffset), endOffset - sizeof(lastEntryOffset)) >= 0);
  uint32_t header;
  PCHECK(fs::Pread(logFile, &header, sizeof(header), lastEntryOffset) >= 0);

  uint32_t truncatePoint = lastEntryOffset + sizeof(header) + header;
  size = fs::Size(logName);
  if (truncatePoint < size) {
    LOG(WARNING) << "Repair log file " << logName << ". Truncate " << (size - truncatePoint) << " bytes";
    PCHECK(::ftruncate(logFile, truncatePoint) == 0);
  }
}

bool LogPersist::isValidIndexFile(const std::string& name)
{
  std::string::const_iterator it = name.begin();
  while (it != name.end() && isdigit(*it)) {
    ++it;
  }
  return !name.empty() && it == name.end();
}

uint32_t LogPersist::getLogOffset(int indexFile, uint64_t startIndex, uint64_t index)
{
  long offset = getIndexOffset(startIndex, index);
  uint32_t pos = 0;
  PCHECK(fs::Pread(indexFile, &pos, sizeof(pos), offset) >= 0) << "read index file error.";
  return pos;
}

uint32_t LogPersist::getIndexOffset(uint64_t startIndex, uint64_t index)
{
  return (index - startIndex) * sizeof(uint32_t);
}


int LogPersist::openFile(const std::string& name)
{
  std::string fullname = fs::Join(options_->RaftLogDir, name);
  int fd = ::open(fullname.c_str(), O_CREAT|O_RDWR|O_APPEND|O_CLOEXEC, 0644);
  PCHECK(fd >= 0) << "Open error:" << fullname;
  return fd;
}

bool LogPersist::entryFunc(proto::LogEntry* toReturn, const proto::LogEntry& entry)
{
  CHECK(toReturn);
  toReturn->CopyFrom(entry);
  return false;
}

MetaPersist::MetaPersist(const std::string& logdir)
    : fd_(-1),
      logdir_(logdir),
      term_(0),
      voteFor_(0),
      commitIndex_(0)
{
}

MetaPersist::~MetaPersist()
{
  ::close(fd_);
}

void MetaPersist::Load(uint64_t* term, int32_t* voteFor, uint64_t* commitIndex)
{
  std::string fullname = fs::Join(logdir_, "meta");
  fd_ = ::open(fullname.c_str(), O_CREAT|O_RDWR|O_CLOEXEC, 0644);
  PCHECK(fd_ >= 0) << "Open error:" << fullname;

  uint64_t buf[3];
  ::bzero(buf, sizeof(buf));
  PCHECK(fs::Read(fd_, buf, sizeof(buf)) >= 0) << "read meta error.";

  term_ = buf[0];
  voteFor_ = static_cast<int32_t>(buf[1]);
  commitIndex_ = buf[2];

  *term = term_;
  *voteFor = voteFor_;
  *commitIndex = commitIndex_;

  LOG(INFO) << "Load meta: term:" << term_ << " voteFor: " << voteFor_ << " commitIndex:" << commitIndex_;
}

void MetaPersist::Dump()
{
  uint64_t buf[3] = {term_, voteFor_, commitIndex_};
  PCHECK(lseek(fd_, 0, SEEK_SET) >= 0) << "seek meta file error.";
  PCHECK(fs::Write(fd_, buf, sizeof(buf)) >= 0) << "write meta file error.";
}

void MetaPersist::UpdateTerm(uint64_t term)
{
  term_ = term;
  Dump();
}

void MetaPersist::UpdateVoteFor(int32_t voteFor)
{
  voteFor_ = voteFor;
  Dump();
}

void MetaPersist::UpdateCommitIndex(uint64_t commitIndex)
{
  commitIndex_ = commitIndex;
  Dump();
}


/* LogFile */

LogFile::LogFile(uint64_t startIndex)
    : StartIndex(startIndex)
{
  char buf[32];
  sprintf(buf, "%020"PRIu64, startIndex);
  IndexName.assign(buf);
  LogName = IndexName + ".log";
}

LogFile::LogFile(const std::string& indexName)
    : IndexName(indexName),
      LogName(indexName + ".log"),
      StartIndex(0)
{
  std::stringstream convert(indexName);
  convert >> StartIndex;
}

bool LogFile::operator > (const LogFile& rhs) const
{
  return StartIndex > rhs.StartIndex;
}

/* end of LogFile */
namespace fs {

std::string Join(const std::string& p1, const std::string& p2)
{
  return p1 + "/" + p2;
}

int Writev(int fd, struct iovec* iov, int iovcnt)
{
  size_t total = 0;
  for (int i=0; i<iovcnt; i++) {
    total += iov[i].iov_len;
  }

  size_t remaining = total;
  while (true) {
    int writen = ::writev(fd, iov, iovcnt);
    if (writen < 0) {
      if (errno == EINTR) {
        continue;
      } else {
        return -1;
      }
    }
    remaining -= writen;
    if (remaining == 0) {
      return total;
    }

    for (int i=0; i<iovcnt; i++) {
      if (iov[i].iov_len < static_cast<size_t>(writen)) {
        writen -= iov[i].iov_len;
        iov[i].iov_len = 0;
      } else {
        iov[i].iov_base = static_cast<char*>(iov[i].iov_base) + writen;
        iov[i].iov_len -= writen;
        break;
      }
    }
  }
  return total;
}

int Write(int fd, void* buff, size_t len)
{
  struct iovec iov[1] = {{buff, len}};
  return Writev(fd, iov, 1);
}

int Read(int fd, void* buff, size_t len)
{
  int n = 0;
  size_t remain = len;
  while (true) {
    n = ::read(fd, buff, remain);
    if (n < 0) {
      return -1;
    }

    if (n == 0) {
      return len - remain;
    }

    remain -= n;
    if (remain == 0) {
      return len;
    }

    buff = static_cast<char*>(buff) + n;
  }
  return 0;
}

int Pread(int fd, void* buff, size_t len, off_t offset)
{
  int n = 0;
  size_t remain = len;
  off_t curr = offset;
  while (true) {
    n = ::pread(fd, buff, remain, curr);
    if (n < 0) {
      return -1;
    }

    if (n == 0) {
      return len - remain;
    }

    remain -= n;
    if (remain == 0) {
      return len;
    }

    curr += n;
    buff = static_cast<char*>(buff) + n;
  }
  return 0;
}

int Rename(const std::string& oldpath, const std::string& newpath)
{
  return ::rename(oldpath.c_str(), newpath.c_str());
}

int Remove(const std::string& path)
{
  return ::unlink(path.c_str());
}

off_t Size(const std::string& path)
{
  struct stat buf;
  if (::stat(path.c_str(), &buf) < 0) {
    return -1;
  }
  return buf.st_size;
}

bool Exists(const std::string& path)
{
  return ::access(path.c_str(), F_OK) == 0;
}

} /* namespace fs */

}

/* namespace raft */
