/*
 * raftlog.cc
 *
 *  Created on: Jan 2, 2014
 *      Author: fan
 */

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdio.h>
#include <dirent.h>

#include <vector>
#include <string>
#include <algorithm>
#include <iostream>
#include <sstream>

#include <boost/bind.hpp>

#include <muduo/base/Thread.h>
#include <gflags/gflags.h>
#include <raft/core/raft.pb.h>
#include <raft/core/persist.h>
#include <raft/core/options.h>
#include <raft/base/channel.h>

DEFINE_string(plugin, "", "plugin to decode log body");

void usage()
{
  std::cout << "usage:" << std::endl;
  std::cout << "raftlog check dir" << std::endl;
  std::cout << "raftlog peek dir index | start end" << std::endl;
  std::cout << "raftlog compare dir1 dir2" << std::endl;
}

bool checkfunc(uint64_t* current, const raft::proto::LogEntry& entry)
{
  if (*current != entry.index()) {
    std::cout << "Expect " << *current << " got " << entry.index() << std::endl;
  }
  *current += 1;
  return true;
}

void check(const std::string& dir)
{
  raft::Options options;
  options.RaftLogDir = dir;
  raft::LogPersist persist(&options);
  persist.Init();

  uint64_t current = persist.StartIndex();
  if (persist.Walk(boost::bind(&checkfunc, &current, _1), persist.StartIndex(), raft::kNpos)) {
    std::cout << "check done. no error." << std::endl;
    std::cout << "total " << current << " logs" << std::endl;
  } else {
    std::cout << "check error." << std::endl;
  }
}

typedef raft::Channel<raft::LogEntryPtr> LogEntryChannel;

bool compareSender(LogEntryChannel* ch, const raft::proto::LogEntry& entry)
{
  raft::LogEntryPtr newEntry(new raft::LogEntry(entry));
  ch->Put(newEntry);
  return true;
}

bool compareReceiver(LogEntryChannel* ch, const raft::proto::LogEntry& entry)
{
  raft::LogEntryPtr newEntry;
  ch->Take(&newEntry);
  const raft::proto::LogEntry& tocompare = newEntry->GetProto();
  if (tocompare.SerializeAsString() != entry.SerializeAsString()) {
    std::cout << "not equal:" << std::endl;
    std::cout << newEntry->GetProto().Utf8DebugString();
    std::cout << entry.Utf8DebugString();
  }
  return true;
}

void compare(const std::string& dir, const std::string& dir1)
{
  LogEntryChannel ch;
  raft::Options options, options1;
  options.RaftLogDir = dir;
  options1.RaftLogDir = dir1;

  raft::LogPersist persist(&options);
  raft::LogPersist persist1(&options1);
  persist.Init();
  persist1.Init();

  uint64_t begin = std::max(persist.StartIndex(), persist1.StartIndex());
  uint64_t end = std::min(persist.NextIndex(), persist1.NextIndex());
  raft::LogPersist::WalkCallback sender(boost::bind(&compareSender, &ch, _1));
  raft::LogPersist::WalkCallback receiver(boost::bind(&compareReceiver, &ch, _1));

  muduo::Thread threadSender(boost::bind(&raft::LogPersist::Walk, &persist, sender, begin, end));
  muduo::Thread threadReceiver(boost::bind(&raft::LogPersist::Walk, &persist1, receiver, begin, end));
  std::cout << "comparing..." << std::endl;
  threadSender.start();
  threadReceiver.start();
  threadSender.join();
  threadReceiver.join();
  std::cout << "done." << std::endl;

}

bool peekFunc(int *fd, const raft::proto::LogEntry& entry)
{
  std::cout << "type: " << (entry.type() == raft::proto::NEW_LEADER ? "NEW_LEADER" : "LOG_ENTRY") << std::endl;
  std::cout << "index: " << entry.index() << std::endl;
  std::cout << "term: " << entry.term() << std::endl;

  std::string body(entry.body());
  size_t header = body.size();
  write(fd[0], &header, sizeof(header));
  write(fd[0], &body[0], header);

  read(fd[0], &header, sizeof(header));
  body.resize(header);
  read(fd[0], &body[0], header);
  std::cout << "body: >>> " << body << std::endl;
  std::cout << "<<<" << std::endl;
  return true;
}

void defaultDecoder()
{
  size_t header;
  std::string body;
  while (read(0, &header, sizeof(header)) > 0) {
    body.resize(header);
    read(0, &body[0], header);
    write(1, &header, sizeof(header));
    write(1, &body[0], header);
  }
}

void peek(const std::string& dir, uint64_t start, uint64_t end)
{
  raft::Options options;
  options.RaftLogDir = dir;
  raft::LogPersist persist(&options);
  persist.Init();

  int fd[2];
  ::socketpair(AF_UNIX, SOCK_STREAM, 0, fd);
  int pid = fork();
  if (pid == 0) {
    dup2(fd[1], 1);
    dup2(fd[1], 0);
    ::close(fd[0]);
    ::close(fd[1]);
    std::string plugin(FLAGS_plugin);
    if (!plugin.empty()) {
      if (execl(plugin.c_str(), plugin.c_str(), NULL) < 0) {
        perror("exec plugin");
        exit(-1);
      }
    } else {
      defaultDecoder();
      exit(0);
    }
  }
  persist.Walk(boost::bind(&peekFunc, fd, _1), start, end);
  ::close(fd[0]);
  ::close(fd[1]);
  int status;
  ::waitpid(pid, &status, 0);
}

int main(int argc, char* argv[])
{
  google::ParseCommandLineFlags(&argc, &argv, true);
  if (argc < 3) {
    usage();
    return 0;
  }

  std::string cmd(argv[1]);
  std::string dir(argv[2]);

  if (cmd == "check") {
    std::cout << "checking..." << std::endl;
    check(dir);
  } else if (cmd == "compare") {
    if (argc < 4) {
      std::cerr << "missing dir2" << std::endl;
      return -1;
    }
    compare(dir, argv[3]);
  } else if (cmd == "peek") {
    if (argc < 4) {
      std::cerr << "missing index" << std::endl;
      return -1;
    }
    uint64_t start = 0;
    uint64_t end = 0;
    std::stringstream ss(argv[3]);
    ss >> start;
    end = start + 1;

    if (argc == 5) {
      ss.str(argv[4]);
      ss.clear();
      ss >> end;
    }

    peek(dir, start, end);
  } else {
    std::cerr << "invalid command " << cmd << std::endl;
    usage();
  }

  return 0;
}
