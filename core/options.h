#ifndef RAFT_CORE_OPTIONS_H
#define RAFT_CORE_OPTIONS_H

#include <string>
#include <stdint.h>

namespace raft {

struct Options {
  // myid
  int MyID;

  // raft 存放回放日志的地方, default "raftlog"
  std::string RaftLogDir;

  // 等待选举回复的超时时间, in ms, 默认为1000
  int ElectionTimeout;

  // 用来指示leader隔多久发送心跳, in ms. 默认为50
  int HeartbeatTimeout;

  // 经过多少commited log产生snapshot, 默认为1000000
  uint64_t SnapshotLogSize;

  // 经过多长时间产生snapshot，单位为秒，默认一天
  uint64_t SnapshotInterval;

  // 是否每一个需要同步的日志立马向各个节点同步,如果为false,
  // 经过HeartbeatTimeout同步日志.
  // 默认为false
  bool ForceFlush;

  // 在一次raft loop中最多提交多少日志,太多的话可能会导致不能及时回复心跳
  // 默认为200条
  uint64_t MaxCommitSize;

  // 日志分割条数 default 1000000
  uint64_t LogRollSize;

  // 内存缓存日志条数, default 10000
  uint64_t MemoryLogSize;

  Options()
      : MyID(0),
        RaftLogDir("raftlog"),
        ElectionTimeout(1000),
        HeartbeatTimeout(50),
        SnapshotLogSize(1000000),
        SnapshotInterval(3600 * 24),
        ForceFlush(false),
        MaxCommitSize(200),
        LogRollSize(1000000),
        MemoryLogSize(10000)
  {
  }
};

};

#endif
