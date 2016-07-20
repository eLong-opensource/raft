#ifndef RAFT_CORE_RAFT_H
#define RAFT_CORE_RAFT_H

#include <stdint.h>
#include <string>
#include <boost/function.hpp>

namespace raft {

enum ServerState {
    LEADER, FOLLOWER, CANDIDATE, STOP
};

typedef boost::function<void (uint64_t, const std::string&)> SyncedCallback;
typedef boost::function<void (ServerState)> StateChangedCallback;
typedef boost::function<void ()> TakeSnapshotDoneCallback;
typedef boost::function<void (uint64_t, const TakeSnapshotDoneCallback&)> TakeSnapshotCallback;
// 返回值作为commitindex的参考值，如果非0则使用这个值作为当前的commitindex
typedef boost::function<uint64_t (uint64_t)> LoadSnapshotCallback;

class Raft
{
 public:
  virtual ~Raft() {};

  virtual int LeaderId() = 0;
  virtual int Broadcast(const std::string& data) = 0;
  virtual void SetSyncedCallback(const SyncedCallback& cb) = 0;
  virtual void SetStateChangedCallback(const StateChangedCallback& cb) = 0;
  virtual void SetTakeSnapshotCallback(const TakeSnapshotCallback& cb) = 0;
  virtual void SetLoadSnapshotCallback(const LoadSnapshotCallback& cb) = 0;

};

}

#endif
