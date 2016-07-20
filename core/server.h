/*
 * server.h
 *
 *  Created on: Dec 12, 2013
 *      Author: fan
 */

#ifndef RAFT_CORE_SERVER_H_
#define RAFT_CORE_SERVER_H_

#include <set>
#include <boost/function.hpp>
#include <boost/noncopyable.hpp>
#include <muduo/base/Thread.h>
#include <toft/base/closure.h>
#include <toft/base/random.h>
#include <toft/system/atomic/atomic.h>

#include <raft/core/event.h>
#include <raft/core/transporter.h>
#include <raft/core/peer.h>
#include <raft/core/log.h>
#include <raft/core/command.h>
#include <raft/core/raft.h>
#include <raft/core/options.h>

namespace raft {

typedef boost::function<void (int)> LeaderChangedCallback;

void defaultSyncedCallback(uint64_t index, const std::string& data);
void defaultStateChangedCallback(ServerState state);
void defaultLeaderChangedCallback(int leaderId);
void defaultTakeSnapshotCallback(int index, const TakeSnapshotDoneCallback& done);
uint64_t defaultLoadSnapshotCallback(int index);

const int kCommandChannelSize = 2048;

class Server: public boost::noncopyable, public Raft {
 public:

  typedef std::vector<uint64_t> LogIndexList;
  typedef std::set<std::string> DisconnectedPeers;

  Server(const Options& options, Transporter* trans);
  ~Server();

  // run in background
  void Start();

  // run forever
  void Run();

  EventChannelPtr RpcEventChannel() { return ch_; }

  uint64_t Term() { return currentTerm_.Value(); }

  int MyId() { return options_.MyID; }

  uint64_t CommitIndex() { return commitIndex_.Value(); }

  int LeaderId() { return leaderId_.Value(); }

  Log& GetLog() { return log_; }

  void SetSyncedCallback(const SyncedCallback& cb) { syncedCallback_ = cb; }
  void SetStateChangedCallback(const StateChangedCallback& cb) { stateChangedCallback_ = cb; }
  void SetLeaderChangedCallback(const LeaderChangedCallback& cb) { leaderChangedCallback_ = cb; }
  void SetTakeSnapshotCallback(const TakeSnapshotCallback& cb) { takeSnapshotCallback_ = cb; }
  void SetLoadSnapshotCallback(const LoadSnapshotCallback& cb) { loadSnapshotCallback_ = cb; }

  void AddPeer(const std::string& addr);

  int Broadcast(const std::string& data);

 private:
  EventPtr get();

  void mainLoop();
  void followerLoop();
  void candidateLoop();
  void leaderLoop();

  // @ret whether to update timeout
  bool handleEvent(EventPtr event);
  bool handleRequestVote(const EventPtr& event);
  bool handleAppendEntry(const EventPtr& event);
  void handleQueryLeader(const EventPtr& event);
  void handleAppendEntryReply(AppendEntryReply* rep, LogIndexList* list);
  void handleCommand(const CommandPtr& req, LogIndexList* list, AppendChannelPtr appendch);
  void handleDisconnect(const std::string& req, DisconnectedPeers* peers);

  bool commitLogEntry(const proto::LogEntry& entry);

  void stepDown(uint64_t term);

  void sendVotes(VoteChannelPtr votech);

  void sendQuery(QueryChannelPtr querych);

  size_t quoramSize();
  uint64_t quoramIndex(const LogIndexList& list);

  void stopPeers();

  void broadCastNewLeader();

  void setTerm(uint64_t term);
  void setVoteFor(uint32_t voteFor);
  void setCommitIndex(uint64_t index);

  void flushPeers();

  void takeSnapshotDone(uint64_t index);

  int randomBetween(int begin, int end);

  Options options_;
  toft::Atomic<uint32_t> leaderId_;
  toft::Atomic<uint64_t> currentTerm_;
  toft::Atomic<uint64_t> commitIndex_;
  Transporter* trans_;
  EventChannelPtr ch_;
  CommandChannelPtr commandch_;
  ServerState state_;
  uint64_t commitSize_;
  toft::Random random_;
  int32_t voteFor_;
  std::vector<PeerPtr> peers_;
  muduo::MutexLock mutex_;
  Log log_;
  MetaPersist meta_;
  // 是否是单例模式,在这个模式下不用同步日志
  bool single_;

  // 最后一次生成快照，启动的时候置为当前时间
  uint64_t lastSnapshotTime_;

  SyncedCallback syncedCallback_;
  StateChangedCallback stateChangedCallback_;
  LeaderChangedCallback leaderChangedCallback_;
  TakeSnapshotCallback takeSnapshotCallback_;
  LoadSnapshotCallback loadSnapshotCallback_;
};

} /* namespace raft */
#endif /* RAFT_CORE_SERVER_H_ */
