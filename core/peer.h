/*
 * peer.h
 *
 *  Created on: Dec 22, 2013
 *      Author: fan
 */

#ifndef RAFT_CORE_PEER_H_
#define RAFT_CORE_PEER_H_

#include <string>
#include <muduo/base/Mutex.h>

#include <raft/core/message.h>
#include <raft/core/transporter.h>
#include <raft/core/options.h>

namespace raft {

const int kAppendRequestTimeout = 5 * 1000;

class Server;
class ServerContext;

class Peer {
 public:
  Peer(Options* options, const std::string& addr,
       Server* server, Transporter* trans);
  void Loop(const ServerContext& context);
  void Stop();
  void Flush();
  std::string Addr();

 private:
  AppendEntryRequestPtr prepareAppendRequest(const ServerContext& c);

  AppendEntryReplyPtr sendAppendRequest(const AppendEntryRequestPtr& req, const ServerContext& c);

  void handleAppendReply(AppendEntryReplyPtr& rep, uint64_t entrySize, const AppendChannelPtr& appendch);

  void doAppendEntry(const ServerContext& c);

  Options* options_;
  std::string addr_;
  Server* server_;
  Transporter* trans_;
  boost::shared_ptr<Channel<bool> > stopch_;
  boost::shared_ptr<Channel<bool> > flushch_;
  AppendChannelPtr appendch_;
  uint64_t lastIndex_;
  bool synced_;
  muduo::MutexLock mutex_;
};

bool fillAppendReqFunc(AppendEntryRequest* req, size_t* currentSize, size_t limit, const proto::LogEntry& entry);
typedef boost::shared_ptr<Peer> PeerPtr;

} /* namespace raft */
#endif /* RAFT_CORE_PEER_H_ */
