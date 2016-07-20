/*
 * rpc.h
 *
 *  Created on: Dec 17, 2013
 *      Author: fan
 */

#ifndef RAFT_CORE_TRANSPORTER_MUDUORPC_H_
#define RAFT_CORE_TRANSPORTER_MUDUORPC_H_

#include <muduo/net/EventLoop.h>
#include <muduo/net/InetAddress.h>
#include <muduo/net/protorpc/RpcServer.h>
#include <muduo/net/protorpc/RpcChannel.h>
#include <muduo/net/TcpClient.h>
#include <muduo/base/CountDownLatch.h>

#include <boost/shared_ptr.hpp>
#include <raft/core/transporter.h>
#include <raft/core/transporter/service.h>

namespace raft {

class Server;

namespace muduorpc {

// 简单rpc server. start会新起一个线程。
class RpcServer: boost::noncopyable {
 public:
  RpcServer(muduo::net::EventLoop* loop, EventChannelPtr ch, const muduo::net::InetAddress& addr);
  ~RpcServer();
  void Start();

 private:
  RaftServiceImpl impl_;
  muduo::net::EventLoop* loop_;
  muduo::net::RpcServer server_;

};

typedef ::google::protobuf::Closure* Callback;

// 简单rpc client。
class RpcClient: boost::noncopyable {
 public:
  RpcClient(muduo::net::EventLoop* loop, const muduo::net::InetAddress& addr);
  ~RpcClient();

  // nonblock. need check.
  void Connect();

  bool Connected();

  // 当异步使用的时候，用户保证数据的有效性。
  int RequestVote(RequestVoteRequest* req,
                  RequestVoteReply* rep, Callback callback);
  int AppendEntry(AppendEntryRequest* req,
                  AppendEntryReply* rep, Callback callback);
  int QueryLeader(QueryLeaderRequest* req,
                  QueryLeaderReply* rep, Callback callback);

 private:
  void onConnection(const muduo::net::TcpConnectionPtr& conn);
  bool connected_;
  bool connecting_;
  muduo::net::EventLoop* loop_;
  muduo::net::TcpClient client_;
  muduo::net::RpcChannel channel_;
  proto::RaftService::Stub stub_;
};

// not safe
class RpcTransporter: public raft::Transporter, boost::noncopyable {
 public:
  RpcTransporter(muduo::net::EventLoop* loop);
  bool AddPeer(const std::string& addr);
  void Start();

  bool SendRequestVote(const std::string& peer, RequestVoteRequestPtr req,
                       VoteChannelPtr repch);
  bool SendAppendEntry(const std::string& peer, AppendEntryRequestPtr req,
                       AppendChannelPtr repch);
  bool SendQueryLeader(const std::string& peer, QueryLeaderRequestPtr req,
                       QueryChannelPtr repch);

  void SendAppendEntryDone(AppendEntryRequestPtr req, AppendEntryReplyPtr rep, AppendChannelPtr repch);
  void SendRequestVoteDone(RequestVoteRequestPtr req, RequestVoteReplyPtr rep, VoteChannelPtr repch);
  void SendQueryLeaderDone(QueryLeaderRequestPtr req, QueryLeaderReplyPtr rep, QueryChannelPtr repch);

 private:
  typedef boost::shared_ptr<RpcClient> RpcClientPtr;
  muduo::net::EventLoop* loop_;
  std::map<std::string, RpcClientPtr> clients_;
};

} /* namespace muduorpc */
} /* namespace raft */
#endif /* RAFT_CORE_TRANSPORTER_MUDUORPC_H_ */
