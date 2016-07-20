/*
 * rpc.cc
 *
 *  Created on: Dec 17, 2013
 *      Author: fan
 */

#include <stdio.h>

#include <glog/logging.h>

#include <muduo/net/InetAddress.h>
#include <muduo/net/EventLoop.h>
#include <boost/bind.hpp>
#include <raft/core/transporter/closure.h>
#include <raft/base/address.h>
#include <raft/core/transporter/muduorpc.h>

namespace raft {

namespace muduorpc {
/*
 * RPC server
 */

RpcServer::RpcServer(muduo::net::EventLoop* loop, EventChannelPtr ch, const muduo::net::InetAddress& addr)
    : impl_(ch),
      loop_(loop),
      server_(loop, addr)
{
  server_.registerService(&impl_);
}

RpcServer::~RpcServer()
{
  LOG(INFO) << "Raft RpcServer dtor.";
}

void RpcServer::Start()
{
  loop_->runInLoop(boost::bind(&muduo::net::RpcServer::start, &server_));
  LOG(INFO) << "raft rpc server started.";
}

/*
 * RPC client
 */

RpcClient::RpcClient(muduo::net::EventLoop* loop, const muduo::net::InetAddress& addr)
    : connected_(false),
      connecting_(false),
      loop_(loop),
      client_(loop, addr, "raftRpcClient"),
      channel_(),
      stub_(&channel_)
{
  client_.enableRetry();
  client_.setConnectionCallback(boost::bind(&RpcClient::onConnection, this, _1));
  client_.setMessageCallback(boost::bind(&muduo::net::RpcChannel::onMessage, &channel_, _1, _2, _3));
}

RpcClient::~RpcClient()
{
  LOG(INFO) << "Raft RpcClient dtor.";
}

void RpcClient::Connect()
{
  client_.connect();
}

bool RpcClient::Connected()
{
  return connected_;
}

int RpcClient::RequestVote(RequestVoteRequest* req,
                           RequestVoteReply* rep, Callback callback)
{
  if (!connected_) {
    return -1;
  }
  stub_.RequestVote(NULL, req, rep, callback);
  return 0;
}

int RpcClient::AppendEntry(AppendEntryRequest* req,
                           AppendEntryReply* rep, Callback callback)
{
  if (!connected_) {
    return -1;
  }
  stub_.AppendEntry(NULL, req, rep, callback);
  return 0;
}


int RpcClient::QueryLeader(QueryLeaderRequest* req,
                           QueryLeaderReply* rep, Callback callback)
{
  if (!connected_) {
    return -1;
  }
  stub_.QueryLeader(NULL, req, rep, callback);
  return 0;
}

void RpcClient::onConnection(const muduo::net::TcpConnectionPtr& conn)
{
  if (conn->connected()) {
    LOG(INFO) << "raft rpc client connected.";
    channel_.setConnection(conn);
    connected_ = true;
  } else {
    LOG(INFO) << "raft rpc client with peer " << conn->peerAddress().toIpPort() << " lost connection.";
    channel_.clearOutstandings();
    connected_ = false;
  }
}

/*
 * RPC实现的transporter
 */

RpcTransporter::RpcTransporter(muduo::net::EventLoop* loop)
    : loop_(loop)
{
}

bool RpcTransporter::AddPeer(const std::string& addr)
{
  std::string ip;
  uint16_t port;
  if (!Address(addr, &ip, &port)) {
    LOG(ERROR) << "address error:" << addr;
  }

  muduo::net::InetAddress peeraddr(ip, port);
  RpcClientPtr client(new RpcClient(loop_, peeraddr));
  clients_[addr] = client;
  LOG(INFO) << "add client:" << addr;

  return 0;
}

void RpcTransporter::Start()
{
  std::map<std::string, RpcClientPtr>::iterator it;
  for (it = clients_.begin(); it != clients_.end(); it++) {
    it->second->Connect();
  }
  LOG(INFO) << "rcp transporter start.";
}

bool RpcTransporter::SendRequestVote(const std::string& peer,
                                     RequestVoteRequestPtr req, VoteChannelPtr repch)
{
  if (clients_.count(peer) < 1) {
    return false;
  }

  if (!clients_[peer]->Connected()) {
    return false;
  }

  RequestVoteReplyPtr rep(new RequestVoteReply);

  Callback callback = toft::NewClosure(this, &RpcTransporter::SendRequestVoteDone, req, rep, repch);
  int ret = clients_[peer]->RequestVote(req.get(), rep.get(), callback);
  CHECK(ret == 0);

  return true;
}

bool RpcTransporter::SendAppendEntry(const std::string& peer,
                                     AppendEntryRequestPtr req, AppendChannelPtr repch)
{
  if (clients_.count(peer) < 1) {
    return false;
  }

  if (!clients_[peer]->Connected()) {
    return false;
  }

  AppendEntryReplyPtr rep(new AppendEntryReply);

  Callback callback = toft::NewClosure(this,
                                       &RpcTransporter::SendAppendEntryDone, req, rep, repch);
  int ret = clients_[peer]->AppendEntry(req.get(), rep.get(), callback);
  CHECK(ret == 0);

  return true;
}


bool RpcTransporter::SendQueryLeader(const std::string& peer, QueryLeaderRequestPtr req,
                                     QueryChannelPtr repch)
{
  if (clients_.count(peer) < 1) {
    return false;
  }

  if (!clients_[peer]->Connected()) {
    return false;
  }

  QueryLeaderReplyPtr rep(new QueryLeaderReply);

  Callback callback = toft::NewClosure(this,
                                       &RpcTransporter::SendQueryLeaderDone, req, rep, repch);
  int ret = clients_[peer]->QueryLeader(req.get(), rep.get(), callback);
  CHECK(ret == 0);

  return true;
}

void RpcTransporter::SendAppendEntryDone(AppendEntryRequestPtr req, AppendEntryReplyPtr rep, AppendChannelPtr repch)
{
  repch->Put(rep);
}

void RpcTransporter::SendRequestVoteDone(RequestVoteRequestPtr req, RequestVoteReplyPtr rep, VoteChannelPtr repch)
{
  repch->Put(rep);
}

void RpcTransporter::SendQueryLeaderDone(QueryLeaderRequestPtr req, QueryLeaderReplyPtr rep, QueryChannelPtr repch)
{
  repch->Put(rep);
}

} // end of namespace muduorpc
} // end of namespace raft

