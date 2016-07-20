/*
 * sofarpc.cc
 * Created on: 2014-08-06
 * Author: fan
 */

#include <thirdparty/glog/logging.h>
#include <sofa/pbrpc/rpc_controller.h>
#include <raft/core/transporter/sofarpc.h>
#include <raft/core/transporter/service.h>

namespace raft {
namespace sofarpc {

RpcServer::RpcServer(const std::string& addr, EventChannelPtr ch)
    : server_(NULL),
      addr_(addr)
{
  sofa::pbrpc::RpcServerOptions options;
  options.work_thread_num = 1;
  server_.reset(new sofa::pbrpc::RpcServer(options));
  server_->RegisterService(new RaftServiceImpl(ch));
}

RpcServer::~RpcServer()
{
}

void RpcServer::Start()
{
  CHECK(server_->Start(addr_)) << "Start rpc server failed.";
}

Transporter::Transporter()
    : client_(),
      clients_()
{
  sofa::pbrpc::RpcClientOptions options;
  options.work_thread_num = 1;
  client_.reset(new sofa::pbrpc::RpcClient(options));
}

Transporter::~Transporter()
{
}

bool Transporter::AddPeer(const std::string& addr)
{
  RpcClientPtr client(new RpcClient(client_.get(), addr));
  clients_[addr] = client;
  return true;
}

bool Transporter::SendRequestVote(const std::string& addr, RequestVoteRequestPtr req,
                                  VoteChannelPtr repch)
{
  RpcClientPtr cli = clients_[addr];
  if (!cli) {
    return false;
  }
  RequestVoteReplyPtr rep(new RequestVoteReply);
  sofa::pbrpc::RpcController ctl;
  cli->Stub.RequestVote(&ctl, req.get(), rep.get(), NULL);
  if (ctl.Failed()) {
    LOG_EVERY_N(ERROR, 20) << "RequestVote rpc failed. " << ctl.ErrorText();
    return false;
  }
  repch->Put(rep);
  return true;
}

bool Transporter::SendAppendEntry(const std::string& addr, AppendEntryRequestPtr req,
                                  AppendChannelPtr repch)
{
  RpcClientPtr cli = clients_[addr];
  if (!cli) {
    return false;
  }
  AppendEntryReplyPtr rep(new AppendEntryReply);
  sofa::pbrpc::RpcController ctl;
  cli->Stub.AppendEntry(&ctl, req.get(), rep.get(), NULL);
  if (ctl.Failed()) {
    LOG_EVERY_N(ERROR, 10) << "AppendEntry rpc failed. " << ctl.ErrorText();
    return false;
  }
  repch->Put(rep);
  return true;
}

bool Transporter::SendQueryLeader(const std::string& addr, QueryLeaderRequestPtr req,
                                  QueryChannelPtr repch)
{
  RpcClientPtr cli = clients_[addr];
  if (!cli) {
    return false;
  }
  QueryLeaderReplyPtr rep(new QueryLeaderReply);
  sofa::pbrpc::RpcController ctl;
  cli->Stub.QueryLeader(&ctl, req.get(), rep.get(), NULL);
  if (ctl.Failed()) {
    LOG_EVERY_N(ERROR, 10) << "QueryLeader rpc failed. " << ctl.ErrorText();
    return false;
  }
  repch->Put(rep);
  return true;
}

} // end of namespace sofarpc
} // end of namespace raft
