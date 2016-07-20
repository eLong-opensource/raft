/*
 * sofarpc.h
 * Created on: 2014-08-06
 * Author: fan
 */

#ifndef RAFT_CORE_TRANSPORTER_SOFARPC_H_
#define RAFT_CORE_TRANSPORTER_SOFARPC_H_

#include <string>
#include <boost/scoped_ptr.hpp>
#include <boost/noncopyable.hpp>
#include <sofa/pbrpc/rpc_server.h>
#include <sofa/pbrpc/rpc_client.h>
#include <sofa/pbrpc/rpc_channel.h>

#include <raft/core/transporter.h>
#include <raft/core/event.h>

namespace raft {
namespace sofarpc {

class RpcServer: boost::noncopyable {
 public:
  RpcServer(const std::string& addr, EventChannelPtr ch);
  ~RpcServer();
  void Start();
 private:
  boost::scoped_ptr<sofa::pbrpc::RpcServer> server_;
  std::string addr_;
};

class RpcClient: boost::noncopyable {
 public:
  RpcClient(sofa::pbrpc::RpcClient* client, const std::string& addr)
      : channel_(client, addr),
        Stub(&channel_)
  {
  }

 private:
  sofa::pbrpc::RpcChannel channel_;
 public:
  proto::RaftService::Stub Stub;
};

class Transporter: public raft::Transporter, boost::noncopyable {
 public:
  Transporter();
  ~Transporter();

  bool AddPeer(const std::string& addr);

  bool SendRequestVote(const std::string& peer, RequestVoteRequestPtr req,
                       VoteChannelPtr repch);
  bool SendAppendEntry(const std::string& peer, AppendEntryRequestPtr req,
                       AppendChannelPtr repch);
  bool SendQueryLeader(const std::string& peer, QueryLeaderRequestPtr req,
                       QueryChannelPtr repch);

 private:
  typedef boost::shared_ptr<RpcClient> RpcClientPtr;
  boost::scoped_ptr<sofa::pbrpc::RpcClient> client_;
  std::map<std::string, RpcClientPtr> clients_;
};

} // end of namespace sofarpc
} // end of namespace raft
#endif
