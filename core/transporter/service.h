/*
 * service.h
 * Created on: 2014-08-06
 * Author: fan
 */

#ifndef RAFT_CORE_TRANSPORTER_SERVICE_H_
#define RAFT_CORE_TRANSPORTER_SERVICE_H_

#include <raft/core/raft.pb.h>
#include <raft/core/event.h>

namespace raft {

class RaftServiceImpl: public proto::RaftService, boost::noncopyable {
 public:
  RaftServiceImpl(EventChannelPtr ch);
  ~RaftServiceImpl();

  // block call. send event to channel.
  void Send(const EventPtr& event);

  virtual void RequestVote(::google::protobuf::RpcController* controller,
                           const proto::RequestVoteRequest* request,
                           proto::RequestVoteReply* response,
                           ::google::protobuf::Closure* done);

  virtual void AppendEntry(::google::protobuf::RpcController* controller,
                           const proto::AppendEntryRequest* request,
                           proto::AppendEntryReply* response,
                           ::google::protobuf::Closure* done);

  virtual void QueryLeader(::google::protobuf::RpcController* controller,
                           const proto::QueryLeaderRequest* request,
                           proto::QueryLeaderReply* response,
                           ::google::protobuf::Closure* done);

 private:
  EventChannelPtr ch_;
};

} // end of namespace raft

#endif
