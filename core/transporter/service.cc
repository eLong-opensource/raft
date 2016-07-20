/*
 * service.cc
 *
 *  Created on: Aug 08, 2014
 *      Author: fan
 */

#include <raft/core/message.h>
#include <raft/core/transporter/service.h>

namespace raft {

RaftServiceImpl::RaftServiceImpl(EventChannelPtr ch)
    : ch_(ch)
{
}

RaftServiceImpl::~RaftServiceImpl()
{
}

void RaftServiceImpl::RequestVote(
    google::protobuf::RpcController* controller,
    const proto::RequestVoteRequest* request,
    proto::RequestVoteReply* response,
    google::protobuf::Closure* done)
{
  EventPtr event(new Event(request, response, done));
  Send(event);
}

void RaftServiceImpl::AppendEntry(
    google::protobuf::RpcController* controller,
    const proto::AppendEntryRequest* request,
    proto::AppendEntryReply* response,
    google::protobuf::Closure* done)
{
  EventPtr event(new Event(request, response, done));
  Send(event);
}

void RaftServiceImpl::QueryLeader(
    google::protobuf::RpcController* controller,
    const proto::QueryLeaderRequest* request,
    proto::QueryLeaderReply* response,
    google::protobuf::Closure* done)
{

  EventPtr event(new Event(request, response, done));
  Send(event);
}

void RaftServiceImpl::Send(const EventPtr& event)
{
  ch_->Put(event);
}

} // end of namespace raft
