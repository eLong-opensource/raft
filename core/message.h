/*
 * message.h
 *
 *  Created on: Dec 17, 2013
 *      Author: fan
 */

#ifndef RAFT_CORE_MESSAGE_H_
#define RAFT_CORE_MESSAGE_H_

#include <boost/shared_ptr.hpp>
#include <google/protobuf/message.h>
#include <google/protobuf/descriptor.h>

#include <raft/core/raft.pb.h>
#include <raft/base/channel.h>

namespace raft {

using proto::RequestVoteRequest;
using proto::RequestVoteReply;
using proto::AppendEntryRequest;
using proto::AppendEntryReply;
using proto::QueryLeaderRequest;
using proto::QueryLeaderReply;

using google::protobuf::Message;

const std::string kRequestVoteRequest = "RequestVoteRequest";
const std::string kRequestVoteReply = "RequestVoteReply";
const std::string kAppendEntryRequest = "AppendEntryRequest";
const std::string kAppendEntryReply = "AppendEntryReply";
const std::string kQueryLeaderRequest = "QueryLeaderRequest";
const std::string kQueryLeaderReply = "QueryLeaderReply";

const size_t kMaxMessageSize = 1024 * 1024 - 1024;

typedef boost::shared_ptr<RequestVoteRequest> RequestVoteRequestPtr;
typedef boost::shared_ptr<RequestVoteReply> RequestVoteReplyPtr;

typedef boost::shared_ptr<AppendEntryRequest> AppendEntryRequestPtr;
typedef boost::shared_ptr<AppendEntryReply> AppendEntryReplyPtr;

typedef boost::shared_ptr<QueryLeaderRequest> QueryLeaderRequestPtr;
typedef boost::shared_ptr<QueryLeaderReply> QueryLeaderReplyPtr;

typedef boost::shared_ptr<Message> MessagePtr;
typedef Channel<MessagePtr> MessageChannel;
typedef boost::shared_ptr<MessageChannel> MessageChannelPtr;

typedef Channel<RequestVoteReplyPtr> VoteChannel;
typedef boost::shared_ptr<VoteChannel> VoteChannelPtr;

typedef Channel<AppendEntryReplyPtr> AppendChannel;
typedef boost::shared_ptr<AppendChannel> AppendChannelPtr;

typedef Channel<QueryLeaderReplyPtr> QueryChannel;
typedef boost::shared_ptr<QueryChannel> QueryChannelPtr;

typedef Channel<std::string> DisconnectChannel;
typedef boost::shared_ptr<DisconnectChannel> DisconnectChannelPtr;

inline const std::string& Type(const Message* msg)
{
  const google::protobuf::Descriptor* desp = msg->GetDescriptor();
  return desp->name();
}

inline void Swap(Message* msg1, Message* msg2)
{
  const google::protobuf::Reflection* ref = msg1->GetReflection();
  ref->Swap(msg1, msg2);
}

}
#endif /* RAFT_CORE_MESSAGE_H_ */
