/*
 * peer.cc
 *
 *  Created on: Dec 22, 2013
 *      Author: fan
 */


#include <muduo/base/CurrentThread.h>
#include <raft/base/selector.h>
#include <raft/base/timer.h>
#include <raft/core/peer.h>
#include <raft/core/server.h>
#include <raft/core/context.h>

namespace raft {

Peer::Peer(Options* options, const std::string& addr,
           Server* server, Transporter* trans)
    : options_(options),
      addr_(addr),
      server_(server),
      trans_(trans),
      stopch_(new Channel<bool>),
      flushch_(new Channel<bool>),
      appendch_(new AppendChannel),
      lastIndex_(0),
      synced_(false),
      mutex_()
{
}

void Peer::Stop()
{
  stopch_->Put(true);
}

void Peer::Flush()
{
  flushch_->Put(true);
}

std::string Peer::Addr()
{
  return addr_;
}

void Peer::Loop(const ServerContext& c)
{
  ServerContext context(c);
  lastIndex_ = context.LastLogIndex;

  LOG(INFO) << "Peer loop start. " 
            << " - " << addr_ << " - " << muduo::CurrentThread::tid();
  Timer timer;
  Selector selector;

  selector.Register(flushch_.get());
  selector.Register(stopch_.get());
  selector.Register(&timer);
  for (;;) {
    timer.After(options_->HeartbeatTimeout);
    selector.Select(NULL, -1);

    // for stop
    if (stopch_->IsReadable()) {
      break;
    }

    // for flush
    if (flushch_->IsReadable()) {
      bool tmp;
      while (!flushch_->Empty()) {
        flushch_->Take(&tmp);
      }
      doAppendEntry(context);
    }

    // for timeout
    if (timer.IsReadable()) {
      doAppendEntry(context);
    }
  }
  LOG(INFO) << "Peer thread " << addr_ << " end.";
}

void Peer::doAppendEntry(const ServerContext& c)
{
  AppendEntryRequestPtr req = prepareAppendRequest(c);
  if (!req) {
    return;
  }

  AppendEntryReplyPtr rep = sendAppendRequest(req, c);
  if (!rep) {
    return;
  }

  uint64_t entrySize = req->entries_size();
  handleAppendReply(rep, entrySize, c.Appendch);
}


AppendEntryRequestPtr Peer::prepareAppendRequest(const ServerContext& c)
{
  uint64_t lastTerm = 0;
  // 如果leader的日志被删掉过，并且follower正好差这些日志，错误警告
  if (!server_->GetLog().GetTerm(lastIndex_, &lastTerm)) {
    LOG(ERROR) << "Error to find the log with index:" << lastIndex_;
    return AppendEntryRequestPtr();
  }

  AppendEntryRequestPtr req(new AppendEntryRequest);

  size_t currentSize = 0;
  // 如果没有同步的话，发送空的log entry来寻找同步点
  if (synced_) {
    uint64_t begin = lastIndex_ + 1;
    uint64_t end = kNpos;
    CHECK(server_->GetLog().Walk(
        boost::bind(&fillAppendReqFunc, req.get(), &currentSize, kMaxMessageSize, _1),
        begin, end));
  } else {
    LOG_EVERY_N(INFO, 20) << "Not synced.Send empty entries";
  }

  req->set_term(c.Term);
  req->set_logterm(lastTerm);
  req->set_logindex(lastIndex_);
  req->set_leaderid(options_->MyID);
  req->set_commitindex(server_->CommitIndex());

  return req;
}

AppendEntryReplyPtr Peer::sendAppendRequest(const AppendEntryRequestPtr& req, const ServerContext& c)
{
  //LOG(INFO) << "send append to " << addr_ << " : " << req->logindex() << "-" << req->commitindex() << "-" << req->entries_size();
  if (!trans_->SendAppendEntry(addr_, req, appendch_)) {
    // a follower disconnect
    synced_ = false;
    c.Disconnectch->Put(addr_);
    lastIndex_ = server_->GetLog().LastIndex();
    return AppendEntryReplyPtr();
  }

  Selector selector;
  selector.Poll(appendch_.get(), kAppendRequestTimeout);
  if (!appendch_->IsReadable()) {
    LOG(WARNING) << "wait " << addr_ << " reply timeout!!! clear appendch_.";
    appendch_.reset(new AppendChannel);
    return AppendEntryReplyPtr();
  }

  AppendEntryReplyPtr rep;

  CHECK(appendch_->Take(&rep));

  return rep;
}

void Peer::handleAppendReply(AppendEntryReplyPtr& rep, uint64_t entrySize, const AppendChannelPtr& appendch)
{
  if (rep->success()) {
    synced_ = true;
    lastIndex_ += entrySize;
  } else {
    CHECK(lastIndex_ > 0);
    LOG(INFO) << addr_ << " deny log entry - index: " << lastIndex_;
    if (rep->logindex() < lastIndex_) {
      lastIndex_ = rep->logindex();
      LOG(INFO) << addr_ << " log far behind. update lastIndex_: " << lastIndex_;
    } else {
      lastIndex_--;
      flushch_->Put(true);
    }
    rep->set_logindex(0);
  }

  // send to master
  appendch->Put(rep);
}

bool fillAppendReqFunc(AppendEntryRequest* req, size_t* currentSize, size_t limit, const proto::LogEntry& entry)
{
  if (*currentSize >= limit) {
    return false;
  }
  req->add_entries()->CopyFrom(entry);
  *currentSize += entry.ByteSize();
  return true;
}

} /* namespace raft */
