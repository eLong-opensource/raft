/*
 * server.cc
 *
 *  Created on: Dec 12, 2013
 *      Author: fan
 */

#include <stdlib.h>
#include <algorithm>
#include <boost/bind.hpp>
#include <boost/shared_ptr.hpp>

#include <muduo/base/Thread.h>
#include <toft/base/closure.h>

#include <raft/base/selector.h>
#include <raft/base/timer.h>
#include <raft/core/message.h>
#include <raft/core/server.h>
#include <raft/core/context.h>

namespace raft {

// TODO init vars
Server::Server(const Options& options, Transporter* trans)
    : options_(options),
      leaderId_(0),
      currentTerm_(0),
      commitIndex_(0),
      trans_(trans),
      ch_(new EventChannel),
      commandch_(new CommandChannel(kCommandChannelSize)),
      state_(FOLLOWER),
      commitSize_(0),
      random_(options_.MyID),
      voteFor_(0),
      peers_(),
      mutex_(),
      log_(&options_),
      meta_(options_.RaftLogDir),
      single_(false),
      lastSnapshotTime_(::time(NULL)),
      syncedCallback_(defaultSyncedCallback),
      stateChangedCallback_(defaultStateChangedCallback),
      leaderChangedCallback_(defaultLeaderChangedCallback),
      takeSnapshotCallback_(defaultTakeSnapshotCallback),
      loadSnapshotCallback_(defaultLoadSnapshotCallback)
{
  CHECK(options_.MyID > 0);
}

Server::~Server()
{
  ch_->Close();
}

void Server::Start()
{
  muduo::Thread(boost::bind(&Server::Run, this), "raft").start();
}

void Server::Run()
{
  uint64_t currentTerm, commitIndex;
  meta_.Load(&currentTerm, &voteFor_, &commitIndex);
  currentTerm_ = currentTerm;
  commitIndex_ = commitIndex;

  // 如果状态机的commit index大于meta里面记录的，使用状态机里面的
  uint64_t idx = loadSnapshotCallback_(commitIndex);
  if (idx != 0) {
    commitIndex_ = idx;
  }
  LOG(INFO) << "Load snapshot done. index:" << commitIndex_;
  log_.Load();

  // 如果把所有的日志清除了，导致状态机存储的commit index大于日志的index
  // 使用日志的index作为新的index
  if (commitIndex_ > log_.LastIndex()) {
    LOG(WARNING) << "commit index " << commitIndex_
                 << " bigger than last log index " << log_.LastIndex();
    commitIndex_ = log_.LastIndex();
  }

  LOG(INFO) << "raft server start with id:" << options_.MyID << " .";
  if (peers_.empty()) {
    LOG(INFO) << "no peers, enter single mode.";
    single_ = true;
    state_ = LEADER;
  }
  mainLoop();
}

void Server::AddPeer(const std::string& addr)
{
  PeerPtr peer(new Peer(&options_, addr, this, trans_));
  peers_.push_back(peer);
}


int Server::Broadcast(const std::string& data)
{
  if (LeaderId() != MyId()) {
    return -1;
  }
  CommandPtr cmd(new Command(proto::LOG_ENTRY, data));
  commandch_->Put(cmd);
  return 0;
}

EventPtr Server::get()
{
  EventPtr event;
  bool ok = ch_->Take(&event);
  CHECK(ok);
  return event;
}

void Server::mainLoop()
{
  for (;;) {
    switch (state_) {
      case FOLLOWER:
        LOG(INFO) << "now state: FOLLOWER";
        followerLoop();
        break;
      case CANDIDATE:
        LOG(INFO) << "now state: CANDIDATE";
        candidateLoop();
        break;
      case LEADER:
        LOG(INFO) << "now state: LEADER";
        leaderLoop();
        break;
      default:
        LOG(FATAL) << "Unknow state " << state_;
        break;
    }
  }
}

void Server::followerLoop()
{
  bool update = false;
  Selector selector;
  Timer timer;

  int timeout = randomBetween(options_.ElectionTimeout,
                              2 * options_.ElectionTimeout);
  LOG(INFO) << "follower set timeout:" << timeout;
  timer.After(timeout);

  selector.Register(&timer);
  selector.Register(ch_.get());

  // notify change to follower
  stateChangedCallback_(state_);

  for (;;) {
    update = false;
    SelectableList list;
    int n = selector.Select(&list, -1);

    // rarely happen.
    if (n == 0) {
      continue;
    }

    Selectable* choose = list[0];

    // timeout!!! switch candidate
    if (&timer == choose) {
      LOG(INFO) << "follower timeout!!!, switch to candidate.";
      state_ = CANDIDATE;
      break;
    }

    if (ch_.get() == choose) {
      EventPtr e = get();
      update = handleEvent(e);
    }

    if (update) {
      timer.After(randomBetween(options_.ElectionTimeout,
                                2 * options_.ElectionTimeout));
    }
  } // end of for loop

}

void Server::candidateLoop()
{
  VoteChannelPtr votech(new VoteChannel);
  QueryChannelPtr querych(new QueryChannel);
  Timer timer;
  Selector selector;

  selector.Register(votech.get());
  selector.Register(querych.get());
  selector.Register(&timer);
  selector.Register(ch_.get());

  int timeout = randomBetween(options_.ElectionTimeout,
                              2 * options_.ElectionTimeout);
  LOG(INFO) << "candidate set timeout:" << timeout;
  timer.After(timeout);

  leaderId_ = 0;
  size_t grants = 1;
  size_t noleader = 1;
  sendQuery(querych);

  // notify change to candidate
  stateChangedCallback_(state_);

  for (;;) {
    SelectableList list;
    int n = selector.Select(&list, -1);
    Selectable* choose = list[0];
    // rarely happen.
    if (n == 0) {
      continue;
    }

    // for timer
    if (&timer == choose) {
      LOG(INFO) << "candidate timeout, again.";
      break;
    }

    if (querych.get() == choose) {
      QueryLeaderReplyPtr rep;
      querych->Take(&rep);
      if (rep->leaderid() == 0) {
        noleader++;
        LOG(INFO) << "query leader failed. noleader size:" << noleader;
      } else {
        LOG(INFO) << "query leader ok. leader id " << rep->leaderid();
      }
      if (noleader >= quoramSize()) {
        setTerm(Term() + 1);
        setVoteFor(options_.MyID);

        LOG(INFO) << "candidate send votes.";
        sendVotes(votech);
        // do not receive query channel
        querych.reset(new QueryChannel);
      }
    }

    // for vote reply
    if (votech.get() == choose) {
      RequestVoteReplyPtr rep;
      bool ok = votech->Take(&rep);
      CHECK(ok);

      if (rep->term() > Term()) {
        LOG(INFO) << "candidate stepDown.";
        stepDown(rep->term());
      }
      if (rep->granted()) {
        grants++;
        LOG(INFO) << "vote accepted. current votes:" << grants;
      } else {
        LOG(INFO) << "vote request deny. current votes:" << grants;
      }
      if (grants >= quoramSize()) {
        LOG(INFO) << "candidate become leader.";
        state_ = LEADER;
        break;
      }
    }

    // for rpc request
    if (ch_.get() == choose) {
      EventPtr e = get();
      handleEvent(e);
    }

    if (state_ != CANDIDATE) {
      break;
    }
  } // end of for loop

}

void Server::leaderLoop()
{
  AppendChannelPtr appendch(new AppendChannel);
  DisconnectChannelPtr disconnectch(new DisconnectChannel);
  LogIndexList logIndexList(peers_.size() + 1, 0);
  DisconnectedPeers disconnectedPeers;

  // clear command channel
  commandch_.reset(new CommandChannel);

  ServerContext context(Term(), log_.LastIndex(), appendch, disconnectch);

  // thread will end when loop ends.
  for (size_t i = 0; i < peers_.size(); i++) {
    std::string addr = peers_[i]->Addr();
    peers_[i].reset(new Peer(&options_, addr, this, trans_));
    muduo::Thread thread(boost::bind(&Peer::Loop, peers_[i], context));
    thread.start();
  }

  broadCastNewLeader();
  leaderId_ = options_.MyID;

  Selector selector;
  selector.Register(ch_.get());
  selector.Register(appendch.get());
  selector.Register(disconnectch.get());
  selector.Register(commandch_.get());

  // notify change to leader
  stateChangedCallback_(state_);

  for (;;) {
    SelectableList list;
    int n = selector.Select(&list, -1);
    if (n == 0) {
      continue;
    }

    Selectable* choose = list[0];

    // for append entry reply
    if (appendch.get() == choose) {
      AppendEntryReplyPtr rep;
      bool ok = appendch->Take(&rep);
      CHECK(ok);
      handleAppendEntryReply(rep.get(), &logIndexList);
    }

    if (disconnectch.get() == choose) {
      std::string req;
      disconnectch->Take(&req);
      handleDisconnect(req, &disconnectedPeers);
    }

    if (ch_.get() == choose) {
      EventPtr e = get();
      handleEvent(e);
    }

    // for command
    if (commandch_.get() == choose) {
      CommandPtr cmd;
      commandch_->Take(&cmd);
      handleCommand(cmd, &logIndexList, appendch);
    }

    if (state_ != LEADER) {
      LOG(INFO) << "Send stop signal to peers";
      stopPeers();
      break;
    }
  } // end of for loop
}

void Server::stopPeers()
{
  for (size_t i = 0; i < peers_.size(); i++) {
    peers_[i]->Stop();
  }
}

bool Server::handleEvent(EventPtr e)
{
  const std::string& type = Type(e->Req);

  if (type == kRequestVoteRequest) {
    return handleRequestVote(e);
  } else if (type == kAppendEntryRequest) {
    return handleAppendEntry(e);
  } else if (type == kQueryLeaderRequest){
    handleQueryLeader(e);
  } else {
    // some other message
  }
  return false;
}

bool Server::handleRequestVote(const EventPtr& e)
{
  const RequestVoteRequest* req = dynamic_cast<const RequestVoteRequest*>(e->Req);
  RequestVoteReply* rep = dynamic_cast<RequestVoteReply*>(e->Rep);
  CHECK(req != NULL);
  CHECK(rep != NULL);

  uint64_t currentTerm = Term();

  LOG(INFO) << "receive new request vote: " << req->Utf8DebugString();
  LOG(INFO) << "server state - term: " << currentTerm << " voteFor: " << voteFor_ <<
      " lastTerm: " << log_.LastTerm() << " lastIndex: " << log_.LastIndex();

  if (req->term() < currentTerm) {
    LOG(INFO) << "deny vote - term old.";
    rep->set_term(currentTerm);
    rep->set_granted(false);
    e->Callback->Run();
    return false;
  }

  if (req->term() > currentTerm) {
    stepDown(req->term());
  }

  if (voteFor_ != 0 && voteFor_ != req->candidateid()) {
    LOG(INFO) << "vote deny - had voted " << voteFor_;
    rep->set_term(currentTerm);
    rep->set_granted(false);
    e->Callback->Run();
    return false;
  }

  if ((req->logterm() > log_.LastTerm()) ||
      (req->logterm() == log_.LastTerm() && req->logindex() >= log_.LastIndex())) {
    LOG(INFO) << "vote pass. - vote " << req->candidateid();
    setVoteFor(req->candidateid());
    rep->set_term(currentTerm);
    rep->set_granted(true);
    e->Callback->Run();
    return true;
  }

  LOG(INFO) << "vote deny - log old.";
  rep->set_term(currentTerm);
  rep->set_granted(false);
  e->Callback->Run();
  return false;
}

bool Server::handleAppendEntry(const EventPtr& e)
{
  const AppendEntryRequest* req = dynamic_cast<const AppendEntryRequest*>(e->Req);
  AppendEntryReply* rep = dynamic_cast<AppendEntryReply*>(e->Rep);
  CHECK(req != NULL);
  CHECK(rep != NULL);
  //LOG(INFO) << "receive append entry." << req->logindex() << "-" << req->commitindex() << " - " << req->entries_size();

  uint64_t currentTerm = Term();
  uint64_t lastIndex = log_.LastIndex();
  uint32_t serverId = options_.MyID;

  if (req->term() < currentTerm) {
    LOG(INFO) << "append entry from old leader:" << req->term() << " my term:" << currentTerm;
    rep->set_serverid(serverId);
    rep->set_term(currentTerm);
    rep->set_logindex(lastIndex);
    rep->set_success(false);
    e->Callback->Run();
    return false;
  }
  // 此处使用了即使term相等也stepDown, 否则一个down掉的follower会一直处于candidate查询leader,收到心跳也不会stepDown
  if (req->term() >= currentTerm) {
    stepDown(req->term());
  }

  if (!log_.IsAccepted(req->logindex(), req->logterm())) {
    LOG(INFO) << "log entry not equal. wait again. index: "
              << req->logindex() << " term: " << req->logterm();
    if (req->logindex() <= log_.LastIndex()) {
      uint64_t t;
      log_.GetTerm(req->logindex(), &t);
      LOG(INFO) << "my state - term: " << t;
    } else {
      LOG(INFO) << "my state -index:" << log_.LastIndex();
    }

    rep->set_serverid(serverId);
    rep->set_term(currentTerm);
    rep->set_logindex(lastIndex);
    rep->set_success(false);
    e->Callback->Run();
    return true;
  }

  leaderId_ = req->leaderid();

  log_.Trunc(req->logindex() + 1);
  log_.Append(*req);
  lastIndex = log_.LastIndex();

  // leader发来的commitIndex可能会大于自己的最后一条日志
  uint64_t start = CommitIndex() + 1;
  uint64_t maxUpto = start + options_.MaxCommitSize;
  uint64_t commitUpto = std::min(maxUpto,
                                 std::min(req->commitindex(),
                                          log_.LastIndex()));

  uint64_t now = ::time(NULL);
  if (lastSnapshotTime_ + options_.SnapshotInterval < now) {
    takeSnapshotCallback_(CommitIndex(), boost::bind(&Server::takeSnapshotDone, this, CommitIndex()));
    lastSnapshotTime_ = now;
    LOG(INFO) << "Take snapshot by time, commit index:" << CommitIndex();
  }

  if (CommitIndex() >= commitUpto) {
    if (CommitIndex() > commitUpto) {
      LOG(WARNING) << "commit index bigger than leader commit index:" << CommitIndex() << " - " << commitUpto;
    }
    rep->set_serverid(serverId);
    rep->set_term(currentTerm);
    rep->set_logindex(lastIndex);
    rep->set_success(true);
    e->Callback->Run();
    return true;
  }

  //LOG(INFO) << "will commit " << (commitUpto + 1 - start) << " logs";

  if (!log_.Walk(
          boost::bind(&Server::commitLogEntry, this, _1),
          start, commitUpto + 1))
  {
    LOG(FATAL) << "commit error.";
  }

  commitSize_ += (commitUpto + 1 - start);
  setCommitIndex(commitUpto);
  if (options_.SnapshotLogSize > 0 &&
      commitSize_ >= options_.SnapshotLogSize) {
    takeSnapshotCallback_(commitUpto, boost::bind(&Server::takeSnapshotDone, this, commitUpto));
    commitSize_ = 0;
    LOG(INFO) << "Take snapshot, commit index:" << commitUpto;
  }

  rep->set_serverid(serverId);
  rep->set_term(currentTerm);
  rep->set_logindex(lastIndex);
  rep->set_success(true);
  e->Callback->Run();
  return true;
}

void Server::handleQueryLeader(const EventPtr& e)
{
  //const QueryLeaderRequest* req = dynamic_cast<const QueryLeaderRequest*>(e->Req);
  QueryLeaderReply* rep = dynamic_cast<QueryLeaderReply*>(e->Rep);
  CHECK(rep != NULL);
  if (state_ == CANDIDATE) {
    rep->set_leaderid(0);
  } else {
    rep->set_leaderid(leaderId_);
  }
  e->Callback->Run();
}

void Server::handleAppendEntryReply(AppendEntryReply* rep, LogIndexList* list)
{
  uint64_t currentTerm = Term();

  if (rep->term() < currentTerm) {
    return;
  }

  if (rep->term() > currentTerm) {
    stepDown(rep->term());
    LOG(INFO) << "find higher term from append entry reply, stepDown. currentTerm: " <<
        currentTerm << " term: " << rep->term();
  }

  if (!rep->success()) {
    return;
  }

  (*list)[rep->serverid() - 1] = rep->logindex();
  uint64_t n = quoramIndex(*list);
  //LOG(INFO) << "commit point: " << n;

  uint64_t start = CommitIndex() + 1;
  uint64_t maxUpto = start + options_.MaxCommitSize;
  uint64_t commitUpto = std::min(maxUpto,
                                 std::min(n, log_.LastIndex()));

  uint64_t now = ::time(NULL);
  if (lastSnapshotTime_ + options_.SnapshotInterval < now) {
    takeSnapshotCallback_(CommitIndex(), boost::bind(&Server::takeSnapshotDone, this, CommitIndex()));
    lastSnapshotTime_ = now;
    LOG(INFO) << "Take snapshot by time, commit index:" << CommitIndex();
  }

  // 在单例模式下，日志提交的驱动在于刚启动的时候发布的LEADER日志和之后的日志提交，
  // 因此在刚启动的时候会面临着日志提交不够到最新的日志，因此这里直接提交到最新的日志
  if (single_) {
    commitUpto = log_.LastIndex();
  }

  if (CommitIndex() >= commitUpto) {
    if (CommitIndex() > commitUpto) {
      LOG(WARNING) << "commit index bigger than quoram index:" << start - 1 << " - " << commitUpto;
    }
    return;
  }

  //LOG(INFO) << "will commit " << (commitUpto + 1 - start) << " logs";

  if (!log_.Walk(
          boost::bind(&Server::commitLogEntry, this, _1),
          start, commitUpto + 1))
  {
    LOG(FATAL) << "commit error.";
  }

  commitSize_ += (commitUpto + 1 - start);
  setCommitIndex(commitUpto);
  if (options_.SnapshotLogSize > 0 &&
      commitSize_ >= options_.SnapshotLogSize) {
    takeSnapshotCallback_(commitUpto, boost::bind(&Server::takeSnapshotDone, this, commitUpto));
    commitSize_ = 0;
    LOG(INFO) << "Take snapshot, commit index:" << commitUpto;
  }
}

void Server::handleCommand(const CommandPtr& req, LogIndexList* list, AppendChannelPtr appendch)
{
  //LOG(INFO) << "new command:" << "type:" << req->Type << " body:" << req->Body;
  uint64_t newIndex = log_.LastIndex() + 1;
  LogEntryPtr entry(new LogEntry(req->Type, Term(), newIndex, req->Body));
  log_.Append(entry);
  (*list)[options_.MyID - 1] = newIndex;
  if (options_.ForceFlush) {
    flushPeers();
  }

  // 单例模式下直接返回日志同步成功
  if (single_) {
    AppendEntryReplyPtr rep(new AppendEntryReply);
    rep->set_serverid(options_.MyID);
    rep->set_term(currentTerm_);
    rep->set_logindex(newIndex);
    rep->set_success(true);
    appendch->Put(rep);
  }
}

void Server::handleDisconnect(const std::string& req, DisconnectedPeers* peers)
{
  if (peers->count(req) != 0) {
    // ignore
    return;
  }

  peers->insert(req);
  LOG(INFO) << "lost a peer:" << req << ". now lost:"<< peers->size();
  if (peers->size() >= quoramSize()) {
    LOG(INFO) << "lost too much peer. stepdown.";
    state_ = FOLLOWER;
  }
}

bool Server::commitLogEntry(const proto::LogEntry& entry)
{
  if (entry.type() == LOG_ENTRY) {
    syncedCallback_(entry.index(), entry.body());
  } else if (entry.type() == NEW_LEADER){
    if (Term() == entry.term()) {
      leaderChangedCallback_(leaderId_);
    }
    LOG(INFO) << "commit new leader entry.";
  }

  return true;
}

void Server::flushPeers()
{
  for (size_t i=0; i<peers_.size(); i++) {
    peers_[i]->Flush();
  }
}

void Server::stepDown(uint64_t term)
{
  CHECK(term >= Term());
  if (term > Term()) {
    setTerm(term);
    setVoteFor(0);
    // TODO
  }
  state_ = FOLLOWER;
}

void Server::sendVotes(VoteChannelPtr ch)
{
  RequestVoteRequestPtr req(new RequestVoteRequest);
  req->set_term(Term());
  req->set_logindex(log_.LastIndex());
  req->set_logterm(log_.LastTerm());
  req->set_candidateid(options_.MyID);

  LOG(INFO) << "send votes:" << req->Utf8DebugString();
  for (size_t i = 0; i < peers_.size(); i++) {
    muduo::Thread(boost::bind(&Transporter::SendRequestVote,
                              trans_, peers_[i]->Addr(), req, ch),
                  "sendVote").start();
  }
}

void Server::sendQuery(QueryChannelPtr ch)
{
  QueryLeaderRequestPtr req(new QueryLeaderRequest);
  LOG(INFO) << "send query";
  for (size_t i = 0; i < peers_.size(); i++) {
    muduo::Thread(boost::bind(&Transporter::SendQueryLeader,
                              trans_, peers_[i]->Addr(), req, ch),
                  "sendVote").start();
  }
}

size_t Server::quoramSize()
{
  return peers_.size() / 2 + 1;
}

uint64_t Server::quoramIndex(const LogIndexList& list)
{
  LogIndexList lastIndexs;
  lastIndexs.assign(list.begin(), list.end());

  std::sort(lastIndexs.begin(), lastIndexs.end(), std::greater<uint64_t>());

  return lastIndexs[quoramSize() - 1];
}

void Server::broadCastNewLeader()
{
  CommandPtr cmd(new Command(proto::NEW_LEADER, ""));
  commandch_->Put(cmd);
}


void Server::setTerm(uint64_t term)
{
  currentTerm_ = term;
  meta_.UpdateTerm(term);
}

void Server::setVoteFor(uint32_t voteFor)
{
  voteFor_ = voteFor;
  meta_.UpdateVoteFor(voteFor);
}

void Server::setCommitIndex(uint64_t index)
{
  commitIndex_ = index;
}

void Server::takeSnapshotDone(uint64_t index)
{
  meta_.UpdateCommitIndex(index);
  LOG(INFO) << "Take snapshot done. commit index:" << index;
}

int Server::randomBetween(int begin, int end)
{
  if (begin > end) {
    return -1;
  }
  return begin + random_.Uniform(end - begin);
}


void defaultSyncedCallback(uint64_t index, const std::string& data)
{
}

void defaultStateChangedCallback(ServerState state)
{
}

void defaultLeaderChangedCallback(int leaderId)
{
}

void defaultTakeSnapshotCallback(int index, const TakeSnapshotDoneCallback& done)
{
  done();
}

uint64_t defaultLoadSnapshotCallback(int index)
{
  return index;
}

} // end of namespace raft
