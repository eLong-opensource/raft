/*
 * command.h
 *
 *  Created on: Dec 25, 2013
 *      Author: fan
 */

#ifndef RAFT_CORE_COMMAND_H_
#define RAFT_CORE_COMMAND_H_

#include <muduo/base/CountDownLatch.h>
#include <boost/function.hpp>
#include <boost/bind.hpp>
#include <boost/shared_ptr.hpp>
#include <raft/core/raft.pb.h>

#include <raft/base/channel.h>

namespace raft {

typedef boost::function<void (int)> CommandCallback;

class Command {
 public:
  proto::EntryType Type;
  std::string Body;

  Command(proto::EntryType type, const std::string body)
      :Type(type),
       Body(body)
  {}

 private:
  CommandCallback callback_;
};

typedef boost::shared_ptr<Command> CommandPtr;
typedef Channel<CommandPtr> CommandChannel;
typedef boost::shared_ptr<CommandChannel> CommandChannelPtr;

}

#endif /* RAFT_CORE_COMMAND_H_ */
