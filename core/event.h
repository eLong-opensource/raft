/*
 * event.h
 *
 *  Created on: Dec 17, 2013
 *      Author: fan
 */

#ifndef RAFT_CORE_EVENT_H_
#define RAFT_CORE_EVENT_H_

#include <muduo/base/CountDownLatch.h>
#include <raft/base/channel.h>
#include <raft/core/message.h>

namespace raft {

typedef ::google::protobuf::Closure* EventCallback;

// request和reponse一直有效，直到callback被触发。
struct Event {
  const Message* Req;
  Message* Rep;
  EventCallback Callback;

  Event(const Message* req, Message* rep, EventCallback cb)
      : Req(req),
        Rep(rep),
        Callback(cb)
  {
  }

  Event()
      : Req(),
        Rep(),
        Callback(NULL)
  {
  }
};

typedef boost::shared_ptr<Event> EventPtr;
typedef Channel<EventPtr> EventChannel;
typedef boost::shared_ptr<EventChannel> EventChannelPtr;
}

#endif /* EVENT_H_ */
