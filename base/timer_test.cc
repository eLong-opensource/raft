/*
 * testTimer.cc
 *
 *  Created on: Dec 17, 2013
 *      Author: fan
 */

#include <gtest/gtest.h>
#include <muduo/base/Thread.h>
#include <boost/bind.hpp>
#include <raft/base/selector.h>
#include <raft/base/timer.h>

TEST(timer, selector)
{
    raft::Timer timer;
    LOG(INFO) << "timer start.";
    timer.After(100);
    raft::Selector selector;
    selector.Register(&timer);
    raft::SelectableList list;
    selector.Select(&list, -1);
    LOG(INFO) << "timer end.";
    ASSERT_EQ(list.size(), static_cast<size_t>(1));
    ASSERT_TRUE(list[0]->IsReadable());
}

TEST(timer, block)
{
    raft::Timer timer;
    timer.After(10);

    timer.Wait();
    ASSERT_TRUE(timer.IsReadable());
}

TEST(timer, reset)
{
    raft::Timer timer;
    timer.After(100);
    timer.After(10);
    timer.Wait();
    ASSERT_TRUE(timer.IsReadable());
}

// 两个timer, 一个时间短， 一个时间长。
// 短的先过期，然后修改长的。
TEST(timer, reset_after_poll)
{
    raft::Timer timer;
    timer.After(100);

    raft::Timer immediate;
    immediate.After(1);

    raft::Selector selector;
    selector.Register(&timer);
    selector.Register(&immediate);

    selector.Select(NULL, -1);
    ASSERT_TRUE(immediate.IsReadable());

    timer.After(10);
    immediate.After(1000);
    selector.Select(NULL, -1);
    ASSERT_TRUE(timer.IsReadable());
}

void reset(raft::Timer* t)
{
    usleep(20 * 1000);
    t->After(10);
}

TEST(timer, reset_when_poll)
{
    raft::Timer timer;
    timer.After(100);

    muduo::Thread thread(boost::bind(&reset, &timer));
    thread.start();
    raft::Selector selector;
    selector.Poll(&timer, -1);
    ASSERT_TRUE(timer.IsReadable());
}

