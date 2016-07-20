/*
 * testSelector.cc
 *
 *  Created on: Dec 16, 2013
 *      Author: fan
 */

#include <stdio.h>
#include <stdlib.h>
#include <map>
#include <set>
#include <string>
#include <raft/base/selector.h>
#include <raft/base/channel.h>
#include <gtest/gtest.h>
#include <boost/bind.hpp>
#include <muduo/base/Thread.h>

typedef raft::Channel<std::string> StrChannel;

class Sender {
public:
    Sender(StrChannel& ch)
            : ch_(ch),
              times_(0),
              thread_(boost::bind(&Sender::threadFunc, this), "sender")
    {
    }

    void Start(int times)
    {
        times_ = times;
        thread_.start();
    }

private:
    void threadFunc()
    {
        unsigned int seedp;
        for (int i = 0; i < times_; i++) {
            char buf[32];
            snprintf(buf, sizeof(buf), "data %d", i);
            poll(NULL, 0, rand_r(&seedp) % 100 + 100);
            ch_.Put(buf);
        }
        ch_.Close();
    }

    StrChannel& ch_;
    int times_;
    muduo::Thread thread_;
};

class Receiver {
public:
    Receiver()
            : chmap_(),
              thread_(boost::bind(&Receiver::threadFunc, this), "receiver")
    {
    }

    ~Receiver()
    {
        std::map<StrChannel*, std::string>::iterator it = chmap_.begin();
        for (; it != chmap_.end(); it++) {
            delete it->first;
        }
    }

    StrChannel& Register(const std::string& name)
    {
        StrChannel* ch = new StrChannel(10);
        chmap_[ch] = name;
        return *ch;
    }

    void Start()
    {
        thread_.start();
    }

    void Join()
    {
        thread_.join();
    }

private:
    void threadFunc()
    {
        raft::Selector selector;
        std::map<StrChannel*, std::string>::iterator it = chmap_.begin();
        for (; it != chmap_.end(); it++) {
            selector.Register(it->first);
        }

        std::set<StrChannel*> cnt;
        bool running = true;
        raft::SelectableList list;
        while (running) {
            list.clear();
            selector.Select(&list, -1);
            for (size_t i = 0; i < list.size(); i++) {
                StrChannel* ptr = dynamic_cast<StrChannel*>(list[i]);
                CHECK_NOTNULL(ptr);

                std::string s;
                bool ok = ptr->Take(&s);
                if (ok) {
                    LOG(INFO) << "name:" << chmap_[ptr] << " send data:" << s;
                } else {
                    cnt.insert(ptr);
                }
                running = (chmap_.size() != cnt.size());
            }
        }
    }
    std::map<StrChannel*, std::string> chmap_;
    muduo::Thread thread_;
};

TEST(selector, one_channel)
{
    raft::Selector selector;
    raft::Channel<int> channel(10);
    for (int i = 0; i < 10; i++) {
        channel.Put(i);
    }
    selector.Register(&channel);
    raft::SelectableList list;
    int n = selector.Select(&list, -1);
    ASSERT_EQ(n, 1);
    ASSERT_EQ(list.size(), size_t(1));
    ASSERT_TRUE(list[0]->IsReadable());

    raft::Channel<int>* ch = dynamic_cast<raft::Channel<int>*>(list[0]);
    ASSERT_TRUE(ch->IsReadable());
    for (int i = 0; i < 10; i++) {
        int n;
        ASSERT_TRUE(ch->Take(&n));
        ASSERT_EQ(n, i);
    }
}

TEST(selector, poll)
{
    raft::Channel<int> channel(1);
    raft::Selector selector;
    int n = selector.Poll(&channel, 1);
    ASSERT_EQ(n, 0);
    ASSERT_FALSE(channel.IsReadable());

    channel.Put(0);
    n = selector.Poll(&channel, -1);
    ASSERT_TRUE(channel.IsReadable());
    ASSERT_EQ(n, 1);
}

TEST(selector, muti_channel)
{
    raft::Selector selector;
    raft::Channel<int> channel(10);
    for (int i = 0; i < 10; i++) {
        channel.Put(i);
    }
    selector.Register(&channel);
    raft::SelectableList list;
    selector.Select(&list, -1);
    ASSERT_EQ(list.size(), size_t(1));

    raft::Channel<int>* ch = dynamic_cast<raft::Channel<int>*>(list[0]);
    ASSERT_TRUE(ch != NULL);
    for (int i = 0; i < 10; i++) {
        ASSERT_TRUE(ch->IsReadable());
        int n;
        ASSERT_TRUE(ch->Take(&n));
        ASSERT_EQ(n, i);
    }
}

TEST(selector, null)
{
    raft::Selector selector;
    raft::SelectableList list;
    selector.Select(&list, 1);
    ASSERT_EQ(list.size(), size_t(0));
}

TEST(selector, null_list)
{
    raft::Selector selector;
    selector.Select(NULL, 0);

    raft::Channel<int> channel(10);
    channel.Put(0);
    selector.Register(&channel);
    selector.Select(NULL, -1);
    ASSERT_TRUE(channel.IsReadable());
}

TEST(selector, sender_receiver)
{
    Receiver recv;
    std::vector<boost::shared_ptr<Sender> > senderList;
    int numSender = 5;
    for (int i = 0; i < numSender; i++) {
        char buf[32];
        snprintf(buf, sizeof(buf), "sender%d", i);
        StrChannel& ch = recv.Register(buf);
        senderList.push_back(boost::shared_ptr<Sender>(new Sender(ch)));
    }

    for_each(senderList.begin(), senderList.end(),
            boost::bind(&Sender::Start, _1, 5));
    recv.Start();
    recv.Join();
    ASSERT_TRUE(true);
}

