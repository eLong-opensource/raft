#include <raft/base/channel.h>
#include <raft/base/selector.h>
#include <muduo/base/Mutex.h>
#include <muduo/base/CountDownLatch.h>
#include <muduo/base/Thread.h>

#include <boost/bind.hpp>
#include <string>
#include <vector>
#include <stdio.h>

#include <gtest/gtest.h>


typedef boost::shared_ptr<muduo::Thread> ThreadPtr;
class ChannelTest {
public:
    ChannelTest(int numThreads)
            : latch_(numThreads), threads_(), numData_(0), mutex_(), channel_()
    {
        char name[32];
        for (int i = 0; i < numThreads; i++) {
            snprintf(name, sizeof(name), "work thread %d", i);
            ThreadPtr thread(
                    new muduo::Thread(
                            boost::bind(&ChannelTest::threadFunc, this),
                            muduo::string(name)));
            threads_.push_back(thread);
        }
    }

    void RunAll(int times)
    {
        for_each(threads_.begin(), threads_.end(),
                boost::bind(&muduo::Thread::start, _1));
        printf("waiting for all thread running up.\n");
        latch_.wait();
        printf("all started.");
        for (int i = 0; i < times; i++) {
            char buf[32];
            snprintf(buf, sizeof(buf), "str %d", i);
            channel_.Put(buf);
        }
        printf("put %d data.\n", times);
    }

    void JoinAll()
    {
        for (size_t i = 0; i < threads_.size(); i++) {
            channel_.Close();
        }
        for_each(threads_.begin(), threads_.end(),
                boost::bind(&muduo::Thread::join, _1));
    }

    void Start(int times)
    {
        RunAll(times);
        JoinAll();
    }

    int Sum()
    {
        return numData_;
    }

private:
    void threadFunc()
    {
        latch_.countDown();
        bool running = true;
        int cnt = 0;
        while (running) {
            std::string s;
            bool ok = channel_.Take(&s);
            if (ok) {
                printf("tid=%d, get:%s\n", muduo::CurrentThread::tid(),
                        s.c_str());
                cnt++;
            } else {
                running = false;
            }
        }
        muduo::MutexLockGuard guard(mutex_);
        numData_ += cnt;
    }
    muduo::CountDownLatch latch_;
    std::vector<ThreadPtr> threads_;
    int numData_;
    muduo::MutexLock mutex_;
    raft::Channel<std::string> channel_;
};

TEST(Channel, muti_reader)
{
    ChannelTest test(10);
    test.Start(100);
    ASSERT_EQ(test.Sum(), 100);
}

TEST(channel, close)
{
    int N = 5;
    raft::Channel<int> ch;
    for (int i = 0; i < N; i++) {
        ch.Put(i);
    }
    ch.Close();
    int n = 0;
    int cnt = 0;
    while (ch.Take(&n)) {
        ASSERT_EQ(n, cnt++);
    }
}

TEST(channel, shuffle)
{
    raft::SelectableList list;
    raft::Selector selector;

    const int n = 5;
    std::vector<raft::Channel<int>* > chs(n);
    for (int i=0; i<n; i++) {
        chs[i] = new raft::Channel<int>(n);
        chs[i]->Put(i);
        selector.Register(chs[i]);
    }

    raft::SelectableList list1;
    raft::SelectableList list2;
    selector.Select(&list, -1);
    list2.assign(list.begin(), list.end());

    for (int k=0; k<100; k++) {
        selector.Select(&list, -1);
        list1.assign(list.begin(), list.end());
        bool diff = false;
        for (int i=0; i<n; i++) {
            if (list1[i] != list2[i]) {
                diff = true;
                break;
            }
        }
        ASSERT_TRUE(diff);
        list2.assign(list1.begin(), list1.end());
    }

    for (int i=0; i<n; i++) {
        delete chs[i];
    }
}

void take(raft::Channel<int>* ch)
{
    int n;
    usleep(1000 * 10);
    ch->Take(&n);
}

TEST(channel, block)
{
    raft::Channel<int> ch(1);
    muduo::Thread(boost::bind(&take, &ch)).start();
    ch.Put(0);
    ch.Put(1);
}
