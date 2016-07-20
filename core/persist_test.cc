/*
 * testPersist.cc
 *
 *  Created on: Jan 3, 2014
 *      Author: fan
 */

#include <unistd.h>
#include <sys/types.h>
#include <fcntl.h>
#include <gtest/gtest.h>
#include <raft/core/persist.h>
#include <raft/core/options.h>

using namespace raft;

class TestPersist
{

};

TEST(logfile, indexName)
{
  {
    LogFile logFile(0);
    ASSERT_EQ(logFile.IndexName, "00000000000000000000");
  }
  {
    LogFile logFile(0xFFFFFFFFFFFFFFFF);
    ASSERT_EQ(logFile.IndexName, "18446744073709551615");
  }
}

TEST(logfile, startIndex)
{
  {
    LogFile logFile("00000000000000000000");
    ASSERT_EQ(logFile.StartIndex, static_cast<uint64_t>(0));
  }
  {
    LogFile logFile("18446744073709551615");
    ASSERT_EQ(logFile.StartIndex, 0xFFFFFFFFFFFFFFFF);
  }
}

TEST(logfile, logName)
{
  LogFile logFile(0);
  ASSERT_EQ(logFile.LogName, "00000000000000000000.log");
}

TEST(fs, pread)
{
  ::system("rm -f pread.dat");
  int fd = ::open("pread.dat", O_CREAT | O_TRUNC | O_RDWR);
  ASSERT_TRUE(fd > 0);
  for (uint32_t i=0; i<10; i++) {
    ASSERT_EQ(raft::fs::Write(fd, &i, sizeof(i)), 4);
  }

  for (uint32_t i=0; i<10; i++) {
    uint32_t n;
    ASSERT_EQ(raft::fs::Pread(fd, &n, sizeof(n), sizeof(n) * i), 4);
    ASSERT_EQ(n, i);
  }
  ::close(fd);
  ::system("rm -f pread.dat");
}

TEST(logPersist, init)
{
  ::system("rm -fr testdir && mkdir testdir");
  Options options;
  options.RaftLogDir = "testdir";
  LogPersist persist(&options);
  persist.Init();
  ASSERT_EQ(persist.NextIndex(), static_cast<uint64_t>(0));
  ::system("rm -fr testdir");
}
