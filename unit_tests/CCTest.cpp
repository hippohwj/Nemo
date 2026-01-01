//
// Created by Zhihan Guo on 4/15/23.
//

#include <thread>
#include <algorithm>
#include "gtest/gtest.h"

#include "CCManager.h"

using namespace arboretum;

void ExclusiveLatchTask(std::atomic<uint32_t> * test_latch, int * counter, int tid) {
  while (CCManager::AcquireLock(*test_latch, true) != OK) continue;
  LOG_DEBUG("thd %d hold ex lock", tid);
  for (int i = 0; i < 100000; i++) {
    (*counter)++;
  }
  LOG_DEBUG("thd %d release ex lock", tid);
  CCManager::ReleaseLock(*test_latch, UPDATE);
}

void SharedLatchTask(std::atomic<uint32_t> * test_latch, const int * counter, int tid) {
  while (CCManager::AcquireLock(*test_latch, false) != OK) continue;
  LOG_DEBUG("thd %d hold sh latch", tid);
  int total;
  for (int i = 0; i < 100000; i++) {
    total += *counter;
  }
  LOG_DEBUG("thd %d release sh latch", tid);
  CCManager::ReleaseLock(*test_latch, READ);
}

TEST(CCTest, MixTest) {
  std::atomic<uint32_t> test_lock{0};
  int counter = 0;
  std::vector<std::thread> threads;
  for (int i = 0; i < 10; i++) {
    threads.emplace_back(ExclusiveLatchTask, &test_lock, &counter, i);
    threads.emplace_back(SharedLatchTask, &test_lock, &counter, i);
  }
  std::for_each(threads.begin(),threads.end(), std::mem_fn(&std::thread::join));
  EXPECT_EQ(test_lock.load(), 0);
}

TEST(CCTest, WriteTest) {
  std::atomic<uint32_t> test_lock{0};
  int counter = 0;
  std::vector<std::thread> threads;
  for (int i = 0; i < 10; i++) {
    threads.emplace_back(ExclusiveLatchTask, &test_lock, &counter, i);
  }
  std::for_each(threads.begin(),threads.end(), std::mem_fn(&std::thread::join));
  EXPECT_EQ(counter, 100000 * 10);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  ::testing::GTEST_FLAG(filter) = "CCTest.*";
  return RUN_ALL_TESTS();
}