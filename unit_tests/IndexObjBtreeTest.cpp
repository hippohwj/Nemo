//
// Created by Zhihan Guo on 4/4/23.
//

#include "gtest/gtest.h"

#include "db/ARDB.h"
#include "db/access/IndexBTreeObject.h"
#include "ITuple.h"


using namespace arboretum;

void PrintLatencyDist(std::vector<uint64_t>& insert_latency) {
  std::sort(insert_latency.begin(), insert_latency.end());
  auto total_inserts = insert_latency.size();
  LOG_INFO("Finish operation on %zu rows", total_inserts);
  LOG_INFO("latency   0 percentage: %.2f us", NANO_TO_US(insert_latency[0]));
  LOG_INFO("latency  25 percentage: %.2f us", NANO_TO_US(insert_latency[total_inserts * 0.25]));
  LOG_INFO("latency  50 percentage: %.2f us", NANO_TO_US(insert_latency[total_inserts * 0.50]));
  LOG_INFO("latency  75 percentage: %.2f us", NANO_TO_US(insert_latency[total_inserts * 0.75]));
  LOG_INFO("latency  99 percentage: %.2f us", NANO_TO_US(insert_latency[total_inserts * 0.99]));
  LOG_INFO("latency 100 percentage: %.2f us", NANO_TO_US(insert_latency[total_inserts - 1]));
}

void ExclusiveLatchTask(RWLock * test_latch, int * counter, int tid) {
  test_latch->LockEX();
  for (int i = 0; i < 100000; i++) {
    (*counter)++;
  }
  usleep(1000);
  LOG_DEBUG("thread %d held ex lock", tid)
  M_ASSERT(test_latch->IsEXLocked(),"fail exclusive latch task");
  test_latch->Unlock();
}

void SharedLatchTask(RWLock * test_latch, const int * counter, int tid) {
  test_latch->LockSH();
  int total;
  for (int i = 0; i < 100000; i++) {
    total += *counter;
  }
  usleep(1000);
  LOG_DEBUG("thread %d held sh lock", tid)
  M_ASSERT(test_latch->status > 0 && !(test_latch->status & 1),
           "fail shared latch task");
  M_ASSERT(!test_latch->IsEXLocked(),"fail exclusive latch task");
  test_latch->Unlock();
}

TEST(IndexObjBtreeTest, RWLockTest) {
  RWLock test_latch;
  int counter = 0;
  std::vector<std::thread> threads;
  int num_thds = 100;
  for (int i = 0; i < num_thds; i++) {
    threads.emplace_back(ExclusiveLatchTask, &test_latch, &counter, i);
    if (i % 10 == 0) {
      threads.emplace_back(SharedLatchTask, &test_latch, &counter, i);
    }
  }
  std::for_each(threads.begin(),threads.end(), std::mem_fn(&std::thread::join));
  EXPECT_EQ(test_latch.status, 0);
  EXPECT_EQ(counter, 100000 * num_thds);
}

TEST(IndexObjBtreeTest, InitTest) {
  IndexBTreeObject index;
  index.AddCoveringCol(0);
  g_idx_btree_fanout = 5;
  std::vector<uint64_t> latency;
  uint64_t total_latency = 0;
  for (int i = 0; i < 100; i++) {
    // LOG_DEBUG("Inserting key %d", i);
    auto starttime = GetSystemClock();
    index.Insert(SearchKey(i), reinterpret_cast<ITuple *>(i));
    auto elapsed = GetSystemClock() - starttime;
    latency.push_back(elapsed);
    total_latency += elapsed;
  }
  index.PrintTree();
  PrintLatencyDist(latency);
  LOG_INFO("Single thread insert throughput = %.f ops / s",
           100.0 / total_latency * 1000000000);
  for (int i = 0; i < 100; i++) {
    auto tags = index.Search(SearchKey(i));
    EXPECT_EQ(i, reinterpret_cast<uint64_t>(tags[0].Get()));
    DEALLOC(tags);
  }
}

void SearchTask(uint64_t * search_latency, IndexBTreeObject * index, int thd_id,
                int cnt, size_t tbl_sz) {
  arboretum::ARDB::InitRand(thd_id);
  for (int i = 0; i < cnt; i++) {
    auto starttime = GetSystemClock();
    uint64_t key = ARDB::RandDouble() * tbl_sz;
    auto tags = index->Search(SearchKey(key));
    search_latency[i] = GetSystemClock() - starttime;
    if (key != reinterpret_cast<uint64_t>(tags[0].Get())) {
      LOG_ERROR("search result for %lu does not match (%lu) on thd-%d",
                key, reinterpret_cast<uint64_t>(tags[0].Get()), thd_id);
    }
    EXPECT_EQ(key, reinterpret_cast<uint64_t>(tags[0].Get()));
    DEALLOC(tags);
  }
}

TEST(IndexObjBtreeTest, ConcurrentSearchTest) {
  IndexBTreeObject index;
  index.AddCoveringCol(0);
  g_idx_btree_fanout = 5;
  int table_sz = 100;
  for (int i = 0; i < table_sz; i++) {
    index.Insert(SearchKey(i), reinterpret_cast<ITuple *>(i));
  }
  // index.PrintTree();
  int num_threads = 8;
  int num_accesses = 100;
  std::vector<uint64_t *> latency;
  std::vector<std::thread> threads;
  for (int i = 0; i < num_threads; i++) {
    latency.push_back(NEW_SZ(uint64_t, num_accesses));
  }
  for (int i = 0; i < num_threads; i++) {
    threads.emplace_back(SearchTask, latency[i], &index, i,
                         num_accesses, table_sz);
  }
  std::for_each(threads.begin(),threads.end(), std::mem_fn(&std::thread::join));
  std::vector<uint64_t> all;
  uint64_t total_latency = 0;
  for (int i = 0; i < num_threads; i++) {
    for (int j = 0; j < num_accesses; j++) {
      all.push_back(latency[i][j]);
      total_latency += latency[i][j];
    }
  }
  PrintLatencyDist(all);
  LOG_INFO("Concurrent search (8 thds) throughput = %.f ops / s",
           100.0 * 8 / total_latency * 1000000000);
}

void InsertTask(uint64_t * search_latency, IndexBTreeObject * index, int thd_id,
                int cnt, size_t tbl_sz, int num_thds) {
  auto accesses = tbl_sz / num_thds;
  for (int i = 0; i < accesses; i++) {
    auto starttime = GetSystemClock();
    uint64_t key = thd_id * accesses + i;
    index->Insert(SearchKey(key), reinterpret_cast<ITuple *>(key));
    search_latency[i] = GetSystemClock() - starttime;
    LOG_DEBUG("thread-%d inserted key %lu", thd_id, key);
  }
}

TEST(IndexObjBtreeTest, ConcurrentInsertTest) {
  IndexBTreeObject index;
  index.AddCoveringCol(0);
  g_idx_btree_fanout = 5;
  int table_sz = 100;
  int num_threads = 8;
  int num_accesses = 100;
  std::vector<uint64_t *> latency;
  std::vector<std::thread> threads;
  for (int i = 0; i < num_threads; i++) {
    latency.push_back(NEW_SZ(uint64_t, num_accesses));
  }
  for (int i = 0; i < num_threads; i++) {
    threads.emplace_back(InsertTask, latency[i], &index, i,
                         num_accesses, table_sz, num_threads);
  }
  std::for_each(threads.begin(),threads.end(), std::mem_fn(&std::thread::join));
  index.PrintTree();
  std::vector<uint64_t> all;
  uint64_t total_latency = 0;
  for (int i = 0; i < num_threads; i++) {
    for (int j = 0; j < num_accesses; j++) {
      all.push_back(latency[i][j]);
      total_latency += latency[i][j];
    }
  }
  PrintLatencyDist(all);
  LOG_INFO("Concurrent insert (8 thds) throughput = %.f ops / s",
           100.0 * 8 / total_latency * 1000000000);
}

TEST(IndexObjBtreeTest, DeleteTest) {
  IndexBTreeObject index;
  index.AddCoveringCol(0);
  g_idx_btree_fanout = 5;
  std::vector<uint64_t> latency;
  std::vector<ITuple *> tuples;
  uint64_t total_latency = 0;
  for (int i = 0; i < 100; i++) {
    // LOG_DEBUG("Inserting key %d", i);
    tuples.push_back(NEW(ITuple)(0, i));
    index.Insert(SearchKey(i), tuples[i]);
  }
  index.PrintTree();
  for (int i = 9; i <= 20; i++) {
    //LOG_DEBUG("Deleting key %d", i);
    auto starttime = GetSystemClock();
    index.Delete(SearchKey(i), tuples[i]);
    auto elapsed = GetSystemClock() - starttime;
    latency.push_back(elapsed);
    total_latency += elapsed;
  }
  // check split
  for (int i = 15; i <= 20; i++) {
    LOG_DEBUG("Re-inserting key %d", i);
    index.Insert(SearchKey(i), tuples[i]);
  }
  for (int i = 15; i < 20; i++) {
    auto tags = index.Search(SearchKey(i));
    EXPECT_EQ(i, reinterpret_cast<ITuple *>(tags[0].Get())->GetTID());
    DEALLOC(tags);
  }
  index.PrintTree();
  PrintLatencyDist(latency);
  LOG_INFO("Single thread delete throughput = %.f ops / s",
           100.0 / total_latency * 1000000000);
}

TEST(IndexObjBtreeTest, DeleteStressTest) {
  IndexBTreeObject index;
  index.AddCoveringCol(0);
  g_idx_btree_fanout = 5;
  std::vector<uint64_t> latency;
  std::vector<ITuple *> tuples;
  uint64_t total_latency = 0;
  for (int i = 0; i < 90; i++) {
    // LOG_DEBUG("Inserting key %d", i);
    tuples.push_back(NEW(ITuple)(0, i));
    index.Insert(SearchKey(i), tuples[i]);
  }
  index.PrintTree();
  for (int i = 9; i <= 35; i++) {
    //LOG_DEBUG("Deleting key %d", i);
    auto starttime = GetSystemClock();
    index.Delete(SearchKey(i), tuples[i]);
    auto elapsed = GetSystemClock() - starttime;
    latency.push_back(elapsed);
    total_latency += elapsed;
  }
  index.PrintTree();
  PrintLatencyDist(latency);
  LOG_INFO("Single thread delete throughput = %.f ops / s",
           100.0 / total_latency * 1000000000);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  ::testing::GTEST_FLAG(filter) = "IndexObjBtreeTest.RWLockTest";
  return RUN_ALL_TESTS();
}