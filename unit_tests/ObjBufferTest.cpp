//
// Created by Zhihan Guo on 4/26/23.
//
#include "gtest/gtest.h"

#include "db/ARDB.h"
#include "db/buffer/ObjectBufferManager.h"

using namespace arboretum;

struct ClockNode {
  ClockNode(uint64_t key, int usage) {

  }
  uint64_t key_{0};
  int usage_{2};
  ClockNode * clock_next_{nullptr};
};

TEST(ObjBufferTest, HitRateTest) {
  size_t buf_sz = 100;
  std::atomic<size_t> allocated;
  allocated = 0;
  std::fstream traces;
  auto fname = "../output/query_traces.out";
  traces.open(fname, std::ios::in);
  LOG_INFO("reading from trace file: %s", fname);
  std::string line;
  ClockNode *evict_hand_ = nullptr;
  ClockNode *evict_tail_ = nullptr;
  std::unordered_map<uint64_t, ClockNode *> map;
  auto hits = 0;
  auto accesses = 0;
  auto actual_hits = 0;
  while (getline(traces, line)) {
    uint64_t key;
    int hit;
    std::string token;
    size_t cnt = 0;
    size_t pos;
    while (line.length() != 0) {
      pos = line.find(',');
      if (pos == std::string::npos)
        pos = line.length();
      token = line.substr(0, pos);
      line.erase(0, pos + 1);
      if (cnt == 0) {
        key = std::stoi(token);
      } else {
        hit = std::stoi(token);
        if (hit == 1) actual_hits++;
      }
      cnt++;
    }
    if (map.find(key) != map.end()) {
      hits++;
      map[key]->usage_++;
    } else {
      if (allocated + 1 > buf_sz) {
        // try to evict
        while (evict_hand_) {
          if (evict_hand_->usage_ == 0) {
            // evict this
            map.erase(evict_hand_->key_);
            ClockRemoveHand(evict_hand_, evict_tail_, allocated);
            break;
          } else {
            evict_hand_->usage_--;
          }
          ClockAdvance(evict_hand_, evict_tail_);
        }
      }
      auto node = new ClockNode(key, 2);
      ClockInsert(evict_hand_, evict_tail_, node);
      allocated++;
      map[key] = node;
    }
    accesses++;
  }
  LOG_INFO("Expected hit rate: %.3f, Actual hit rate: %.3f",
           hits * 1.0 / accesses, actual_hits * 1.0 / accesses);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  ::testing::GTEST_FLAG(filter) = "ObjBufferTest.*";
  return RUN_ALL_TESTS();
}