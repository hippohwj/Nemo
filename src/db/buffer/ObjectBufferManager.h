#ifndef ARBORETUM_SRC_DB_BUFFER_OBJECTBUFFERMANAGER_H_
#define ARBORETUM_SRC_DB_BUFFER_OBJECTBUFFERMANAGER_H_

#include "BufferManager.h"
#include "Common.h"
#include "ITable.h"

namespace arboretum {

class ARDB;
class Stats;

class ObjectBufferManager : public BufferManager {
 public:
  explicit ObjectBufferManager(ARDB *db);
  ObjectBufferManager(ARDB *db, size_t buffer_sz);

  static void PeriodicEviction(ObjectBufferManager * buff_mgr, size_t thd_id);
  void StartEvictThds();
  void StopEvictThds();
  bool ReachEvictionThreshold();

  ITuple * AllocTuple(size_t tuple_sz, OID tbl_id, OID tid);
  void AdmitTuple(ITuple* tuple);
  static ITuple *AccessTuple(SharedPtr &ptr, uint64_t expected_key);
  ITuple * LoadFromStorage(OID tbl_id, SearchKey key, char * loaded_data = nullptr, size_t sz = 0);
  ITuple ** LoadRangeFromStorage(OID tbl_id, SearchKey low_key, SearchKey high_key, size_t& cnt);
  static void FinishAccessTuple(SharedPtr &ptr, bool dirty=false);
  static void FinishAccessTuple(ITuple *tuple, bool dirty);
  size_t GetRows() {
    return allocated_.load();
  }
  void AllocSpace(size_t sz) { 
    other_allocated_ += sz; 
    };
  // deallocate internal index nodes
  void DeallocSpace(size_t sz) { 
    other_allocated_ -= sz; 
    batch_cv_.notify_all();
  };
  // deallocate tuples
  void DeallocTuples(size_t tuple_num) {
     allocated_ -= tuple_num; 
     batch_cv_.notify_all();
    };
  size_t GetAllocated() const { 
    return allocated_ + other_allocated_ / g_buf_entry_sz; 
    };

  void PrintUsage() {
    LOG_INFO("Buffer usage: %zu / %zu (allocated_ is %d, other_allocated_ slots is %d, g_buf_entry_sz is %d, g_total_buf_sz is %d, other_allocated_ size is %d)",
       GetAllocated(), num_slots_, allocated_.load(), other_allocated_.load() / g_buf_entry_sz, g_buf_entry_sz, g_total_buf_sz,  other_allocated_.load());
  };

  size_t GetBufferSize() const { return num_slots_; };
  bool IsWarmedUp(uint64_t row_cnt) const {
    auto cnt = allocated_ + other_allocated_ / g_buf_entry_sz;
    bool warmed = cnt >= row_cnt - 1 || cnt >= num_slots_ * 0.9;
    if (g_workingset_partition_num != 0) {
      warmed = warmed || cnt >= (g_workingset_partition_num * g_scan_zipf_partition_sz -1);
    }
    LOG_DEBUG("checking Object Buffer warm up status = %d (allocated_ = %lu, total = %zu)",
              warmed, cnt, num_slots_);
    return warmed;
  };

 public:
  static inline bool CheckFlag(uint32_t state, uint64_t flag) { return (state & BUF_FLAG_MASK) & flag; }
  static inline uint32_t ClearFlag(uint32_t state, uint64_t flag) { state &= ~flag; return state; }
  static inline uint32_t SetFlag(uint32_t state, uint64_t flag) { state |= flag;
    return state;}
  static inline uint32_t GetPinCount(uint32_t state) { return BUF_STATE_GET_REFCOUNT(state); }
  static inline uint32_t GetUsageCount(uint32_t state) { return BUF_STATE_GET_USAGECOUNT(state); }
  static inline void SetUsageCount(ITuple *row, size_t cnt);
  static inline bool IsPinned(uint32_t state) { return GetPinCount(state) > 0; }
  static inline void Pin(ITuple * row);
  static inline void UnPin(ITuple * row);


  // // expect to find ref bit as 1 and set it to 0, otherwise return 0
  // static bool ClockSweep(std::atomic<uint8_t> &refbit) {
  //   uint8_t expected = 1;
  //   return refbit.compare_exchange_strong(expected, 0, std::memory_order_acq_rel);
  // }
  // when access the tuple, set ref bit to 1 if it is 0 now
  // static bool TupleAccess(std::atomic<uint8_t> &refbit) {
  //   uint8_t expected = 0;
  //   return refbit.compare_exchange_strong(expected, 1, std::memory_order_acq_rel);
  // }

  // batch eviction methods

  // expect to find ref bit as 1 and set it to 0
  static bool ClockSweep(ITuple *row) {
    return row->UnsetRefBit();
  }

  // when access the tuple, set ref bit to 1 if it is 0 now
  static bool TupleAccess(ITuple *row) {
    return row->SetRefBit();
  }





 private:
  bool BatchTraverseEvict();
  void RecordEvict();
  void BatchEvict(size_t cnt, volatile bool &is_done);
  void EvictAndLoad(SearchKey &load_key, ITuple *tuple, bool load = false, char * loaded_data = nullptr, size_t sz = 0);

  size_t ReserveSlots(size_t num_slots) {
    return allocated_.fetch_add(num_slots) + num_slots + other_allocated_ / g_buf_entry_sz;
  };

  bool HasAvailableSlots() { return (allocated_ + other_allocated_ / g_buf_entry_sz) <= num_slots_; };

  bool WaitAvailableSlots(); 



  ARDB *db_{nullptr};
  ITuple *evict_hand_{nullptr};
  ITuple *evict_tail_{nullptr};
  size_t num_slots_{0};
  std::atomic<size_t> allocated_{0};
  std::atomic<size_t> other_allocated_{0};
  std::mutex latch_;
  std::vector<std::thread> evict_thds_;

  std::mutex batch_latch_;
  std::condition_variable batch_cv_;
  
};

}

#endif //ARBORETUM_SRC_DB_BUFFER_OBJECTBUFFERMANAGER_H_
