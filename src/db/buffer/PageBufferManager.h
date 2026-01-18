#ifndef ARBORETUM_SRC_DB_BUFFER_PAGEBUFFERMANAGER_H_
#define ARBORETUM_SRC_DB_BUFFER_PAGEBUFFERMANAGER_H_

#include "BufferManager.h"
#include "common/Common.h"
#include "local/IPage.h"
#include "local/ITuple.h"

#define PAGEBUF_MAX_NUM_SLOTS ((OID) -1) // upper limit on the parameter can be
#define PAGEBUF_INVALID_BUFID UINT32_MAX

namespace arboretum {

class ARDB;
class ITable;

class PageBufferManager : public BufferManager {
 public:
  struct BucketNode {
    PageTag tag{};
    BucketNode *next{nullptr};
    BucketNode *prev{nullptr};
    volatile OID buf_id{PAGEBUF_INVALID_BUFID};
  };

  struct BufferDesc {
    OID buf_id;      /* buffer's index number (from 0) */
    std::atomic<uint32_t>buf_state_{0};    /* 32-bit, containing 10-bit flags, 18-bit reference count and 4-bit usage count */
    BufferDesc *free_next_;          /* link in freelist chain */
    BufferDesc *clock_next_;         /* link in evict clock */
    RWLock content_lock_;      /* to lock access to buffer contents */
    BucketNode *bucket_node_{nullptr};
    inline bool CheckFlag(uint64_t flag) { return (buf_state_ & BUF_FLAG_MASK) & flag; }
    inline void ClearFlag(uint64_t flag) { buf_state_ &= ~flag; }
    inline void SetFlag(uint64_t flag) { buf_state_ |= flag; }
    inline uint32_t GetPinCount() { return BUF_STATE_GET_REFCOUNT(buf_state_); }
    inline uint32_t GetUsageCount() { return BUF_STATE_GET_USAGECOUNT(buf_state_); }
    inline bool IsPinned() { return GetPinCount() > 0; }
    void Pin(uint64_t weight = 1, bool reset = false);
    void UnPin(bool dirty);
  };
  typedef struct BufferDesc BufferDesc;

  static size_t CalculateNumSlots(size_t total_sz);
  explicit PageBufferManager(ARDB *db);
  ~PageBufferManager();
  ITuple *AccessTuple(ITable * tbl, SharedPtr &ref, uint64_t expected_key, bool *is_miss = nullptr);
  void FinishAccessTuple(SharedPtr &ref, bool dirty);
  BasePage *AllocNewPage(OID table_id, OID page_id);
  bool IsPageResident(OID table_id, OID page_id);
  BasePage *AccessPage(OID table_id, OID page_id, bool *is_miss = nullptr);
  BasePage *AccessPage(OID buf_id) { return &(pool_[buf_id]); };
  void FinishAccessPage(BasePage *page, bool dirty);
  void LockEX(BasePage *page) { buf_descs_[page->header_.buf_id].content_lock_.LockEX(); };
  void LockSH(BasePage *page) { buf_descs_[page->header_.buf_id].content_lock_.LockSH(); };
  void UnLock(BasePage *page) { buf_descs_[page->header_.buf_id].content_lock_.Unlock(); };

  uint32_t GetPinCount(BasePage *page) { return buf_descs_[page->header_.buf_id].GetPinCount(); };
  uint32_t GetUsageCount(BasePage *page) { return buf_descs_[page->header_.buf_id].GetUsageCount(); };
  void FlushAll();
  size_t GetAllocated() const { return allocated_.load(); };
  bool IsWarmedUp(uint64_t pg_cnt, bool verbose=false) {
    auto cnt = std::min(pg_cnt, g_pagebuf_num_slots) - 5;
    bool warmed = allocated_ >= cnt;
    if (verbose) {
      LOG_DEBUG("checking Page Buffer warm up status = %d (allocated_ = %lu, total = %zu)",
                warmed, allocated_.load(), cnt);
    }
    return warmed;
  };

  static void ExecuteFlush(PageBufferManager * pbm, int thd_id, int num_thds, size_t cnt) {
    pbm->ParallelFlushAll(thd_id, num_thds, cnt);
  };
  void ParallelFlushAll(int thd_id, int num_thds, size_t cnt);
  size_t GetIdxPageNum() {
    return idx_allocated_.load(); 
  }

  BufferDesc *evict_clock_hand_;
  BufferDesc *evict_clock_tail_;

  // another circle for idx page
  BufferDesc *idx_evict_clock_hand_;
  BufferDesc *idx_evict_clock_tail_;


 private:
  void FlushAndLoad(OID buf_id, PageTag &tag, bool load_from_storage, bool use_query = false);
  OID LookUpBufferId(OID bucket_id, OID table_id, OID page_id);
  OID FindBufDescToEvict(bool for_idx_page);
  OID FindEmptyBufDesc(bool is_idx_pg);
  RC FindBufToEvictFrom(OID& page_to_evict, bool for_idx_page, bool from_idx_clock = false);
  
  static BucketNode * AllocNewBucketNode();
  void InsertIntoBucket(OID bucket_id, BucketNode * node);
  OID CalBucketId(OID table_id, OID page_id) const {
    uint32_t hash_val = table_id ^ page_id;
    return hash_val % bucket_sz_;
  }

  // hash map: tag -> buffer id
  BucketNode **bucket_hdrs_;
  RWLock *bucket_latches_;
  uint32_t bucket_sz_;
  BucketNode * bucket_nodes_;
  // buffer descriptor
  BufferDesc *buf_descs_;
  // protecting access to desc
  std::mutex *desc_latches_;
  BufferDesc *free_head_;
  // free_desc_mutex_: provides mutual exclusion for operations that
  // access the buffer free list or select buffers for replacement
  // BufferDesc *evict_clock_hand_;
  // BufferDesc *evict_clock_tail_;
  std::atomic<size_t> allocated_{0};
  std::atomic<size_t> data_pg_clock_allocated_{0};
  std::atomic<size_t> idx_pg_clock_allocated_{0};
  std::atomic<size_t> idx_allocated_{0};
  std::atomic<size_t> flush_tracker_{0};
  std::mutex free_desc_mutex_;
  // buffer pool
  BasePage *pool_;
  // DB
  ARDB *db_{nullptr};
};

}

#endif //ARBORETUM_SRC_DB_BUFFER_PAGEBUFFERMANAGER_H_
