#include "PageBufferManager.h"
#include "common/Worker.h"
#include "common/Stats.h"
#include "local/ITable.h"
#include "remote/IDataStore.h"
#include "db/ARDB.h"



namespace arboretum {

size_t PageBufferManager::CalculateNumSlots(size_t total_sz) {
  size_t unit_page_sz = 0.5 * sizeof(void *) + 0.5 * sizeof(RWLock) + sizeof(BucketNode) 
  + sizeof(BufferDesc) + sizeof(std::mutex) + sizeof(BasePage);
  LOG_INFO("Unit page size is %zu", unit_page_sz);
  auto num_slots = total_sz / unit_page_sz;
  return num_slots;
}

PageBufferManager::PageBufferManager(ARDB *db) {
  LOG_INFO("Initiating page buffer manager with %zu empty slots. ",
           g_pagebuf_num_slots);
  db_ = db;
  // init buckets
  bucket_sz_ = g_pagebuf_num_slots / 2;
  bucket_hdrs_ =
      (BucketNode **) MemoryAllocator::Alloc(sizeof(void *) * bucket_sz_);
  bucket_latches_ =
      (RWLock *) MemoryAllocator::Alloc(sizeof(RWLock) * bucket_sz_);
  for (size_t i = 0; i < bucket_sz_; i++) {
    bucket_hdrs_[i] = nullptr;
    new(&bucket_latches_[i]) RWLock();
  }
  // init buffer descriptor with PAGE_BUF_SLOTS slots.
  M_ASSERT(g_pagebuf_num_slots < PAGEBUF_MAX_NUM_SLOTS,
           "number of buffer slot exceed max of %u.",
           PAGEBUF_MAX_NUM_SLOTS);
  bucket_nodes_ = (BucketNode *) MemoryAllocator::Alloc(
      sizeof(BucketNode) * g_pagebuf_num_slots);
  buf_descs_ = (BufferDesc *) MemoryAllocator::Alloc(
      sizeof(BufferDesc) * g_pagebuf_num_slots);
  desc_latches_ = (std::mutex *) MemoryAllocator::Alloc(
      sizeof(std::mutex) * g_pagebuf_num_slots);
  M_ASSERT(buf_descs_,
           "fail to allocate space, need to decrease PAGE_BUF_SLOTS; currently expected space: %lu bytes.",
           sizeof(BufferDesc) * g_pagebuf_num_slots);
  free_head_ = nullptr;
  for (int64_t i = g_pagebuf_num_slots - 1; i >= 0; i--) {
    new(&bucket_nodes_[i]) BucketNode;
    bucket_nodes_[i].buf_id = i;
    new(&desc_latches_[i]) std::mutex();
    auto desc = new(&buf_descs_[i]) BufferDesc;
    desc->buf_id = i;
    desc->free_next_ = free_head_;
    free_head_ = desc;
  }
  evict_clock_hand_ = nullptr;
  evict_clock_tail_ = nullptr;
  idx_evict_clock_hand_ = nullptr;
  idx_evict_clock_tail_ = nullptr;

  // init buffer pool
  pool_ = (BasePage *) MemoryAllocator::Alloc(
      sizeof(BasePage) * g_pagebuf_num_slots);
  for (size_t i = 0; i < g_pagebuf_num_slots; i++) {
    new(&pool_[i]) BasePage();
    pool_[i].header_.buf_id = i;
  }
}

PageBufferManager::~PageBufferManager() {
  MemoryAllocator::Dealloc(bucket_latches_);
  MemoryAllocator::Dealloc(bucket_nodes_);
  MemoryAllocator::Dealloc(bucket_hdrs_);
  MemoryAllocator::Dealloc(buf_descs_);
  MemoryAllocator::Dealloc(desc_latches_);
  MemoryAllocator::Dealloc(pool_);
}

ITuple *PageBufferManager::AccessTuple(ITable * table, SharedPtr &ref, uint64_t expected_key, bool *is_miss) {
  auto item_tag = ref.ReadAsPageTag();
  auto data_pg = AccessPage(table->GetTableId(), item_tag.pg_id_, is_miss);
  auto tuple = reinterpret_cast<ITuple *>(data_pg->GetData(item_tag.first_));
  // use tuple's buf state to store current data page buf_id
  tuple->buf_state_ = data_pg->header_.buf_id;
  ref.InitAsGeneralPtr(tuple);

  if (tuple->GetTID() != expected_key) {
    // LOG_DEBUG("Error: expecting key %lu, got key %u from page %u offset %u!",
              // expected_key, tuple->GetTID(), item_tag.pg_id_, item_tag.first_);
    // LOG_ERROR("expecting key %lu, got key %u from page %u offset %u!",
                // expected_key, tuple->GetTID(), item_tag.pg_id_, item_tag.first_);
  }
  return tuple;
}

void PageBufferManager::FinishAccessTuple(SharedPtr &ref, bool dirty) {
  auto tuple = reinterpret_cast<ITuple *>(ref.Get());
  auto data_pg = AccessPage(tuple->buf_state_);
  FinishAccessPage(data_pg, dirty);
}

BasePage *PageBufferManager::AllocNewPage(OID table_id, OID page_id) {
  auto bucket_id = CalBucketId(table_id, page_id);
  // create a new bucket node and avoid others checking the bucket node until ready.
  // find an empty slot
  auto bucket_node = AllocNewBucketNode();
  bucket_node->tag = {table_id, page_id};
  bucket_latches_[bucket_id].LockEX();
  free_desc_mutex_.lock();
  // book a buffer desc and hold the lock to prevent others accessing it before ready
  bool is_idx_page = table_id >= (1UL << 28); 
  OID buf_id = FindEmptyBufDesc(is_idx_page);
  // acquire free_desc_mutex before bucket latch to avoid deadlock.
  bucket_node->buf_id = buf_id;
  buf_descs_[buf_id].bucket_node_ = bucket_node;
  InsertIntoBucket(bucket_id, bucket_node);
  bucket_latches_[bucket_id].Unlock();
  // flush dirty page
  FlushAndLoad(buf_id, bucket_node->tag, false);
  // record idx page proportion for admitted page
  if (table_id >= (1UL << 28)) {
    idx_allocated_++;
    if (Worker::GetThdId() == 0)
      g_stats->db_stats_[Worker::GetThdId()].set_bufferd_idx_pgs_(idx_allocated_);
  }
  pool_[buf_id].header_.buf_id = buf_id;
  pool_[buf_id].header_.tag = bucket_node->tag;
  buf_descs_[buf_id].Pin(table_id >= (1UL << 28) ? 2 : 1, true);
  desc_latches_[buf_id].unlock();
  return &pool_[buf_id];
}

bool PageBufferManager::IsPageResident(OID table_id, OID page_id) {
  auto bucket_id = CalBucketId(table_id, page_id);
  auto buf_id = LookUpBufferId(bucket_id, table_id, page_id);
  bool found = buf_id != PAGEBUF_INVALID_BUFID;
  bucket_latches_[bucket_id].Unlock();
  return found;
}

BasePage *
PageBufferManager::AccessPage(OID table_id, OID page_id, bool *is_miss) {
  // LOG_DEBUG("Finish access page tbl-%u pg-%u", table_id, page_id);
  if (g_warmup_finished) {
    g_stats->db_stats_[Worker::GetThdId()].incr_accesses_(1);
    if (table_id >= (1UL << 28))
      g_stats->db_stats_[Worker::GetThdId()].incr_idx_accesses_(1);
  }
  auto bucket_id = CalBucketId(table_id, page_id);
  auto buf_id = LookUpBufferId(bucket_id, table_id, page_id);
  if (buf_id != PAGEBUF_INVALID_BUFID) {
    // found the page, pin it
    auto desc = &buf_descs_[buf_id];
    desc_latches_[buf_id].lock();
    desc->Pin(table_id >= (1UL << 28) ? 2 : 1, false);
    desc_latches_[buf_id].unlock();
    bucket_latches_[bucket_id].Unlock();
    // if (is_miss && (*is_miss)) {
    //   *is_miss = false;
    // }
    return &pool_[buf_id];
  }
  // not found, try to load from storage
  auto bucket_node = AllocNewBucketNode();
  bucket_node->tag = {table_id, page_id};
  free_desc_mutex_.lock();
  bool is_idx_page = table_id >= (1UL << 28); 
  buf_id = FindEmptyBufDesc(is_idx_page);
  // insert into bucket while desc lock is still held
  bucket_node->buf_id = buf_id;
  buf_descs_[buf_id].bucket_node_ = bucket_node;
  InsertIntoBucket(bucket_id, bucket_node);
  bucket_latches_[bucket_id].Unlock();
  // load from storage or flush current
  if (is_miss) {
    bool use_query = true;
    if (is_miss != nullptr && *is_miss) {
        // if is_miss is true, it means the page is not the first missing page of current scan, so we do not need to use query to load the page 
        use_query = false;
    } 
    FlushAndLoad(buf_id, bucket_node->tag, true, use_query);
  } else {
    FlushAndLoad(buf_id, bucket_node->tag, true, false);
  }

  pool_[buf_id].header_.buf_id = buf_id;
  pool_[buf_id].header_.tag = bucket_node->tag;
  buf_descs_[buf_id].Pin(table_id >= (1UL << 28) ? 2 : 1, true);
  // record idx page proportion for admitted page
  desc_latches_[buf_id].unlock();
  // count towards a miss
  if (g_warmup_finished) {
    g_stats->db_stats_[Worker::GetThdId()].incr_misses_(1);
    if (table_id >= (1UL << 28))
      g_stats->db_stats_[Worker::GetThdId()].incr_idx_misses_(1);
  }
  if (is_miss && (*is_miss == false)) {
    *is_miss = true;
  }
  return &pool_[buf_id];
}

OID PageBufferManager::FindEmptyBufDesc(bool is_idx_pg) {
  BufferDesc *desc;
  OID buf_id;
  // protected by global latch, allocated will not change during the time
  if (allocated_ + 1 > g_pagebuf_num_slots) {
    // evict
    buf_id = FindBufDescToEvict(is_idx_pg);
    // need to make sure current slot is always pinned to avoid being evicted by others
    free_desc_mutex_.unlock();
    // LOG_DEBUG("XXX Find Buf to evict for bufid %d", buf_id);

  } else {
    desc = free_head_;
    free_head_ = desc->free_next_;
    buf_id = desc->buf_id;
    desc_latches_[buf_id].lock();
    desc->buf_state_ += BUF_REFCOUNT_ONE; // must pin now to avoid being evicted
    if (!(g_retain_idx_page && is_idx_pg)) {
      ClockInsert(evict_clock_hand_, evict_clock_tail_, desc);
      // LOG_DEBUG("insert page into data page clock with idx %d", buf_id);
      data_pg_clock_allocated_++;
    } else {
      ClockInsert(idx_evict_clock_hand_, idx_evict_clock_tail_, desc);
      // LOG_DEBUG("insert page into idx page clock with idx %d", buf_id);
      idx_pg_clock_allocated_++;
    }
    allocated_++;
    free_desc_mutex_.unlock();
  }
  return buf_id;
}

void PageBufferManager::FinishAccessPage(BasePage *page, bool dirty) {
  // LOG_DEBUG("Finish access page tbl-%u pg-%u", page->GetTableID(), page->GetPageID());
  auto buf_id = page->header_.buf_id;
  auto desc = &buf_descs_[buf_id];
  desc_latches_[buf_id].lock();
  desc->UnPin(dirty);
  desc_latches_[buf_id].unlock();
}

OID PageBufferManager::LookUpBufferId(OID bucket_id, OID table_id, OID page_id) {
  OID buf_id = PAGEBUF_INVALID_BUFID;
  bucket_latches_[bucket_id].LockEX();
  auto bucket_node = bucket_hdrs_[bucket_id];
  while (bucket_node) {
    // remove invalid node if encountered any
    if (bucket_node->buf_id == PAGEBUF_INVALID_BUFID) {
      auto next = bucket_node->next;
      if (bucket_node->prev) {
        bucket_node->prev->next = bucket_node->next;
      } else {
        bucket_hdrs_[bucket_id] = bucket_node->next;
      }
      if (bucket_node->next) {
        bucket_node->next->prev = bucket_node->prev;
      }
      MemoryAllocator::Dealloc(bucket_node);
      bucket_node = next;
      continue;
    }
    if (bucket_node->tag.first_ == table_id && bucket_node->tag.pg_id_ == page_id) {
      buf_id = bucket_node->buf_id;
      break;
    }
    bucket_node = bucket_node->next;
  }
  return buf_id;
}

PageBufferManager::BucketNode * PageBufferManager::AllocNewBucketNode() {
  auto node = (BucketNode *) MemoryAllocator::Alloc(sizeof(BucketNode));
  node->buf_id = PAGEBUF_INVALID_BUFID;
  node->next = nullptr;
  node->prev = nullptr;
  return node;
}

void PageBufferManager::InsertIntoBucket(OID bucket_id, BucketNode * node) {
  node->next = bucket_hdrs_[bucket_id];
  node->prev = nullptr;
  if (node->next)
    node->next->prev = node;
  bucket_hdrs_[bucket_id] = node;
}


RC PageBufferManager::FindBufToEvictFrom(OID& buf_to_evict, bool for_idx_page, bool from_idx_clock) {
  BufferDesc ** clock_hand_ptr = from_idx_clock ? &idx_evict_clock_hand_ : &evict_clock_hand_;
  BufferDesc ** clock_tail_ptr = from_idx_clock ? &idx_evict_clock_tail_ : &evict_clock_tail_;

  // auto cnt = (!g_retain_idx_page)? allocated_.load() : (from_idx_clock? idx_allocated_.load(): (allocated_.load() - idx_allocated_.load()));
  auto cnt = (!g_retain_idx_page)? allocated_.load() : (from_idx_clock? idx_pg_clock_allocated_.load(): data_pg_clock_allocated_.load());
  auto iterations = 0;
  // if g_retain_idx_page is set, first find page from data page clock, then from idx page clock
  while (*clock_hand_ptr) {
    if (cnt == 0) {
      iterations++;
      // cnt = (!g_retain_idx_page)? allocated_.load() : (from_idx_clock? idx_allocated_.load(): (allocated_.load() - idx_allocated_.load()));
      cnt = (!g_retain_idx_page)? allocated_.load() : (from_idx_clock? idx_pg_clock_allocated_.load(): data_pg_clock_allocated_.load());
      if (iterations >= BM_MAX_USAGE_COUNT) {
        LOG_INFO("All pinned. please increase buffer size, is idx clock? %s", (from_idx_clock?"true": "false"));
        return RC::ERROR;
      }
    }
    cnt--;
    OID buf_id = (*clock_hand_ptr)->buf_id;
    if ((*clock_hand_ptr)->IsPinned()) {
      // cannot evict pinned
      ClockAdvance((*clock_hand_ptr), (*clock_tail_ptr));
      // LOG_INFO("Cannot evict (pinned page) with buf id %d, pin count %d, usage count %d", buf_id, (*clock_hand_ptr)->GetPinCount(), (*clock_hand_ptr)->GetUsageCount());
      continue;
    }
    // check usage count
    if ((*clock_hand_ptr)->GetUsageCount() != 0) {
      // decrease usage count
      (*clock_hand_ptr)->buf_state_.fetch_sub(BUF_USAGECOUNT_ONE);
      ClockAdvance((*clock_hand_ptr), (*clock_tail_ptr));
      // LOG_INFO("Cannot evict (usage count) with buf id %d, pin count %d, usage count %d, is idx clock? %s", buf_id, (*clock_hand_ptr)->GetPinCount(), (*clock_hand_ptr)->GetUsageCount(), (from_idx_clock?"true": "false"));
      continue;
    }
    // book the slot so that no one else can access it
    if (!desc_latches_[buf_id].try_lock()) {
      ClockAdvance((*clock_hand_ptr), (*clock_tail_ptr));
      continue;
    }
    // remove evicted bucket node from bucket list
    auto desc = (*clock_hand_ptr);
    if (g_retain_idx_page && (!from_idx_clock) && for_idx_page) {
      // move evicted buf from data page clock to idx page clock
      ClockRemoveHand((*clock_hand_ptr), (*clock_tail_ptr));
      data_pg_clock_allocated_--;
      ClockInsert(idx_evict_clock_hand_, idx_evict_clock_tail_, desc);
      idx_pg_clock_allocated_++;
      // LOG_DEBUG("Move page from data page clock to idx page clock for buf id %d", desc->buf_id);
      // // make sure current node will not be immediately checked by next evictor
      // ClockAdvance(idx_evict_clock_hand_, idx_evict_clock_tail_);
    } else if (g_retain_idx_page && from_idx_clock && (!for_idx_page)) { 
      // move evicted buf from idx page clock to data page clock
      ClockRemoveHand((*clock_hand_ptr), (*clock_tail_ptr));
      idx_pg_clock_allocated_--;
      ClockInsert(evict_clock_hand_, evict_clock_tail_, desc);
      data_pg_clock_allocated_++;
      // LOG_DEBUG("Move page from idx page clock to data page clock for buf id %d", desc->buf_id);
    } else {
      // make sure current node will not be immediately checked by next evictor
      ClockAdvance((*clock_hand_ptr), (*clock_tail_ptr));
    }
    // pin current page immediately to avoid others evict it
    desc->buf_state_ += BUF_REFCOUNT_ONE;
    // LOG_DEBUG("Pin buffer with id %d", desc->buf_id);
    // avoid others accessing the olUnpin a page withd page, but the node will be removed later
    desc->bucket_node_->buf_id = PAGEBUF_INVALID_BUFID;
    // // need to make sure current slot is always pinned to avoid being evicted by others
    // free_desc_mutex_.unlock();
    buf_to_evict = desc->buf_id; 
    return RC::OK;
  }
  LOG_ERROR("Unexpected clock hand: nullptr");
  return RC::ERROR;
}



OID PageBufferManager::FindBufDescToEvict(bool for_idx_page) {
  OID buf_id = 0;
  auto rc = FindBufToEvictFrom(buf_id, for_idx_page, false);
  if (rc == RC::OK) {
    // LOG_DEBUG("find buf to evict from data page clock %d", buf_id);
    return buf_id;
  } else if (g_retain_idx_page) {
    // if g_retain_idx_page is set, first find page from data page clock, then from idx page clock
    auto rc = FindBufToEvictFrom(buf_id, for_idx_page, true);
    if (rc == RC::OK) {
      // LOG_DEBUG("find buf to evict from idx page clock %d", buf_id);
      return buf_id;
    } else {
      LOG_ERROR("increase buffer size");
    }
  } else {
    LOG_ERROR("increase buffer size");
  }

  // auto cnt = allocated_.load();
  // auto iterations = 0;
  // // if g_retain_idx_page is set, first find page from data page clock, then from idx page clock
  // while (evict_clock_hand_) {
  //   if (cnt == 0) {
  //     iterations++;
  //     cnt = allocated_.load();
  //     if (iterations >= BM_MAX_USAGE_COUNT) {
  //       LOG_ERROR("All pinned. please increase buffer size");
  //     }
  //   }
  //   cnt--;
  //   OID buf_id = evict_clock_hand_->buf_id;
  //   if (evict_clock_hand_->IsPinned()) {
  //     // cannot evict pinned
  //     ClockAdvance(evict_clock_hand_, evict_clock_tail_);
  //     continue;
  //   }
  //   // check usage count
  //   if (evict_clock_hand_->GetUsageCount() != 0) {
  //     // decrease usage count
  //     evict_clock_hand_->buf_state_.fetch_sub(BUF_USAGECOUNT_ONE);
  //     ClockAdvance(evict_clock_hand_, evict_clock_tail_);
  //     continue;
  //   }
  //   // book the slot so that no one else can access it
  //   if (!desc_latches_[buf_id].try_lock()) {
  //     ClockAdvance(evict_clock_hand_, evict_clock_tail_);
  //     continue;
  //   }
  //   // if (g_retain_idx_page) {
  //   //   bool is_idx_node = (evict_clock_hand_->bucket_node_->tag.first_) >= (1UL << 28);
  //   //   if (is_idx_node) {
  //   //     // LOG_INFO("skip idx page in clock while enabling g_retain_idx_page");
  //   //     ClockAdvance(evict_clock_hand_, evict_clock_tail_);
  //   //     continue; 
  //   //   }
  //   // }

  //   // remove evicted bucket node from bucket list
  //   auto desc = evict_clock_hand_;
  //   if (g_retain_idx_page && for_idx_page) {
  //     ClockRemoveHand(evict_clock_hand_, evict_clock_tail_);
  //   } else {
  //     // make sure current node will not be immediately checked by next evictor
  //     ClockAdvance(evict_clock_hand_, evict_clock_tail_);
  //   }
  //   // pin current page immediately to avoid others evict it
  //   desc->buf_state_ += BUF_REFCOUNT_ONE;
  //   // avoid others accessing the old page, but the node will be removed later
  //   desc->bucket_node_->buf_id = PAGEBUF_INVALID_BUFID;
  //   // need to make sure current slot is always pinned to avoid being evicted by others
  //   free_desc_mutex_.unlock();
  //   return desc->buf_id;
  // }
  // LOG_ERROR("increase buffer size");
}

void PageBufferManager::FlushAndLoad(OID buf_id, PageTag &tag, bool load_from_storage, bool use_query) {
  auto desc = &buf_descs_[buf_id];
  auto &pg = pool_[buf_id];
  auto pg_start = reinterpret_cast<char *> (&pg);
  // record idx page proportion for evicted page
  if (pg.GetTableID() >= (1UL << 28)) {
    idx_allocated_--;
    if (Worker::GetThdId() == 0)
      g_stats->db_stats_[Worker::GetThdId()].set_bufferd_idx_pgs_(idx_allocated_);
  }
  if (load_from_storage) {
    // LOG_DEBUG("try to load tbl-%u pg-%u from storage to buf slot %u",
    // tag.first_, tag.pg_id_, buf_id);
    std::string data;
    // record idx page proportion for admitted page
    if (tag.first_ >= (1UL << 28)) {
      idx_allocated_++;
      if (Worker::GetThdId() == 0)
        g_stats->db_stats_[Worker::GetThdId()].set_bufferd_idx_pgs_(idx_allocated_);
    }
    auto load_tbl_name = ITable::GetStorageId(tag.first_);
    auto load_key = tag.GetStorageKey();
    if (desc->CheckFlag(BM_DIRTY)) {
      auto store_tbl_name = ITable::GetStorageId(pg.header_.tag.first_);
      auto store_key = pg.header_.tag.GetStorageKey();
      if (store_tbl_name == load_tbl_name) {
        g_data_store->WriteAndRead(store_tbl_name, store_key,
                                   pg_start, sizeof(BasePage),
                                   load_key, data);
      } else {
        g_data_store->Write(store_tbl_name,store_key, pg_start, sizeof(BasePage));
        g_data_store->Read(load_tbl_name, load_key, data);
      }
    } else {
      if (g_warmup_finished) {
        auto starttime = GetSystemClock();
        if (use_query) {
          g_data_store->ReadQuery(load_tbl_name, load_key, data);
        } else {
          g_data_store->Read(load_tbl_name, load_key, data);
        }
        LOG_INFO("XXX read from remote storage for key %s in time %.2f us, use_query %d", load_key.ToString().c_str(), (GetSystemClock() - starttime)*1.0/1000, (use_query?1:0));
      } else {
        g_data_store->Read(load_tbl_name, load_key, data); 
      }
    }
    // printStackTrace();
    pg.SetFromLoaded(const_cast<char *>(data.c_str()), sizeof(BasePage));
    M_ASSERT(pg.header_.tag == tag, "Loaded page does not match requested info!");
  } else {
    if (desc->CheckFlag(BM_DIRTY)) {
      auto store_key = pg.header_.tag.GetStorageKey();
      g_data_store->Write(ITable::GetStorageId(pg.header_.tag.first_),
                          store_key, pg_start, sizeof(BasePage));
    }
    pg.ResetPage();
  }
}

void PageBufferManager::ParallelFlushAll(int thd, int num_thds, size_t cnt) {
  auto starttime = GetSystemClock();
  int req = 0;
  BufferDesc * cursor = evict_clock_hand_;
  auto portion = (cnt/num_thds) / 100;
  if (portion < 1) {
    portion = 1;
  }
  bool reach_end = false;
  int i = 0;
  for (; (i < cnt) && (!reach_end); i++) {

    if (cursor == evict_clock_tail_) {
      reach_end = true;
    }
    if (i % num_thds != thd) {
      cursor = cursor->clock_next_;
      continue;
    }
    if (!g_restore_from_remote &&  ((req % portion) == 0) && req != 0 && thd == 0 ) {
      auto latency = (GetSystemClock() - starttime) / 1000000.0;
      auto total_flushed = flush_tracker_.load();
      LOG_DEBUG("[thd-%d] loading progress %zu / %zu, thread throughput = %.2f pages/sec, "
                "latency = %.2f ms per page",
                thd, total_flushed, cnt,
                total_flushed / (latency / 1000.0), latency / total_flushed);
      std::cout.flush();
    }
    auto &pg = pool_[cursor->buf_id];
    auto start = reinterpret_cast<char *> (&pg);
    auto key = pg.header_.tag.GetStorageKey();
    auto table_name = ITable::GetStorageId(pg.header_.tag.first_);
    g_data_store->Write(table_name, key, start, sizeof(BasePage));
    if (req == 0 && thd == 0) {
       LOG_DEBUG("[thd-%d] flush to table %s", thd, table_name.c_str());
    }
    cursor = cursor->clock_next_;
    flush_tracker_++;
    req++;
  }
  LOG_DEBUG("[thd-%d] clock items %d", thd, i);

}


void PageBufferManager::FlushAll() {
  auto progress = 0;
  auto portion = allocated_ / 100;
  auto starttime = GetSystemClock();
  auto cnt = allocated_.load();

  LOG_DEBUG("start flushing %lu data pages with %ld threads", cnt, g_num_load_thds);
  auto &pg_st = pool_[evict_clock_hand_->buf_id];
  auto &pg_end = pool_[evict_clock_tail_->buf_id];
  LOG_DEBUG("XXX Evict hand bufid %d, pageid %d, Evict tail bufid %d, pageid %d", evict_clock_hand_->buf_id, pg_st.header_.tag.pg_id_, evict_clock_tail_->buf_id,  pg_end.header_.tag.pg_id_);
  std::vector<std::thread> threads;
  // flush data pages
  for (size_t i = 0; i < g_num_load_thds; i++) {
    threads.emplace_back(PageBufferManager::ExecuteFlush, this, i, g_num_load_thds, cnt);
  }
  std::for_each(threads.begin(), threads.end(), std::mem_fn(&std::thread::join));
  LOG_INFO("Data loading takes %.2f seconds. flushed %lu pages",
           (GetSystemClock() - starttime) / 1000000000.0, cnt);
  while (allocated_ > 0 && evict_clock_hand_) {
    ClockRemoveHand(evict_clock_hand_, evict_clock_tail_, allocated_);
  }
  LOG_INFO("Data loading in total takes %.2f seconds.",
           (GetSystemClock() - starttime) / 1000000000.0, cnt);
}



// BufferDesc Methods
void PageBufferManager::BufferDesc::Pin(uint64_t weight, bool reset) {
  // pin cnt can be larger than # worker thread since a txn may access
  // a page multiple times to fetch different tuples.
  
  // a hack for uniform weight
  weight = 1;

  auto init_pin_cnt = GetPinCount();
  M_ASSERT(init_pin_cnt < BM_MAX_REF_COUNT,
           "pin count %u exceed the limit of %d", init_pin_cnt, BM_MAX_REF_COUNT);
  // usage count cannot exceed the max, protected by desc_latch
  if (reset) {
    buf_state_ = BUF_REFCOUNT_ONE + weight * BUF_USAGECOUNT_ONE;
    assert(GetPinCount() == 1);
  } else {
    auto usage = GetUsageCount();
    if (usage + weight <= BM_MAX_USAGE_COUNT) {
      buf_state_ += weight * BUF_USAGECOUNT_ONE + BUF_REFCOUNT_ONE;
    } else {
      buf_state_ += (BM_MAX_USAGE_COUNT - usage) * BUF_USAGECOUNT_ONE
          + BUF_REFCOUNT_ONE;
    }
    M_ASSERT(GetUsageCount() <= BM_MAX_USAGE_COUNT,
             "usage exceeding limit; may due to pin cnt overflow");
  }
  // LOG_INFO("Pin a page with buf id %d, pin count %d, usage count %d", buf_id, GetPinCount(), GetUsageCount());
  M_ASSERT(IsPinned(), "pin failure");
}

void PageBufferManager::BufferDesc::UnPin(bool dirty) {
  // decrement refcount and set dirty bit if needed
  if (dirty && g_warmup_finished) {
    // must hold the lock in exclusive mode
    SetFlag(BM_DIRTY);
    if (GetUsageCount() < BM_MAX_USAGE_COUNT)
      buf_state_ += BUF_USAGECOUNT_ONE;
  }
  // M_ASSERT(GetPinCount() != 0, "Unpin a page with 0 pin count for buf id %d", buf_id);
  if (GetPinCount() == 0) {
     LOG_DEBUG("Unpin a page with 0 pin count for buf id %d", buf_id);
  }
  buf_state_ -= BUF_REFCOUNT_ONE;
  // LOG_INFO("UnPin a page with buf id %d, pin count %d, usage count %d", buf_id, GetPinCount(), GetUsageCount());

}


}