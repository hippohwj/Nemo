#include "ITable.h"
#include "ITuple.h"
#include "db/access/IndexBTreeObject.h"
#include "db/access/IndexBTreePage.h"
#include "db/buffer/ObjectBufferManager.h"
#include "db/buffer/PageBufferManager.h"
#include "remote/IDataStore.h"
#include "common/Worker.h"
#include "common/Stats.h"
#include "db/ITxn.h"
#include "db/CCManager.h"
#include <thread>
#include <chrono>

namespace arboretum {

ITable::ITable(std::string tbl_name, OID tbl_id, ISchema *schema)  :
    tbl_name_(std::move(tbl_name)), tbl_id_(tbl_id), schema_(schema) {
  storage_id_ = g_dataset_id;
  storage_id_ += "-" + std::to_string(tbl_id);

  if (g_index_type == TWOTREE) {
    top_storage_id_ = g_top_dataset_id;
    bottom_storage_id_ = g_bottom_dataset_id;
    top_storage_id_ += "-" + std::to_string(tbl_id);
    bottom_storage_id_ += "-" + std::to_string(tbl_id);
  }

  // if (!g_restore_from_remote) {
    g_data_store->CreateTable(storage_id_);
  // }
};

IIndex * ITable::GetBottomIdx() {
   M_ASSERT(g_index_type == TWOTREE, "only support twotree");
   return indexes_[2];
}

IIndex * ITable::GetTopIdx() {
   M_ASSERT(g_index_type == TWOTREE, "only support twotree");
   return indexes_[1];
}
// used in cache warmpup and data generation phase
void ITable::InitInsertTupleWithPK(char* data, size_t sz, uint64_t pk, bool is_negative,  bool unset_gapbit) {
  if (g_buf_type == NOBUF && g_load_to_remote_only) {
    // for data loading
    auto tid = row_cnt_.fetch_add(1);
    SearchKey pkey = SearchKey(pk);
    g_data_store->Write(GetStorageId(), std::to_string(pkey.ToUInt64() / g_partition_sz), pkey.ToString(), data, sz);
  } else if (g_buf_type == OBJBUF || g_buf_type == HYBRBUF) {
    // for tuple warmup 
    row_cnt_.fetch_add(1);
    OID tid = pk;
    auto tuple = new (MemoryAllocator::Alloc( GetTotalTupleSize())) ITuple(tbl_id_, tid);
    tuple->SetData(data, sz);
    if (is_negative) {
      tuple->SetTombstone();
    }
    if (unset_gapbit) {
      tuple->UnsetGapBit();
    }
    bool res = InitIndexInsert(tuple, 0, 0);
    if (!res) {
      // tuple is already in the index
      DEALLOC(tuple);
    } else {
      // need to unpin
      arboretum::ObjectBufferManager::FinishAccessTuple(tuple, !g_restore_from_remote);
    }

  } else {
    // only used in page-based index data generation phase
    M_ASSERT(g_buf_type == PGBUF, "unexpected usage for InitInsertTupleWithPK");
    M_ASSERT(g_restore_from_remote == false, "should only reach this branch for data loading");
    row_cnt_.fetch_add(1);
    OID tid = pk;
    // keep inserting in the latest page without holding latches.
    // if full, allocate a new page and finish accessing this one
    // once all done. send a message to close last one.
    auto buf_mgr = (PageBufferManager *) g_buf_mgr;
    if (!pg_cursor_ || pg_cursor_->MaxSizeOfNextItem() < GetTotalTupleSize()) {
      // alloc new page
      if (pg_cursor_) {
        buf_mgr->FinishAccessPage(pg_cursor_, true);
      }
      auto pid = pg_cnt_.fetch_add(1);
      pg_cursor_ = buf_mgr->AllocNewPage(tbl_id_, pid);
      LOG_INFO("Allocated new data page %lu for tuple %lu", pid, tid);
      std::cout.flush();
    }
    // insert into the page and indexes
    auto addr = pg_cursor_->AllocDataSpace(GetTotalTupleSize());
    auto tuple = new (addr) ITuple(tbl_id_, tid);
    tuple->SetData(data, sz);
    InitIndexInsert(tuple, pg_cursor_->GetPageID(),
    pg_cursor_->GetMaxOffsetNumber());
    // TODO: not thread safe. for future insert request after loading, need to
    //  protect it with latches
    // insert into lock table
    lock_tbl_.push_back(NEW(std::atomic<uint32_t>)(0));
  }
}

// only used in page-based index data generation phase
void ITable::InitInsertTuple(char *data, size_t sz) {
  if (g_buf_type == NOBUF) {
    auto tid = row_cnt_.fetch_add(1);
    auto tuple = new (MemoryAllocator::Alloc(GetTotalTupleSize())) ITuple(tbl_id_, tid);
    tuple->SetData(data, sz);
    if (g_load_to_remote_only) {
      auto pkey = tuple->GetPrimaryKey(schema_);
      g_data_store->Write(GetStorageId(), std::to_string(pkey.ToUInt64() /
      g_partition_sz), pkey.ToString(), data, sz);
    }
  } else if (g_buf_type == OBJBUF || g_buf_type == HYBRBUF) {
    auto tid = row_cnt_.fetch_add(1);
    auto tuple = new (MemoryAllocator::Alloc( GetTotalTupleSize())) ITuple(tbl_id_, tid);
    tuple->SetData(data, sz);
    InitIndexInsert(tuple, 0, 0);
    // need to unpin
    arboretum::ObjectBufferManager::FinishAccessTuple(tuple, !g_restore_from_remote);
  } else {
    M_ASSERT(g_restore_from_remote == false, "should only reach this branch for data loading");
    auto tid = row_cnt_.fetch_add(1);
    // keep inserting in the latest page without holding latches.
    // if full, allocate a new page and finish accessing this one
    // once all done. send a message to close last one.
    auto buf_mgr = (PageBufferManager *) g_buf_mgr;
    if (!pg_cursor_ || pg_cursor_->MaxSizeOfNextItem() < GetTotalTupleSize()) {
      // alloc new page
      if (pg_cursor_) {
        buf_mgr->FinishAccessPage(pg_cursor_, true);
      }
      auto pid = pg_cnt_.fetch_add(1);
      pg_cursor_ = buf_mgr->AllocNewPage(tbl_id_, pid);
    }
    // insert into the page and indexes
    auto addr = pg_cursor_->AllocDataSpace(GetTotalTupleSize());
    auto tuple = new (addr) ITuple(tbl_id_, tid);
    tuple->SetData(data, sz);
    InitIndexInsert(tuple, pg_cursor_->GetPageID(),
                pg_cursor_->GetMaxOffsetNumber());
  }
  // TODO: not thread safe. for future insert request after loading, need to
  //  protect it with latches
  // insert into lock table
  lock_tbl_.push_back(NEW(std::atomic<uint32_t>)(0));
}

void ITable::PrintIndex() {
  M_ASSERT(g_buf_type == BufferType::OBJBUF || g_buf_type == BufferType::PGBUF, "only support print index for OBJBUF or PGBUF");
  for (auto & pairs : indexes_) {
    // generate search key for corresponding index
    // does not support composite key yet
    SearchKey idx_key;
    auto & index = pairs.second;
    LOG_INFO("Print Current Index =================================");
    if (g_buf_type == BufferType::OBJBUF) {
      ((IndexBTreeObject *) index)->PrintTree();
    } else if (g_buf_type == BufferType::PGBUF) {
      ((IndexBTreePage *) index)->PrintTree();
    }
  } 

}

bool ITable::InitIndexInsert(ITuple * p_tuple, OID pg_id, OID offset) {
  //TODO(Hippo): adapt to two-tree 
  if (g_index_type == IndexType::REMOTE) {
    // not used
    auto key = p_tuple->GetPrimaryKey(schema_);
    g_data_store->Write(GetStorageId(), key, p_tuple->GetData(),
                        GetTupleDataSize());
  } else if (g_buf_type == BufferType::OBJBUF) {
    RC rc = RC::OK;
    for (auto & pairs : indexes_) {
      // generate search key for corresponding index
      // does not support composite key yet
      SearchKey idx_key;
      auto & index = pairs.second;
      p_tuple->GetField(schema_, *index->GetCols().begin(), idx_key);
      auto tag = NEW(SharedPtr);
      ((IndexBTreeObject *) index)->Insert(idx_key, p_tuple, INSERT, nullptr, tag);
      if (tag->Get() == p_tuple) {
        // tuple is newly inserted in the index (rather than an existed item)
        // if (g_negative_scan_wl && (!p_tuple->IsTombstone())) {
          if (p_tuple->IsTombstone()) {
            ((ObjectBufferManager *) ((g_buf_type == HYBRBUF)? g_top_buf_mgr:g_buf_mgr))->AllocSpace(sizeof(ITuple));
          } else {
            ((ObjectBufferManager *) ((g_buf_type == HYBRBUF)? g_top_buf_mgr:g_buf_mgr))->AdmitTuple(p_tuple);
          }
        // }
        return true;
      } else {
        return false;
      }
    }
  } else if (g_buf_type == BufferType::PGBUF) {
    // insert into index
    for (auto &pairs: indexes_) {
      // generate search key for corresponding index
      // NOTE: does not support composite key yet
      SearchKey idx_key;
      auto &index = pairs.second;
      p_tuple->GetField(schema_, *index->GetCols().begin(), idx_key);
      ((IndexBTreePage *) index)->Insert(idx_key, pg_id, offset);
    }
  } else if (g_buf_type == BufferType::HYBRBUF) {
    M_ASSERT(g_load_to_remote_only == false, "Only reach InitIndexInsert() in TWOTREE for warmup");
    // for warm up cache, only warm up the top index tree
      SearchKey idx_key;
      auto index = GetTopIdx();
      p_tuple->GetField(schema_, *index->GetCols().begin(), idx_key);
      ((IndexBTreeObject *) index)->Insert(idx_key, p_tuple, INSERT, nullptr);
  }
  return true;
}

// This method is only called by user query txn
RC ITable::InsertTuple(SearchKey& pkey, char *data, size_t sz, ITxn *txn) {
  RC rc;
  if (g_buf_type == OBJBUF) {
    // update row_cnt
    row_cnt_.fetch_add(1);
    auto tuple = ((ObjectBufferManager *) g_buf_mgr)->AllocTuple(
        GetTotalTupleSize(), tbl_id_, pkey.ToUInt64());
    tuple->SetData(data, sz);
    // insert into every index
    for (auto & pairs : indexes_) {
      auto & index = pairs.second;
      // generate search key for corresponding index; composite key not supported yet
      SearchKey idx_key;
      tuple->GetField(schema_, *index->GetCols().begin(), idx_key);
      rc = ((IndexBTreeObject *) index)->Insert(idx_key, tuple, INSERT, txn);
      if (rc == ABORT) {
        tuple->buf_state_ = ObjectBufferManager::SetFlag(tuple->buf_state_, BM_NOT_IN_INDEX);
        break;
      }
      // LOG_DEBUG("inserted key %lu", idx_key.ToUInt64());
    }
    // need to unpin
    arboretum::ObjectBufferManager::FinishAccessTuple(tuple, rc != ABORT);
  } else {
    LOG_ERROR("not supported yet");
  }
  return rc;
}


SharedPtr * ITable::IndexRangeSearchNegativePhantomOp(SearchKey low_key, SearchKey high_key, OID idx_id,
  ITxn *txn, RC &rc, size_t &cnt) {
    std::vector<std::pair<SearchKey, SearchKey>> fetch_req;
    size_t result_cnt = 0;
    SharedPtr * tags;
    auto index = (IndexBTreeObject *) indexes_[idx_id];
    BTreeObjNode* hint_parent = nullptr;
    BTreeObjNode* hint_child = nullptr;

    if (NeedPhantom(g_phantom_protection_type)) {
        uint64_t phantom_starttime = GetSystemClock(); 
      // first try to insert phantoms for bounds of the range
      // M_ASSERT(false, "TODO: finish this part")
      // LOG_INFO("Insert phantom entries for range bounds");
      // insert low key
      auto tuple = new (MemoryAllocator::Alloc(GetTotalTupleSize())) ITuple(tbl_id_, low_key.ToUInt64());
      // auto tuple = ((ObjectBufferManager *) g_buf_mgr)->AllocTuple(
      //   GetTotalTupleSize(), tbl_id_, low_key.ToUInt64());
      tuple->SetPhantom();
      // TODO: need to set data and primary key for the tuple?
      // tuple->SetData(data, sz);
      // schemas_[0]->SetPrimaryKey((char*)data, key);
      // SharedPtr * temp_tag = NEW(SharedPtr);
      rc = index->PhantomInsert(low_key, tuple, AccessType::READ, txn, nullptr, &hint_parent, &hint_child);
      if (rc == RC::ABORT) {
        // DEALLOC(temp_tag);
        DEALLOC(tuple);
        if (g_warmup_finished) {
          uint64_t phantom_insert_time = GetSystemClock() - phantom_starttime;
          txn->AddPhantomInsertTime(phantom_insert_time);
          LOG_INFO("XXX Phantom insert time: %ld us for low key %ld", phantom_insert_time, low_key.ToUInt64());
        }
        return nullptr;
      }
      txn->AddPhantomEntry(tuple);
      // insert high key
      // auto high_tuple = ((ObjectBufferManager *) g_buf_mgr)->AllocTuple(
      //   GetTotalTupleSize(), tbl_id_, high_key.ToUInt64());
      
      auto high_tuple = new (MemoryAllocator::Alloc(GetTotalTupleSize())) ITuple(tbl_id_, high_key.ToUInt64());
      high_tuple->SetPhantom();
      // SharedPtr * temp_tag = NEW(SharedPtr);
      // BTreeObjNode* fake_parent = nullptr;
      // BTreeObjNode* fake_child = nullptr;
      // rc = index->PhantomInsert(high_key, high_tuple, AccessType::READ, txn, nullptr, &fake_parent, &fake_child);
      rc = index->EndPhantomInsert(high_key, high_tuple, AccessType::READ, txn, nullptr,hint_parent,hint_child);

      if (rc == RC::ABORT) {
        DEALLOC(high_tuple);
        // DEALLOC(temp_tag);
        if (g_warmup_finished) {
          uint64_t phantom_insert_time = GetSystemClock() - phantom_starttime;
          txn->AddPhantomInsertTime(phantom_insert_time);
          LOG_INFO("XXX Phantom insert time: %ld us for high key %ld", phantom_insert_time, high_key.ToUInt64());
        }
        return nullptr;
      }
      txn->AddPhantomEntry(high_tuple);

      if (g_warmup_finished) {
        uint64_t phantom_insert_time = GetSystemClock() - phantom_starttime;
        txn->AddPhantomInsertTime(phantom_insert_time);
      }
    }

    tags = index->RangeSearch(low_key, high_key, txn, fetch_req, rc, cnt, hint_parent, hint_child);
    if (rc == ABORT) {
      return nullptr;
    }

    // fetch missing tuples.
    for (auto& req : fetch_req) {
      // fetch range (req.first, req.second). exclusive.
      auto low = SearchKey(req.first.ToUInt64());
      auto high = SearchKey(req.second.ToUInt64());

      M_ASSERT((low.ToUInt64() >= low_key.ToUInt64()) && (high.ToUInt64() <= high_key.ToUInt64() && low.ToUInt64() <= high.ToUInt64()),
               "unexpected remote fetch(low %ld, high %ld), original range query with (low key %ld, high key %ld)",
               low.ToUInt64(), high.ToUInt64(), low_key.ToUInt64(), high_key.ToUInt64());

      size_t result_cnt = 0;
      char* addrs = nullptr;
      g_data_store->ReadRange(GetStorageId(), low, high, &addrs, GetTotalTupleSize(), result_cnt);
      cnt += result_cnt;
      if (addrs) {
        DEALLOC(addrs);
      }

      if (g_warmup_finished && result_cnt == 0) {
        g_stats->db_stats_[Worker::GetThdId()].incr_false_positive_(1); 
      }
    }
    // LOG_INFO("range search %lu - %lu, cnt = %lu", low_key.ToUInt64(), high_key.ToUInt64(), cnt);
    if (g_warmup_finished) {
      g_stats->db_stats_[Worker::GetThdId()].incr_accesses_(cnt);
      g_stats->db_stats_[Worker::GetThdId()].incr_tuple_accesses_(1);
      if (cnt == 0) {
        g_stats->db_stats_[Worker::GetThdId()].incr_negative_requests_(1); 
      } else {
        g_stats->db_stats_[Worker::GetThdId()].incr_positive_requests_(1); 
      }
      if (fetch_req.size() == 0) {
        if (cnt == 0) {
          g_stats->db_stats_[Worker::GetThdId()].incr_true_negative_(1);
        } 
      } else {
        // LOG_INFO("Cache miss: range search %lu - %lu", low_key.ToUInt64(), high_key.ToUInt64());
        g_stats->db_stats_[Worker::GetThdId()].incr_misses_(1);
      }
        
      g_stats->db_stats_[Worker::GetThdId()].incr_txn_scans_(fetch_req.size());
      // how many txns have more than 1 scan request
      if (fetch_req.size() > 1) {
        g_stats->db_stats_[Worker::GetThdId()].incr_more_than_one_scan_txns_(1);
      }
    }
    return tags;

}


SharedPtr * ITable::IndexRangeSearchNegativeOp(SearchKey low_key, SearchKey high_key, OID idx_id,
  ITxn *txn, RC &rc, size_t &cnt) {
    std::vector<std::pair<SearchKey, SearchKey>> fetch_req;
    size_t result_cnt = 0;
    SharedPtr * tags;
    auto index = (IndexBTreeObject *) indexes_[idx_id];
    tags = index->RangeSearch(low_key, high_key, txn, fetch_req, rc, cnt);
    if (rc == ABORT) {
      return nullptr;
    }
    // auto high_key_int64 = high_key.ToUInt64();
    // auto low_key_int64 = low_key.ToUInt64();
    // for (size_t i = 0; i < high_key_int64 - low_key_int64 + 1; i++) {
    //   char * data;
    //   size_t sz;
    //   if (tags[i].Get()) {
    //     M_ASSERT(g_buf_type != HYBRBUF, "HYBRBUF is an unexpected buffer type for Range query");
         
    //     rc = txn->AccessTuple(&tags[i], this, low_key_int64 + i,
    //                    SCAN, data, sz);  
    //     if (rc == RC::ABORT) {
    //       // LOG_DEBUG("Scan abort for key %d (low key %d, high key %d)",  low_key.ToUInt64() + i, low_key.ToUInt64(), high_key.ToUInt64());
    //       return nullptr;
    //     }
    //   }
    // }

    // fetch missing tuples.
    for (auto& req : fetch_req) {
      // fetch range (req.first, req.second). exclusive.
      // auto low = SearchKey(req.first.ToUInt64() + 1);
      // auto high = SearchKey(req.second.ToUInt64() - 1);
      auto low = SearchKey(req.first.ToUInt64());
      auto high = SearchKey(req.second.ToUInt64());

      M_ASSERT((low.ToUInt64() >= low_key.ToUInt64()) && (high.ToUInt64() <= high_key.ToUInt64() && low.ToUInt64() <= high.ToUInt64()),
               "unexpected remote fetch(low %ld, high %ld), original range query with (low key %ld, high key %ld)",
               low.ToUInt64(), high.ToUInt64(), low_key.ToUInt64(), high_key.ToUInt64());

      size_t result_cnt = 0;
      char* addrs = nullptr;
      g_data_store->ReadRange(GetStorageId(), low, high, &addrs, GetTotalTupleSize(), result_cnt);
      cnt += result_cnt;
      if (addrs) {
        DEALLOC(addrs);
      }



      if (g_warmup_finished && result_cnt == 0) {
        g_stats->db_stats_[Worker::GetThdId()].incr_false_positive_(1); 
      }
    }

    // LOG_INFO("range search %lu - %lu, cnt = %lu", low_key.ToUInt64(), high_key.ToUInt64(), cnt);
    if (g_warmup_finished) {
      g_stats->db_stats_[Worker::GetThdId()].incr_accesses_(cnt);
      g_stats->db_stats_[Worker::GetThdId()].incr_tuple_accesses_(1);
      if (cnt == 0) {
        g_stats->db_stats_[Worker::GetThdId()].incr_negative_requests_(1); 
      } else {
        g_stats->db_stats_[Worker::GetThdId()].incr_positive_requests_(1); 
      }
      if (fetch_req.size() == 0) {
        if (cnt == 0) {
          g_stats->db_stats_[Worker::GetThdId()].incr_true_negative_(1);
        } 
      } else {
        // LOG_INFO("Cache miss: range search %lu - %lu", low_key.ToUInt64(), high_key.ToUInt64());
        g_stats->db_stats_[Worker::GetThdId()].incr_misses_(1);
      }
        
      g_stats->db_stats_[Worker::GetThdId()].incr_txn_scans_(fetch_req.size());
      // how many txns have more than 1 scan request
      if (fetch_req.size() > 1) {
        g_stats->db_stats_[Worker::GetThdId()].incr_more_than_one_scan_txns_(1);
      }
    }
    return tags;

}




SharedPtr* ITable::IndexRangeSearchNoNegativeOp(SearchKey low_key, SearchKey high_key, OID idx_id, ITxn* txn, RC& rc,
                                                size_t& cnt) {
  char* addrs = nullptr;
  g_data_store->ReadRange(GetStorageId(), low_key, high_key, &addrs, GetTotalTupleSize(), cnt);
  // LOG_INFO("remote fetch req: %ld - %ld with returned cnt = %ld", low_key.ToUInt64(), high_key.ToUInt64(), cnt);

  SharedPtr* tags;
  if (cnt == 0) {
    tags = nullptr;
  } else {
    tags = NEW_SZ(SharedPtr, cnt);
  }

  size_t next = 0;
  for (int i = 0; i < cnt; i++) {
    // initially assign a fake TID to ITuple
    auto tuple = new (&addrs[next]) ITuple(tbl_id_, low_key.ToUInt64() + i);
    next += GetTotalTupleSize();
    SearchKey key;
    // no need to set from loaded since memcpy is done in azure client.
    // tuple->SetFromLoaded(const_cast<char *>(data.c_str()), GetTupleDataSize());
    tuple->GetPrimaryKey(schema_, key);
    // set actual TID to ITuple
    tuple->SetTID(key.ToUInt64());
    if (!(low_key.ToUInt64() <= key.ToUInt64() && key.ToUInt64() <= high_key.ToUInt64())) {
      M_ASSERT(false, "Loaded data does not match: low_key: %ld, high_key: %ld, index: %d, cur_key: %ld",
               low_key.ToUInt64(), high_key.ToUInt64(), i, key.ToUInt64());
    } 
    

    tags[i].Init(tuple);
    // Data & sz is placeholder, no actual usage
    char* data;
    size_t sz;
    // TODO(Hippo): support two-tree
    M_ASSERT(g_buf_type != HYBRBUF, "HYBRBUF is an unexpected buffer type for GetTuples()");
    bool free_ptr = (i == 0);
    rc = txn->AccessTuple(&tags[i], this, key.ToUInt64(), SCAN, data, sz, free_ptr);

    if (rc == RC::ABORT) {
      LOG_DEBUG("Scan abort for key %d (low key %d, high key %d)", key.ToUInt64(), low_key.ToUInt64(),
                high_key.ToUInt64());
      return nullptr;
    }

  }

  if (g_warmup_finished) {
    // TODO: need to optimize the stats for range search
    g_stats->db_stats_[Worker::GetThdId()].incr_accesses_(cnt);
    g_stats->db_stats_[Worker::GetThdId()].incr_tuple_accesses_(1);
    g_stats->db_stats_[Worker::GetThdId()].incr_misses_(1);
    if (cnt == 0) {
      g_stats->db_stats_[Worker::GetThdId()].incr_false_positive_(1);
      g_stats->db_stats_[Worker::GetThdId()].incr_negative_requests_(1);
    } else {
      g_stats->db_stats_[Worker::GetThdId()].incr_positive_requests_(1);
    }
    g_stats->db_stats_[Worker::GetThdId()].incr_txn_scans_(1);
  }

  // TODO: need to merge with cached records.
  return tags;
}

SharedPtr *
ITable::IndexRangeSearch(SearchKey low_key, SearchKey high_key, OID idx_id,
                         ITxn *txn, RC &rc, size_t &cnt) {
  // LOG_DEBUG("start range search %lu - %lu", low_key.ToUInt64(), high_key.ToUInt64());
  SharedPtr * tags;
  if (g_buf_type == OBJBUF) {
    if (g_enable_partition_covering_lock) {
      // aquire partition lock before access data
      auto start_pid = GetPartitionID(low_key.ToUInt64());
      auto end_pid = GetPartitionID(high_key.ToUInt64() - 1);
      // std::vector<std::atomic<uint64_t>*> intention_locks;
      LOG_XXX("XXX Try to Acq S lock from pid %d to pid %d for low key %ld, high key %ld", start_pid, end_pid, low_key.ToUInt64(), high_key.ToUInt64());

      for (int i = start_pid; i <= end_pid; i++) {
          auto lock = GetIntentionLockStatePtr(i);
          // auto wait_state = IxLockWaitStatePtr(i);
          uint64_t cc_starttime = GetSystemClock(); 
          // rc = CCManager::AcquireIntentionLock(*lock, ILockType::S, *wait_state);
          rc = CCManager::AcquirePhantomIntentionLock(*lock, false);
          if (g_warmup_finished) {
            uint64_t cctime =  GetSystemClock() - cc_starttime;
           txn->AddCCTime(cctime);
          } 
          if (rc == ABORT) {
            //     // release previously held index locks
            // for (auto & cur_lock : intention_locks) {
            //   CCManager::ReleaseIntentionLock(*cur_lock, ILockType::S);
            //   // LOG_INFO("XXX Rel S lock on pid %d", i);
            // }
            LOG_XXX("XXX Failed to Acq S lock on pid %d for low key %ld", i, low_key.ToUInt64());
            return nullptr;
          }
          LOG_XXX("XXX Success to Acq S lock on pid %d for low key %ld", i, low_key.ToUInt64());
          IntentionLockAccess iac = {i, ILockType::S, lock};
          txn->intention_locks_.push_back(iac);
      }
    }
    switch (g_negative_search_op_type) {
      case NegativeSearchOpType::GAPBIT_PHANTOM:
          // gapbit + phantom entries for phantom protection: 
          return IndexRangeSearchNegativePhantomOp(low_key, high_key, idx_id, txn, rc, cnt);
      case NegativeSearchOpType::GAPBIT_ONLY:
          // Similar operations for both nemo negative search optimization (GAPBIT_PHANTOM) and negative search with only gap bit optimization (GAPBIT_ONLY)
          return IndexRangeSearchNegativePhantomOp(low_key, high_key, idx_id, txn, rc, cnt);
          // return IndexRangeSearchNegativeOp(low_key, high_key, idx_id, txn, rc, cnt);
      case NegativeSearchOpType::NO_OP:
          // since record cache doesn't support range query, directly access the remote storage
          return IndexRangeSearchNoNegativeOp(low_key, high_key, idx_id, txn, rc, cnt);
      default:
          M_ASSERT(false, "unsupported negative search optimization type");
      }
  } else if (g_index_type == IndexType::REMOTE) {
    return IndexRangeSearchNoNegativeOp(low_key, high_key, idx_id, txn, rc, cnt);
  } else if (g_buf_type == PGBUF) {
    auto index = (IndexBTreePage *) indexes_[idx_id];
    tags = index->RangeSearch(low_key, high_key, txn, rc, cnt);
    if (rc == ABORT) return nullptr;
  } else {
    M_ASSERT(false, "unexpected buf type for IndexRangeSearch");
  }
  return tags;
}

void ITable::TwoTreeEviction(SearchKey flush_key, char *flush_data, size_t flush_sz) {
    auto bottom_index = (IndexBTreePage *) GetBottomIdx();
    auto result = bottom_index->Search(flush_key);
    // pin the page when accessing the tuple.
    ITuple * tuple = ((PageBufferManager *) g_bottom_buf_mgr)->AccessTuple(this, *result, flush_key.ToUInt64());
    // TODO(hippo): figure out wether need lock coupling for top and bottom trees.
    // flush_data is from tuple data but tuple here should be page tuple
    tuple->SetData(flush_data, flush_sz, PGBUF);
    ((PageBufferManager *) g_bottom_buf_mgr)->FinishAccessTuple(*result, true);
}

SharedPtr *
ITable::IndexSearch(SearchKey key, AccessType ac_type, ITxn *txn, RC &rc, IndexType idx_type,BufferType buf_type) {
  if (idx_type == IndexType::REMOTE) {
    rc = RC::OK;
    auto tuple_shared_ptr = NEW(SharedPtr);
    std::string data;
    g_data_store->Read(GetStorageId(), key, data);
    auto tuple = new (MemoryAllocator::Alloc(GetTotalTupleSize())) ITuple(tbl_id_, key.ToUInt64());
    tuple->SetFromLoaded(const_cast<char *>(data.c_str()), GetTupleDataSize());
    SearchKey pkey;
    tuple->GetPrimaryKey(schema_, pkey);
    M_ASSERT(pkey == key, "Loaded data does not match!");
    tuple_shared_ptr->Init(tuple);
    return tuple_shared_ptr;
  } else if (buf_type == OBJBUF) {
    auto index = (IndexBTreeObject *) indexes_[1];
    auto tag = index->Search(key, ac_type, txn, rc);
    if (rc == RC::ABORT) {
      return tag;
    }
    if (rc == RC::ERROR) {
      LOG_XXX("XXX Cache miss for key %ld", key.ToUInt64());
      // TODO: currently assume data is always in storage if not found locally
      //   once supporting true delete, need to check next_key_in_storage
      // cache miss! need to load from storage
      auto tuple = ((ObjectBufferManager *)g_buf_mgr)->LoadFromStorage(tbl_id_, key);
      // it will never abort since read lock is acquired during index->Search.
      tag = NEW(SharedPtr);
      if ((!tuple->IsTombstone()) || (g_negative_search_op_enable && tuple->IsTombstone())) {
        rc = index->Insert(key, tuple, AccessType::READ, nullptr, tag);
        // if already exist, mark tuple as no need to delete index.
        if (tag->Get() != tuple) tuple->buf_state_ = ObjectBufferManager::SetFlag(tuple->buf_state_, BM_NOT_IN_INDEX);

      } else {
        // if the tuple is tombstone and g_negative_search_op_enable is disabled
        tag->Init(tuple);
        rc = RC::OK;
      }

      if (g_warmup_finished) {
        g_stats->db_stats_[Worker::GetThdId()].incr_misses_(1);
        if (tuple->IsTombstone()) {
          g_stats->db_stats_[Worker::GetThdId()].incr_false_positive_(1);
        }
        // LOG_INFO("XXX cache miss pk %ld", key.ToUInt64());
      }
    } else {
      //if the returned entry is a tombstone
      if (reinterpret_cast<ITuple *>(tag->Get())->IsTombstone()) {
        // LOG_INFO("XXX cache hit tombstone pk %ld", key.ToUInt64());
        // std::cout.flush();
        if (g_warmup_finished) {
          g_stats->db_stats_[Worker::GetThdId()].incr_true_negative_(1);
        }
      }
    }
    if (g_warmup_finished) g_stats->db_stats_[Worker::GetThdId()].incr_accesses_(1);
    return tag;
  }  else if (buf_type == HYBRBUF) {
    //search top tree
    auto index = (IndexBTreeObject *) GetTopIdx();
    auto tag = index->Search(key, ac_type, txn, rc);
    if (rc == RC::ABORT) {
      return tag;
    }
    if (rc == RC::ERROR) {
      // cache miss in the top tree! need to load from bottom tree 
      auto bottom_index = (IndexBTreePage *) GetBottomIdx();
      auto result = bottom_index->Search(key);
      // pin the page when accessing the tuple.
      ITuple * bt_tuple = ((PageBufferManager *) g_bottom_buf_mgr)->AccessTuple(this, *result, key.ToUInt64());
      // TODO(hippo): figure out wether need lock coupling for top and bottom trees.
      ((PageBufferManager *) g_bottom_buf_mgr)->FinishAccessTuple(*result, false);
      // use LoadFromStorage to trigger eviction logics
      auto tuple = ((ObjectBufferManager *)g_top_buf_mgr)->LoadFromStorage(tbl_id_, key, bt_tuple->GetData(PGBUF), this->GetTupleDataSize());
      // it will never abort since read lock is acquired during index->Search.
      tag = NEW(SharedPtr);
      rc = index->Insert(key, tuple, AccessType::READ, nullptr, tag);
      // if already exist, mark tuple as no need to delete index.
      if (tag->Get() != tuple)
        tuple->buf_state_ = ObjectBufferManager::SetFlag(tuple->buf_state_, BM_NOT_IN_INDEX);
      // if (g_warmup_finished) g_stats->db_stats_[Worker::GetThdId()].incr_misses_(1);
    }
    if (g_warmup_finished) g_stats->db_stats_[Worker::GetThdId()].incr_accesses_(1);
    return tag;

  } else if (buf_type == PGBUF) {
    auto index = (IndexBTreePage *) indexes_[1];
    auto tags = index->Search(key);
    return tags;
  }
  return nullptr;
}


bool ITable::BatchEvictionViaIndexTraverse() {
  // currently only nemo support batch eviction via index traverse
  // TODO: support batch eviction for twotree 
  bool res = false;
  M_ASSERT(g_index_type == IndexType::BTREE && g_buf_type == BufferType::OBJBUF && indexes_.size() == 1 , "unexpected index type for BatchEvictionViaIndexTraverse");
  for (auto & pairs : indexes_) {
    auto & index = pairs.second;
    res = ((IndexBTreeObject *) index)->EvictRandomLeafNode();
  }
  return res;
}

//TODO(Hippo): support two-tree
void ITable::IndexDelete(ITuple * p_tuple) {
  // delete from all indexes
  if (g_index_type == IndexType::REMOTE) {
    LOG_ERROR("should not happen");
  } else if (g_index_type == IndexType::BTREE) {
    for (auto & pairs : indexes_) {
      auto & index = pairs.second;
      // generate search key for corresponding index
      SearchKey idx_key;
      // TODO: does not support composite key yet
      p_tuple->GetField(schema_, *index->GetCols().begin(), idx_key);
      if (g_buf_type == OBJBUF)
        ((IndexBTreeObject *) index)->Delete(idx_key, p_tuple);
      else LOG_ERROR("Not impl yet.");
    }
  } else if (g_index_type == IndexType::TWOTREE) {
      SearchKey idx_key;
      p_tuple->GetField(schema_, *(((IndexBTreeObject *)GetTopIdx())->GetCols().begin()), idx_key);
      ((IndexBTreeObject *)GetTopIdx())->Delete(idx_key, p_tuple);
  } else {
    LOG_ERROR("unexpected index type for IndexDelete()");
  }
}


bool ITable::DeleteWithPK(uint64_t pk, bool is_tombstone) {
  bool deleted = false;
  // delete from all indexes
  if (g_index_type == IndexType::REMOTE) {
    LOG_ERROR("should not happen");
  } else if (g_index_type == IndexType::BTREE) {
    for (auto & pairs : indexes_) {
      auto & index = pairs.second;
      SearchKey idx_key = SearchKey(pk);
      if (g_buf_type == OBJBUF) {
        ((IndexBTreeObject *) index)->Delete(idx_key, nullptr, &deleted);
        if (deleted) {
          if (is_tombstone) {
            ((ObjectBufferManager *) ((g_buf_type == HYBRBUF)? g_top_buf_mgr:g_buf_mgr))->AllocSpace(sizeof(ITuple));
          } else {
            ((ObjectBufferManager *) ((g_buf_type == HYBRBUF)? g_top_buf_mgr:g_buf_mgr))->DeallocTuples(1);
          }
        }
      } else {
        LOG_ERROR("Not impl yet.");
      }
    }
  } else if (g_index_type == IndexType::TWOTREE) {
    LOG_ERROR("Not impl yet");
  } else {
    LOG_ERROR("unexpected index type for IndexDelete()");
  }
  return deleted;
}


void ITable::FinishLoadingData() {
  M_ASSERT(g_buf_type != HYBRBUF, "HYBRBUF is an unexpected buffer type type for FinishLoadingData()");
  if (g_buf_type == PGBUF) {
    LOG_DEBUG("Finish loading %lu data pages", pg_cnt_.load());
    std::cout.flush();
    auto buf_mgr = (PageBufferManager *) g_buf_mgr;
    buf_mgr->FinishAccessPage(pg_cursor_, true);
    for (auto &pairs: indexes_) {
      auto &index = pairs.second;
      ((IndexBTreePage *) index)->FlushMetaData();
    }
    buf_mgr->FlushAll();
    g_data_store->Write(GetStorageId(), "metadata", "pg_cnt_", pg_cnt_);
  }
  g_data_store->Write(GetStorageId(), "metadata", "row_cnt_", row_cnt_);
}

size_t ITable::GetPgCnt() {
  size_t cnt = 0;
  if (g_buf_type == PGBUF) {
    for (auto &pairs: indexes_) {
      cnt += ((IndexBTreePage *) pairs.second)->GetMetaData().num_nodes_;
    }
  } else if (g_buf_type == HYBRBUF) {
    cnt += ((IndexBTreePage *) GetBottomIdx())->GetMetaData().num_nodes_;
  } else {
    M_ASSERT(g_buf_type == PGBUF || g_buf_type == HYBRBUF, "unexpected buffer type for GetPgCnt");
  }
  return cnt + pg_cnt_;
}

} // arboretum