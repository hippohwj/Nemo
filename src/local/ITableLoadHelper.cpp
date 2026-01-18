#include "ITable.h"
#include "ITuple.h"
#include "db/access/IndexBTreeObject.h"
#include "db/access/IndexBTreePage.h"
#include "db/buffer/ObjectBufferManager.h"
#include "db/buffer/PageBufferManager.h"
#include "remote/IDataStore.h"
#include "common/Worker.h"
#include "common/Stats.h"

namespace arboretum {

// OID ITable::CreateIndex(OID col, IndexType tpe) {
//   switch (tpe) {
//     case REMOTE:
//       break;
//     case IndexType::BTREE:
//       if (g_buf_type == OBJBUF) {
//         auto index = NEW(IndexBTreeObject)(tbl_id_);
//         indexes_[index->GetIndexId()] = index;
//         index->AddCoveringCol(col);
//         return index->GetIndexId();
//       } else if (g_buf_type == PGBUF){
//         auto index = NEW(IndexBTreePage)(tbl_id_);
//         indexes_[index->GetIndexId()] = index;
//         index->AddCoveringCol(col);
//         return index->GetIndexId();
//       } else {
//         M_ASSERT(false, "unknown g_buf_type");
//       }
//   }
//   // for only support non-composite key
//   return 0;
// }

OID ITable::CreateIndex(OID col, IndexType tpe, BufferType buf_tpe) {
  switch (tpe) {
    case REMOTE:
      break;
    case IndexType::BTREE:
      if (buf_tpe == OBJBUF) {
        auto index = NEW(IndexBTreeObject)(tbl_id_, this);
        indexes_[index->GetIndexId()] = index;
        index->AddCoveringCol(col);
        return index->GetIndexId();
      } else if (buf_tpe == PGBUF){
        if (g_buf_type == HYBRBUF) {
          auto index = NEW(IndexBTreePage)(tbl_id_, this, (PageBufferManager *)g_bottom_buf_mgr);
          indexes_[index->GetIndexId()] = index;
          index->AddCoveringCol(col);
          return index->GetIndexId();
        } else {
          auto index = NEW(IndexBTreePage)(tbl_id_, this, (PageBufferManager *)g_buf_mgr);
          indexes_[index->GetIndexId()] = index;
          index->AddCoveringCol(col);
          return index->GetIndexId();
        }
      } else {
        M_ASSERT(false, "unknown g_buf_type");
      }
  }
  // for only support non-composite key
  return 0;
}

void ITable::RestoreTable(uint64_t num_rows, uint64_t data_domain_sz) {
  row_cnt_ = g_data_store->Read(GetStorageId(), "metadata", "row_cnt_");
  if (row_cnt_ != num_rows) {
    row_cnt_ = num_rows;
    g_data_store->Write(GetStorageId(), "metadata", "row_cnt_", row_cnt_);
  }
  LOG_DEBUG("restored table %s with %lu rows", GetStorageId().c_str(), row_cnt_.load());
  
  if (g_buf_type == PGBUF) {
    pg_cnt_ = g_data_store->Read(GetStorageId(), "metadata", "pg_cnt_");
    if (!g_negative_point_wl && !g_negative_scan_wl) {
      for (auto & pair : indexes_) {
        pair.second->Load(pg_cnt_);
      }
    }
    LOG_DEBUG("restored table %s with %lu pages where %lu pages in the buffer", GetStorageId().c_str(), pg_cnt_.load(), ((PageBufferManager *) g_buf_mgr)->GetAllocated());
  } else if (g_buf_type == HYBRBUF) {
    pg_cnt_ = g_data_store->Read(GetBottomStorageId(), "metadata", "pg_cnt_");
    GetBottomIdx()->Load(pg_cnt_);
    // for (auto & pair : indexes_) {
    //   pair.second->Load(pg_cnt_);
    // }
    LOG_DEBUG("restored table %s with %lu pages", GetBottomStorageId().c_str(), pg_cnt_.load()); 
  }
  if (g_load_to_remote_only) return;
  if (g_negative_point_wl || g_negative_scan_wl) {
    for (size_t i = lock_tbl_.size(); i < data_domain_sz; i++) {
      lock_tbl_.push_back(NEW(std::atomic<uint32_t>)(0));
    }
  } else {
    for (size_t i = lock_tbl_.size(); i < row_cnt_; i++) {
      lock_tbl_.push_back(NEW(std::atomic<uint32_t>)(0));
    }
  }


  if (g_enable_partition_covering_lock) {
    auto max_partition_id = GetPartitionID(data_domain_sz - 1);
    // insert one more to make range query lock(upper bound) aquisition happy
    for (int i = 0; i <= max_partition_id + 1; i++) {
      // intention_lock_tbl_.push_back(NEW(std::atomic<uint64_t>)(0));
      intention_lock_tbl_.push_back(NEW(std::atomic<uint32_t>)(0));
      // ix_lock_wait_tbl_.push_back(NEW(std::atomic<uint32_t>)(0));
    }
  }

}

void ITable::FinishWarmupCache() {
  if (g_buf_type == OBJBUF && g_restore_from_remote) {
    // after loading need to mark the left flag as not in storage
    for (auto &pairs: indexes_) {
      ((IndexBTreeObject *) pairs.second)->MarkRightBoundAvailInStorage();
    }
  } else if (g_buf_type == HYBRBUF && g_restore_from_remote ) {
    // only the top index is object index
    //TODO: check whether this is valid
    ((IndexBTreeObject *)GetTopIdx())->MarkRightBoundAvailInStorage();

  }

}

}