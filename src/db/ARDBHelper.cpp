#include "ARDB.h"
#include "ITxn.h"
#include "buffer/ObjectBufferManager.h"
#include "buffer/PageBufferManager.h"
#include "remote/IDataStore.h"
#include "local/ITable.h"

namespace arboretum {

double ARDB::RandDouble() {
  std::uniform_real_distribution<double> distribution(0, 1);
  return distribution(*rand_generator_);
}

int ARDB::RandInteger(int min, int max) {
  std::uniform_int_distribution<> dist(min, max + 1);
  return dist(*rand_generator_);
}

int ARDB::RandIntegerInclusive(int min, int max) {
  std::uniform_int_distribution<> dist(min, max);
  return dist(*rand_generator_);
}

bool ARDB::RandBoolean(double probability) {
  std::bernoulli_distribution dist(probability);
  return dist(*rand_generator_);
}

bool ARDB::CheckBufferWarmedUp(uint64_t row_cnt, uint64_t page_cnt) {
  if (g_buf_type == NOBUF) {
    return true;
  } else if (g_buf_type == OBJBUF) {
    return ((ObjectBufferManager *) g_buf_mgr)->IsWarmedUp(row_cnt);
  } else if (g_buf_type == PGBUF) {
    return ((PageBufferManager *) g_buf_mgr)->IsWarmedUp(page_cnt, true);
  } else if (g_buf_type == HYBRBUF) {
    // M_ASSERT(false, "Check the warmup check logic for HYBRBUF");
    return ((ObjectBufferManager *) g_top_buf_mgr)->IsWarmedUp(row_cnt) && ((PageBufferManager *) g_bottom_buf_mgr)->IsWarmedUp(page_cnt, true); 
  }
  return false;
}

void ARDB::PrintWarmedUpBuffer() {
  if (g_buf_type == PGBUF || g_buf_type == HYBRBUF) {
    PageBufferManager * buf_mgr = (PageBufferManager *)((g_buf_type == HYBRBUF)? g_bottom_buf_mgr: g_buf_mgr); 
    LOG_DEBUG("Finish All Warm Up: page bufferd %ld, idx pages num = %ld", ((PageBufferManager *)buf_mgr)->GetAllocated(), ((PageBufferManager *)buf_mgr)->GetIdxPageNum());
  }
}


//only used in data generation phase
void ARDB::BatchInitInsert(OID tbl, OID partition, std::multimap<std::string, std::string> &map) {
  g_data_store->WriteBatch(tables_[tbl]->GetStorageId(), std::to_string(partition), map);
}

// only used in page-based index data generation phase
void ARDB::InitInsert(OID tbl, char *data, size_t sz) {
  tables_[tbl]->InitInsertTuple(data, sz);
}

// used in cache warmpup and data generation phase
void ARDB::InitInsertWithPK(OID tbl, char *data, size_t sz, uint64_t pk, bool is_negative , bool unset_gapbit) {
  tables_[tbl]->InitInsertTupleWithPK(data, sz, pk, is_negative,unset_gapbit);
}

bool ARDB::DeleteWithPK(OID tbl, uint64_t pk, bool is_tombstone) {
  return tables_[tbl]->DeleteWithPK(pk, is_tombstone);
}


void ARDB::FinishLoadingData(OID tbl_id) {
  tables_[tbl_id]->FinishLoadingData();
}

void ARDB::PrintIndex(OID tbl_id) {
  tables_[tbl_id]->PrintIndex();
}


void ARDB::WaitForAsyncBatchLoading(int cnt) {
  g_data_store->WaitForAsyncBatchLoading(cnt);
}

void ARDB::CommitTask(ITxn* txn) {
  txn->Commit();
  DEALLOC(txn);
};

size_t ARDB::GetTotalPgCnt() {
  size_t cnt = 0;
  for (auto & tbl : tables_) {
    cnt += tbl->GetPgCnt();
  }
  return cnt;
}

void ARDB::FinishWarmupCache() {
  for (auto & tbl : tables_) {
    tbl->FinishWarmupCache();
  }
}

}
