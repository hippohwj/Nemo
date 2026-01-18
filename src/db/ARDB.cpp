
#include "ARDB.h"
#include "ITxn.h"
#include "buffer/ObjectBufferManager.h"
#include "buffer/PageBufferManager.h"
#include "common/Stats.h"
#include "common/Worker.h"
#include "local/ITable.h"
#include "remote/IDataStore.h"
#include "remote/ILogStore.h"


using boost::property_tree::ptree;

namespace arboretum {
__thread std::mt19937 * ARDB::rand_generator_;

ARDB::ARDB() {
  CalculateCPUFreq();
  // init classes
  LOG_INFO("Start ARDB instance.")
  g_stats = NEW(Stats);
  g_data_store = NEW(IDataStore);
  g_log_store = NEW(ILogStore);
  if (g_buf_type == OBJBUF) {
    g_buf_mgr = NEW(ObjectBufferManager)(this);
    // if (g_batch_eviction) {
    ((ObjectBufferManager *)g_buf_mgr)->StartEvictThds();
    // }
  } else if (g_buf_type == PGBUF) {
    g_buf_mgr = NEW(PageBufferManager)(this);
  } else if (g_buf_type == HYBRBUF) {
    // g_top_buf_mgr = NEW(ObjectBufferManager)(this, g_top_tree_buf_sz);
    //TODO(nemo): need to start eviction threads for top tree
    g_top_buf_mgr = NEW(ObjectBufferManager)(this);
    g_buf_mgr = g_top_buf_mgr;
    g_bottom_buf_mgr =  NEW(PageBufferManager)(this);
  }
  // create commit threads
  // commit_thd_pool_.Init(g_num_worker_threads * g_commit_pool_sz);
  g_enable_phantom_protection = (g_phantom_protection_type == PHANTOM_NEXT) || (g_phantom_protection_type == NEXT_KEY) || g_enable_phantom_protection;

  // create log threads
  g_log_store->StartLogThd();
  // create eviction threads
}

void ARDB::StartBackgroundEviction() {
  if (g_buf_type == OBJBUF) {
  // if (g_batch_eviction) {
  ((ObjectBufferManager *)g_buf_mgr)->StartEvictThds();
  // }
  }
}

void ARDB::LoadConfig(int argc, char * argv[]) {
  // load configuration
  if (argc > 1) {
    memcpy(g_config_fname, argv[1], 100);
  }
  ptree root;
  LOG_INFO("Loading DB Config from file: %s", g_config_fname);
  read_json(g_config_fname, root);
  auto config_name = root.get<std::string>("config_name");
  LOG_INFO("Loading DB Config: %s", config_name.c_str());
  for (ptree::value_type &item : root.get_child("db_config")){
    DB_CONFIGS(IF_GLOBAL_CONFIG2, IF_GLOBAL_CONFIG3,
               IF_GLOBAL_CONFIG4, IF_GLOBAL_CONFIG5)
    if (item.first == "g_out_fname") {
      std::strcpy(g_out_fname, item.second.get_value<std::string>().c_str());
      continue;
    }
  }
  if (!g_enable_group_commit) {
    g_commit_group_sz = 0;
  }
  if (g_buf_type == PGBUF) {
    g_pagebuf_num_slots = PageBufferManager::CalculateNumSlots(g_total_buf_sz);
  }
  if (g_buf_type == HYBRBUF) {
    g_pagebuf_num_slots = PageBufferManager::CalculateNumSlots(g_bottom_tree_buf_sz);
  }

  if (g_workingset_partition_num != 0) {
    g_scan_zipf_partition_sz = g_partition_covering_lock_unit_sz;
  }
  DB_CONFIGS(PRINT_DB_CONFIG2, PRINT_DB_CONFIG3, PRINT_DB_CONFIG4, PRINT_DB_CONFIG5)
}

OID ARDB::CreateTable(std::string tbl_name, ISchema * schema) {
  OID tbl_id = table_cnt_++;
  auto table = NEW(ITable)(std::move(tbl_name), tbl_id, schema);
  tables_.push_back(table);
  return tbl_id;
}

OID ARDB::CreateIndex(OID tbl_id, OID col, IndexType tpe, BufferType buff_tpe) {
  return tables_[tbl_id]->CreateIndex(col, tpe, buff_tpe);
}

RC ARDB::InsertTuple(SearchKey& pkey, OID tbl, char *data, size_t sz, ITxn * txn) {
  return txn->InsertTuple(pkey, tables_[tbl], data, sz);
}

RC ARDB::GetTuple(OID tbl_id, SearchKey key, std::string &data) {
  // index look up tuple
  if (g_buf_type == NOBUF || g_buf_type == OBJBUF) {
    return g_data_store->Read(GetTable(tbl_id)->GetStorageId(), key, data);
  }
  LOG_ERROR("not supported yet.");
}

RC ARDB::GetTuple(OID tbl_id, OID idx_id, SearchKey key, char *&data, size_t &sz,
                  AccessType ac, ITxn *txn) {
  // index look up tuple
  auto table = tables_[tbl_id];
  // if idx_id = 0, default is searching from primary index
  auto rc = txn->GetTuple(table, idx_id, key, ac, data, sz);
  return rc;
}

RC ARDB::GetTuples(OID tbl_id, OID idx_id, SearchKey low_key, SearchKey high_key, ITxn *txn) {
  // index look up tuple
  auto table = tables_[tbl_id];
  // if idx_id = 0, default is searching from primary index
  return txn->GetTuples(table, idx_id, low_key, high_key);
}

ITxn *ARDB::StartTxn(ITxn *txn) {
  Worker::IncrMaxThdTxnId();
  auto txn_id = g_num_worker_threads * Worker::GetMaxThdTxnId() + Worker::GetThdId();
  return new (MemoryAllocator::Alloc(sizeof(ITxn))) ITxn(txn_id, this, txn);
}

RC ARDB::CommitTxn(ITxn * txn, RC rc) {
  if (rc == ABORT) {
    txn->Abort();
    return ABORT;
  }
  txn->PreCommit();
  if (g_terminate_exec) return RC::ABORT;
  auto commit_thd = Worker::AssignCommitThd();
  // commit_thd_pool_.SubmitTask(commit_thd, ARDB::CommitTask, txn);
  CommitTask(txn);
  // txn->Commit();
  // DEALLOC(txn);
  return RC::OK;
}


void ARDB::Terminate(std::vector<std::thread> & threads) {
  g_terminate_exec = true;
  LOG_INFO("Terminating execution.");
  // join commit threads
  commit_thd_pool_.Join();
  LOG_INFO("Joined all commit threads.");
  std::for_each(threads.begin(), threads.end(), std::mem_fn(&std::thread::join));
  LOG_INFO("Joined all worker threads.");
  g_terminate_evict = true;

  if (g_batch_eviction) {
    if (g_buf_type == OBJBUF) {
      ((ObjectBufferManager *)g_buf_mgr)->StopEvictThds();
    } else {
      //TODO: need to implement periodic (batch) eviction for page buffer and two-tree
    }
    LOG_INFO("Joined all eviction threads.");
  }


  g_stats->SumUp();
  g_stats->Print();
}

void ARDB::RestoreTable(OID tbl_id,  uint64_t num_rows, uint64_t data_domain_sz) {
  tables_[tbl_id]->RestoreTable(num_rows, data_domain_sz);
}

} // arboretum