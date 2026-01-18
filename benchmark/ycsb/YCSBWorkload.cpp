#include <cmath>
#include "YCSBWorkload.h"
#include "common/BenchWorker.h"
#include "ITxn.h"
#include "db/buffer/PageBufferManager.h"
#include "db/buffer/ObjectBufferManager.h"
#include <cmath> 
#include <chrono>
#include <unordered_set>  
// #include <cstddef>   // for size_t
#include <limits>    // for std::numeric_limits

namespace arboretum {

YCSBWorkload::YCSBWorkload(ARDB* db, YCSBConfig* config) : Workload(db) {
  config_ = config;
  std::string schema_file = (g_record_size == 1008) ? "configs/schema_default.cfg"
                                                    : "configs/schema_record" + std::to_string(g_record_size) + ".cfg";

  LOG_DEBUG("Start init schema %s", schema_file.c_str());
  std::cout.flush();
  std::ifstream in(schema_file);
  if (!in) {
    std::cerr << "Error: Unable to open file " << schema_file << std::endl;
    std::cout.flush();
    throw std::runtime_error("Schema file not found: " + schema_file);
  }

  if (g_record_size == 1008) {
      std::istringstream in(YCSB_schema_string);
      InitSchema(in);
  } else {
      InitSchema(in);
  }
  in.close();  // Close the file
  LOG_DEBUG("End init sechma");
  arboretum::ARDB::InitRand(config->num_workers_ * 2);
  CalculateDenom();
  if (g_restore_from_remote) {
    // only for benchmark, warmup the cache
    LOG_DEBUG("Start Warm Up");
    WarmupCache();
    // db_->RestoreTable(tables_["MAIN_TABLE"], config_->num_rows_, config_->dataload_domain_size_);
    db_->RestoreTable(tables_["MAIN_TABLE"], config_->num_rows_, config_->domain_size_);
    LOG_INFO("Finished warming up cache for YCSB workload");
  } else {
    // only for generate databset (batch data loading)
    LoadData();
  }
  // db->StartBackgroundEviction();
  CheckData();
}

void YCSBWorkload::Execute(YCSBWorkload *workload, BenchWorker *worker) {
  arboretum::Worker::SetThdId(worker->worker_id_);
  arboretum::Worker::SetMaxThdTxnId(0);
  arboretum::ARDB::InitRand(worker->worker_id_);
  LOG_INFO("Thread-%u starts execution!", arboretum::Worker::GetThdId());
  YCSBQuery * query = nullptr;
  YCSBWorkload::QueryType tpe = YCSB_RW;
  ITxn * txn = nullptr;
  RC rc = RC::OK;
  uint64_t starttime;
  uint32_t retry = 0;
  while (!g_terminate_exec) {
    if (rc != ABORT) {
      // generate new query
      retry = 0;
      // if (txn && txn->scan_array) {
      //   DEALLOC(txn->scan_array);
      // }
      txn = nullptr;
      if (query) {
        if (tpe != YCSB_SCAN) {
          M_ASSERT(query->req_cnt ==  workload->config_->num_req_per_query_, "wrong query num %d, expected %d", query->req_cnt, workload->config_->num_req_per_query_);
        } else {
          M_ASSERT(query->req_cnt ==  2, "wrong query num for scan query, get %d, expected %d", query->req_cnt, 2);
        }
        DEALLOC(query->requests);
        DEALLOC(query);
        query = nullptr;
      }
      if (g_scan_workload_rw_thd_num == std::numeric_limits<std::size_t>::max()) {
          double r = ARDB::RandDouble();
          if (r < workload->config_->rw_txn_perc_) {
            query = workload->GenRWQuery();
            // LOG_INFO("XXX is readonly query %d", query->read_only);
            tpe = YCSB_RW;
          } else if (r < workload->config_->insert_txn_perc_ + workload->config_->rw_txn_perc_) {
            // query = workload->GenInsertQuery();
            // tpe = YCSB_INSERT;
            query = workload->GenScanQuery();
            tpe = YCSB_SCAN;

          } else {
            // query = workload->GenScanQuery();
            // tpe = YCSB_SCAN;
            query = workload->GenInsertQuery();
            tpe = YCSB_INSERT;
          }
      } else {
        //  M_ASSERT((workload->config_->num_workers_ % g_scan_workload_rw_thd_num) == 0, "num_workers_ must be able to be divided by g_scan_workload_rw_thd_num");
        //  LOG_INFO("g_scan_workload_rw_thd_num %d", g_scan_workload_rw_thd_num);
        //  LOG_INFO("current group number %d", (workload->config_->num_workers_/g_scan_workload_rw_thd_num));
        //  if ((Worker::GetThdId() % (workload->config_->num_workers_/g_scan_workload_rw_thd_num)) == 1) {
        //  if (Worker::GetThdId() % 4 == 1) {
        //  if (Worker::GetThdId() % 2 == 1) {
        // LOG_INFO("XXX Worker::GetThdId() %d, g_scan_workload_rw_thd_num %d", Worker::GetThdId(), g_scan_workload_rw_thd_num);
         if (Worker::GetThdId() < g_scan_workload_rw_thd_num) {
          //  query = workload->GenRWQuery();
          //  // LOG_INFO("XXX is readonly quecry %d", query->read_only);
          //  tpe = YCSB_RW;
          //  query = workload->GenInsertQuery();
          // tpe = YCSB_INSERT;
          query = workload->GenScanQuery();
          tpe = YCSB_SCAN;
         } else {
          // LOG_INFO("XXX Worker::GetThdId() %d, Generate Scan query", Worker::GetThdId());
          //  query = workload->GenScanQuery();
          //  tpe = YCSB_SCAN;
           query = workload->GenInsertQuery();
           tpe = YCSB_INSERT;
         }
      }
    }
    txn = workload->db_->StartTxn(txn);
    if (rc != ABORT) {
      starttime = txn->GetCurrrentStartTime();
    } 
    M_ASSERT(starttime == txn->GetStartTime(), "incorrect startime, correct: %ld, get %ld", starttime, txn->GetStartTime());
    if (tpe == YCSB_RW) {
      rc = workload->RWTxn(query, txn);
      // LOG_INFO("XXX is readonly txn %d", txn->ReadOnlyTxn());
    } else if (tpe == YCSB_INSERT) {
      rc = workload->InsertTxn(query, txn);
    } else {
      rc = workload->ScanTxn(query, txn);
    }
    // M_ASSERT(rc == RC::OK, "unexpected return code");
    workload->db_->CommitTxn(txn, rc);
    if (rc != RC::OK) {
      retry++;
      //  if (tpe != YCSB_INSERT) {
      double sleep_ms = pow(2, retry - 1) * 2;
      //  int sleep_ms_int = static_cast<int>((sleep_ms > 1 )? 1: sleep_ms);
      int sleep_ms_int = static_cast<int>((sleep_ms > 10) ? 10 : sleep_ms);
      // LOG_XXX("XXX backoff for abort with %d ms for retry %d with rc %s", sleep_ms_int, retry, RCToString(rc).c_str());
      if (tpe == YCSB_SCAN) {
        LOG_XXX("XXX backoff for scan abort with %d ms for retry %d with rc %s", sleep_ms_int, retry, RCToString(rc).c_str());
      } else if (tpe == YCSB_INSERT) {
        LOG_XXX("XXX backoff for insert abort with %d ms for retry %d with rc %s", sleep_ms_int, retry, RCToString(rc).c_str());
      }
      usleep(sleep_ms_int * 1000);
      //  }
    }
    // stats
    if (g_warmup_finished) {
      auto endtime = GetSystemClock();
      auto latency = endtime - starttime;
      worker->bench_stats_.incr_thd_runtime_(latency);
      if (rc == RC::OK) {

        // M_ASSERT(txn->HasFlush(), "XXX commit before flushed!!!!");
        auto commit_cnt = g_stats->db_stats_[Worker::GetThdId()].txn_cnt_;
        if (commit_cnt % 2000 == 0) {
          LOG_DEBUG("commit thread %u finishes %lu txns", Worker::GetThdId(), commit_cnt);
        }
        auto user_latency = endtime - txn->GetStartTime();
        g_stats->db_stats_[Worker::GetThdId()].incr_txn_latency_(latency);
        g_stats->db_stats_[Worker::GetThdId()].incr_txn_cnt_(1);
        g_stats->db_stats_[Worker::GetThdId()].incr_cc_time_(txn->GetCCTime());
        g_stats->db_stats_[Worker::GetThdId()].incr_phantom_insert_time_(txn->GetPhantomInsertTime());
        g_stats->db_stats_[Worker::GetThdId()].incr_log_time_(txn->GetLogTime());
        if (retry != 0) {
            g_stats->db_stats_[Worker::GetThdId()].incr_abort_time_(txn->GetCurrrentStartTime() - starttime);
        }
        switch(tpe) {
          case YCSB_RW:
            if (retry != 0) {
            g_stats->db_stats_[Worker::GetThdId()].incr_rw_abort_time_(txn->GetCurrrentStartTime() - starttime);
            }
            g_stats->db_stats_[Worker::GetThdId()].incr_rw_txn_latency_(latency);
            g_stats->db_stats_[Worker::GetThdId()].incr_rw_txn_cnt_(1); 
            worker->bench_stats_.incr_rw_commit_cnt_(1);
            worker->bench_stats_.incr_rw_txn_latency_(latency);
            worker->bench_stats_.incr_rw_txn_user_latency_(user_latency);
            break;
          case YCSB_SCAN:
            if (retry != 0) {
               g_stats->db_stats_[Worker::GetThdId()].incr_scan_abort_time_(txn->GetCurrrentStartTime() - starttime);
            }
            g_stats->db_stats_[Worker::GetThdId()].incr_scan_txn_latency_(latency);
            g_stats->db_stats_[Worker::GetThdId()].incr_scan_txn_cnt_(1); 
            worker->bench_stats_.incr_scan_commit_cnt_(1);
            worker->bench_stats_.incr_scan_txn_latency_(latency);
            worker->bench_stats_.incr_scan_txn_user_latency_(user_latency);
            break;
          case YCSB_INSERT:
            if (retry != 0) {
               g_stats->db_stats_[Worker::GetThdId()].incr_insert_abort_time_(txn->GetCurrrentStartTime() - starttime);
            }
            g_stats->db_stats_[Worker::GetThdId()].incr_insert_txn_latency_(latency);
            g_stats->db_stats_[Worker::GetThdId()].incr_insert_txn_cnt_(1); 
            worker->bench_stats_.incr_insert_commit_cnt_(1);
            worker->bench_stats_.incr_insert_txn_latency_(latency);
            worker->bench_stats_.incr_insert_txn_user_latency_(user_latency);
            break;
        }


        // LOG_INFO("Time to do stats %d us", (GetSystemClock() -  endtime)*1.0/1000);
      }
    }
  }
  LOG_INFO("Thread-%u finishes execution!", Worker::GetThdId());
  // summarize stats
  worker->SumUp();
}

void YCSBWorkload::LoadData() {
    LOG_DEBUG("Start Load");
    std::cout.flush();
    auto start_time = std::chrono::high_resolution_clock::now();
    if (g_buf_type == NOBUF && g_load_to_remote_only) {
      DataGenNemoBUF();
    } else {
      DataGenPGBUF();
    }
    db_->FinishLoadingData(tables_["MAIN_TABLE"]);

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(end_time - start_time).count();
    auto throughput = config_->num_rows_ / duration;

    // if (g_load_to_remote_only && g_buf_type != PGBUF) BatchLoad();
    LOG_INFO("Finished loading YCSB workload in %lld seconds, with throughput %d rows per second", duration, throughput);
}

void YCSBWorkload::WarmupNegativePoint() {
  auto sz = schemas_[0]->GetTupleSz();
  LOG_DEBUG("tuple size = %u", sz);
  char data[sz];
  strcpy(&data[schemas_[0]->GetFieldOffset(1)], "init");
  auto allocated = ((ObjectBufferManager *)g_buf_mgr)->GetAllocated();
  auto num_slots = ((ObjectBufferManager *)g_buf_mgr)->GetBufferSize();
  // M_ASSERT(!((g_workingset_partition_num != 0) || g_zipf_random_hotspots || g_scan_zipf_random_hotspots), "only support random for negative point query warmup");
  M_ASSERT(!((g_workingset_partition_num != 0) || g_scan_zipf_random_hotspots), "only support random for negative point query warmup");
  M_ASSERT(!(g_workingset_partition_num != 0 && g_scan_zipf_random_hotspots), "cannot set g_workingset_partition_num > 0 and g_scan_zipf_random_hotspots at the same time!");

  if (IsUniformDist(config_->zipf_theta_)) {
    // fill in cache using random elements
    // std::unordered_set<int> seen;  // Track selected numbers
    std::vector<int> keys(config_->domain_size_);
    std::iota(keys.begin(), keys.end(), 0); 
    std::mt19937 rng(g_negative_dataset_seed);
    std::shuffle(keys.begin(), keys.end(), rng);
    if (g_negative_search_op_enable) {
      std::unordered_set<int> sample(keys.begin(), keys.begin() + config_->num_rows_);
      uint64_t iter = 0;
      while (allocated < num_slots && iter < config_->domain_size_) {
        if ((allocated % (num_slots / 10) == 0 )|| (iter % (config_->domain_size_ / 100) == 0)) {
          LOG_DEBUG("warm up status: %ld / %lu, current iter: %ld", allocated, num_slots, iter);
        }
        // int num = ARDB::RandInteger(0, config_->domain_size_ - 1);
        int num = iter;
        uint64_t pk = num;
        schemas_[0]->SetPrimaryKey(data, pk);
        // LOG_INFO("XXX warmup pk %ld", pk);
        bool is_negative = sample.find(num) == sample.end();
        db_->InitInsertWithPK(tables_["MAIN_TABLE"], data, sz, pk, is_negative);
        allocated = ((ObjectBufferManager*)g_buf_mgr)->GetAllocated();
        iter++;
      }
    } else {
      std::vector<int> sample(keys.begin(), keys.begin() + config_->num_rows_);
      int count = 0;
      while (allocated < num_slots && count < config_->num_rows_) {
        if (allocated % (num_slots / 10) == 0) {
          LOG_DEBUG("warm up status: %ld / %lu", allocated, num_slots);
        }
        uint64_t pk = sample[count];
        schemas_[0]->SetPrimaryKey(data, pk);
        // LOG_INFO("XXX warmup pk %ld", pk);
        db_->InitInsertWithPK(tables_["MAIN_TABLE"], data, sz, pk);
        allocated = ((ObjectBufferManager*)g_buf_mgr)->GetAllocated();
        count++;
      }
    }
  } else {
    // fill in cache using Zipf TopK 
    std::unordered_set<int> seen;  // Track selected numbers
    std::vector<int> keys(config_->domain_size_);
    std::iota(keys.begin(), keys.end(), 0); 
    std::mt19937 rng(g_negative_dataset_seed);
    std::shuffle(keys.begin(), keys.end(), rng);
    // positive entries
    std::unordered_set<int> sample(keys.begin(), keys.begin() + config_->num_rows_);
    if (g_negative_search_op_enable) {
      int count = 0;   
      while (allocated < num_slots && count < config_->domain_size_) {
        if ((allocated % (num_slots / 10) == 0) || (count % (config_->domain_size_ / 100) == 0)) {
          LOG_DEBUG("warm up status: %ld / %lu, current iter: %d", allocated, num_slots, count);
        }
        uint64_t pk = GetZipfTopK(count);
        schemas_[0]->SetPrimaryKey(data, pk);
        // LOG_INFO("XXX warmup pk %ld", pk);
        bool is_negative = sample.find(pk) == sample.end();
        db_->InitInsertWithPK(tables_["MAIN_TABLE"], data, sz, pk, is_negative);
        allocated = ((ObjectBufferManager*)g_buf_mgr)->GetAllocated(); 
        count++;
      }
    } else {
      int count = 0;
      int iter = 0;
      while (allocated < num_slots && count < config_->num_rows_) {
        if (allocated % (num_slots / 10) == 0) {
          LOG_DEBUG("warm up status: %ld / %lu", allocated, num_slots);
        }
        uint64_t pk = GetZipfTopK(iter);
        bool is_negative = sample.find(pk) == sample.end();
        if (!is_negative) {
          schemas_[0]->SetPrimaryKey(data, pk);
          // LOG_INFO("XXX warmup pk %ld", pk);
          db_->InitInsertWithPK(tables_["MAIN_TABLE"], data, sz, pk);
          allocated = ((ObjectBufferManager*)g_buf_mgr)->GetAllocated();
          count++;
        }
       iter++; 
      }
    }
  }

  db_->FinishWarmupCache();
  LOG_INFO("Loaded %lu rows in memory", allocated);
  ((ObjectBufferManager *)g_buf_mgr)->PrintUsage();
}


void YCSBWorkload::WarmupNegativeScanGapbitPhantom() {
  M_ASSERT(!IsUniformDist(config_->zipf_theta_), "WarmupNegativeScanGapbitPhantom only support skewed workload now");
  auto sz = schemas_[0]->GetTupleSz();
  LOG_DEBUG("tuple size = %u", sz);
  char data[sz];
  strcpy(&data[schemas_[0]->GetFieldOffset(1)], "init");
  auto allocated = ((ObjectBufferManager *)g_buf_mgr)->GetAllocated();
  auto num_slots = ((ObjectBufferManager *)g_buf_mgr)->GetBufferSize();
  LOG_INFO("WarmupNegativeScanGapbitPhantom: warmpup buffer allocated %ld, num_slots %ld", allocated, num_slots);
  std::cout.flush();
  M_ASSERT(!((g_workingset_partition_num != 0) || g_zipf_random_hotspots || g_scan_zipf_random_hotspots), "only support random for negative scan query warmup");
  M_ASSERT(!(g_workingset_partition_num != 0 && g_scan_zipf_random_hotspots), "cannot set g_workingset_partition_num > 0 and g_scan_zipf_random_hotspots at the same time!");

  // fill in cache using Zipf TopK
  std::unordered_set<int> seen;  // Track selected numbers
  // std::vector<int> keys(config_->domain_size_);
  std::vector<int> keys(config_->dataload_domain_size_);
  std::iota(keys.begin(), keys.end(), 0);
  std::mt19937 rng(g_negative_dataset_seed);
  std::shuffle(keys.begin(), keys.end(), rng);
  // positive entries
  std::unordered_set<int> sample(keys.begin(), keys.begin() + config_->num_rows_);

  // insert min boundary to the cache to avoid corner case judges
  uint64_t min_bound = 0;
  schemas_[0]->SetPrimaryKey(data, min_bound);
  // LOG_INFO("XXX warmup pk %ld", pk);
  db_->InitInsertWithPK(tables_["MAIN_TABLE"], data, sz, min_bound, false, false);


  int count = 0;
  int iter = 0;
  // Hack: change this line
  int top_k_firsthalf = 0;
  int top_k_lasthalf = 0;
  int positive_count = 0;
  while (allocated < num_slots && iter < config_->warmup_scan_max_iter_) {
    if ((allocated % (num_slots / 10) == 0) || (iter % (config_->warmup_scan_max_iter_ / 100) == 0)) {
      LOG_DEBUG("warm up status: %ld / %lu, current iter: %d", allocated, num_slots, iter);
      std::cout.flush();
    }
    uint64_t start = GetZipfTopK(iter);
    if (start <= (config_->domain_size_ / 2)) {
      top_k_firsthalf++;
    } else {
      top_k_lasthalf++;
    }

    uint64_t end = start + g_scan_length;
    uint64_t pk = end;
    bool is_positive_request = false;
    // insert the range
    while (pk >= start) {
      bool is_negative = sample.find(pk) == sample.end();
      if (is_negative) {
        //for negative entries, only insert range bounds
        bool unset_gapbit = (pk != end) ;
        if (pk == start || pk == end) {
          schemas_[0]->SetPrimaryKey(data, pk);
          // LOG_INFO("XXX warmup pk %ld", pk);
          db_->InitInsertWithPK(tables_["MAIN_TABLE"], data, sz, pk, true, unset_gapbit);
          // allocated = ((ObjectBufferManager*)g_buf_mgr)->GetAllocated();
          allocated = ((ObjectBufferManager*)g_buf_mgr)->GetRows();
        } else {
          db_->DeleteWithPK(tables_["MAIN_TABLE"], pk, true);
        }
      } else {
        is_positive_request = true;
        // positive entries
        bool unset_gapbit = (pk != end);
        // bool unset_gapbit = true;
        schemas_[0]->SetPrimaryKey(data, pk);
        // LOG_INFO("XXX warmup pk %ld", pk);
        db_->InitInsertWithPK(tables_["MAIN_TABLE"], data, sz, pk, false, unset_gapbit);
        // allocated = ((ObjectBufferManager*)g_buf_mgr)->GetAllocated();
        allocated = ((ObjectBufferManager*)g_buf_mgr)->GetRows();
      }

      if (pk == 0) {
        break;
      } else {
        pk--;
      }
    }


    // LOG_INFO("Warmup: Print Index after an range insertion for start %ld, end %ld", start, end);
    // db_->PrintIndex(tables_["MAIN_TABLE"]);
    // std::cout.flush();

    if (is_positive_request) {
      positive_count++;
    }
    count++;
    iter++;
  }

  LOG_INFO("Finish Warmup with %d range insertions", iter);
  LOG_INFO("Top K first half %d, last half %d", top_k_firsthalf, top_k_lasthalf);
  LOG_INFO("In Warmup, Positive request count %d, Negative request count %d", positive_count, count - positive_count);

  LOG_INFO("Print Final Index after warmup");
  db_->PrintIndex(tables_["MAIN_TABLE"]);
  std::cout.flush();

  db_->FinishWarmupCache();

  LOG_INFO("Loaded %lu rows in memory", allocated);
  std::cout.flush();
  ((ObjectBufferManager *)g_buf_mgr)->PrintUsage();
}

void YCSBWorkload::WarmupNegativeScanGapbitOnly() {
  M_ASSERT(!IsUniformDist(config_->zipf_theta_), "WarmupNegativeScanGapbitOnly only support skewed workload now");
  auto sz = schemas_[0]->GetTupleSz();
  LOG_DEBUG("tuple size = %u", sz);
  char data[sz];
  strcpy(&data[schemas_[0]->GetFieldOffset(1)], "init");
  auto allocated = ((ObjectBufferManager *)g_buf_mgr)->GetAllocated();
  auto num_slots = ((ObjectBufferManager *)g_buf_mgr)->GetBufferSize();
  LOG_INFO("WarmupNegativeScanGapbitOnly: warmpup buffer allocated %ld, num_slots %ld", allocated, num_slots);
  std::cout.flush();
  M_ASSERT(!((g_workingset_partition_num != 0) || g_zipf_random_hotspots || g_scan_zipf_random_hotspots), "only support random for negative scan query warmup");
  M_ASSERT(!(g_workingset_partition_num != 0 && g_scan_zipf_random_hotspots), "cannot set g_workingset_partition_num > 0 and g_scan_zipf_random_hotspots at the same time!");

  // fill in cache using Zipf TopK
  std::unordered_set<int> seen;  // Track selected numbers
  // std::vector<int> keys(config_->domain_size_);
  std::vector<int> keys(config_->dataload_domain_size_);
  std::iota(keys.begin(), keys.end(), 0);
  std::mt19937 rng(g_negative_dataset_seed);
  std::shuffle(keys.begin(), keys.end(), rng);
  // positive entries
  std::unordered_set<int> sample(keys.begin(), keys.begin() + config_->num_rows_);


  // insert min boundary to the cache to avoid corner case judges
  uint64_t min_bound = 0;
  schemas_[0]->SetPrimaryKey(data, min_bound);
  // LOG_INFO("XXX warmup pk %ld", pk);
  db_->InitInsertWithPK(tables_["MAIN_TABLE"], data, sz, min_bound, false, false);

  // uint64_t max_bound = ~0ULL;
  // schemas_[0]->SetPrimaryKey(data,max_bound);
  // // LOG_INFO("XXX warmup pk %ld", pk);
  // db_->InitInsertWithPK(tables_["MAIN_TABLE"], data, sz, max_bound, false, false);

  int top_k_firsthalf = 0;
  int top_k_lasthalf = 0;
  int positive_count = 0;

  int count = 0;
  int iter = 0;
  while (allocated < num_slots && iter < config_->warmup_scan_max_iter_) {
    if ((allocated % (num_slots / 10) == 0) || (iter % (config_->warmup_scan_max_iter_ / 100) == 0)) {
      LOG_DEBUG("warm up status: %ld / %lu, current iter: %d", allocated, num_slots, iter);
      std::cout.flush();
    }
    // LOG_DEBUG("warm up status: %ld / %lu", allocated, iter);
    uint64_t start = GetZipfTopK(iter);
    // int64_t scan_length = g_scan_length;
    // uint64_t start = zipf((config_->domain_size_ - 1 - scan_length), config_->zipf_theta_); 
    // LOG_DEBUG("start key: %ld for iter %lu", start, iter);
    if (start <= (config_->domain_size_ / 2)) {
      top_k_firsthalf++;
    } else {
      top_k_lasthalf++;
    }
 
    uint64_t end = start + g_scan_length;
    // avoid range span partitions
    // if (( start/g_partition_sz)  != (end/g_partition_sz) ) {
    //   end = ((start/g_partition_sz) + 1) * g_partition_sz - 1;  
    // }

    uint64_t pk = end;
    // insert the range
    bool is_last_positive_inrange = true; 
    bool is_positive_request = false;
    bool is_start_with_negative = sample.find(start) == sample.end();
    if (!(is_start_with_negative && !g_scan_query_scatter)){
      while (pk >= start) {
        bool is_negative = sample.find(pk) == sample.end();
        if (!is_negative) {
          is_positive_request = true;
          bool unset_gapbit = (pk != end);
          if (is_last_positive_inrange) {
            unset_gapbit = false;
            // LOG_DEBUG("XXX set gapbit for pk %ld", pk);
            is_last_positive_inrange = false;
          }
          // bool unset_gapbit = true;
          schemas_[0]->SetPrimaryKey(data, pk);
          // LOG_INFO("XXX warmup pk %ld", pk);
          db_->InitInsertWithPK(tables_["MAIN_TABLE"], data, sz, pk, false, unset_gapbit);
  
          // allocated = ((ObjectBufferManager*)g_buf_mgr)->GetAllocated();
          allocated = ((ObjectBufferManager*)g_buf_mgr)->GetRows();
        }
  
        if (pk == 0) {
          break;
        } else {
          pk--;
        }
      }
    }
 


    if (is_positive_request) {
      positive_count++;
    }

    count++;
    iter++;
  }



  LOG_INFO("Finish Warmup with %d range insertions", iter);

  LOG_INFO("Top K first half %d, last half %d", top_k_firsthalf, top_k_lasthalf);
  LOG_INFO("In Warmup, Positive request count %d, Negative request count %d", positive_count, count - positive_count);

  LOG_INFO("Print Final Index after warmup");
  db_->PrintIndex(tables_["MAIN_TABLE"]);
  std::cout.flush();

  db_->FinishWarmupCache();

  LOG_INFO("Loaded %lu rows in memory", allocated);
  std::cout.flush();
  ((ObjectBufferManager *)g_buf_mgr)->PrintUsage();
}

void YCSBWorkload::WarmupNegativeScan() {
  switch (g_negative_search_op_type) {
      case NegativeSearchOpType::GAPBIT_PHANTOM:
          // nemo negative search optimization (gap bit + phantom bounds)
          LOG_INFO("Start warmup negative scan with gap bit + phantom bounds optimization");
          WarmupNegativeScanGapbitPhantom();
          break;

      case NegativeSearchOpType::GAPBIT_ONLY:
          // negative search with only gap bit optimization
          LOG_INFO("Start warmup negative scan with only gap bit optimization");
          std::cout.flush();
          WarmupNegativeScanGapbitOnly();
          break;

      case NegativeSearchOpType::NO_OP:
          // record cache
          // no cache needed since record cache doesn't support range query
          break;
      default:
          M_ASSERT(false, "unsupported negative search optimization type");
  }
}



void YCSBWorkload::WarmupPageNegativePoint() {
  auto sz = schemas_[0]->GetTupleSz();
  LOG_DEBUG("tuple size = %u", sz);
  // char data[sz];
  // strcpy(&data[schemas_[0]->GetFieldOffset(1)], "init");
  auto allocated = ((PageBufferManager *)g_buf_mgr)->GetAllocated();
  // auto num_slots = ((PageBufferManager *)g_buf_mgr)->GetBufferSize();
  // M_ASSERT(!((g_workingset_partition_num != 0) || g_zipf_random_hotspots || g_scan_zipf_random_hotspots), "only support random for negative point query warmup");
  // M_ASSERT(!(g_workingset_partition_num != 0 && g_scan_zipf_random_hotspots), "cannot set g_workingset_partition_num > 0 and g_scan_zipf_random_hotspots at the same time!");

  if (IsUniformDist(config_->zipf_theta_)) {
    // fill in cache using random elements
    // std::unordered_set<int> seen;  // Track selected numbers
    std::vector<int> keys(config_->domain_size_);
    std::iota(keys.begin(), keys.end(), 0); 
    std::mt19937 rng(g_negative_dataset_seed);
    std::shuffle(keys.begin(), keys.end(), rng);
    std::unordered_set<int> sample(keys.begin(), keys.begin() + config_->num_rows_);
    uint64_t iter = 0;
    while (allocated < g_pagebuf_num_slots && iter < config_->domain_size_) {
      if ((allocated % (g_pagebuf_num_slots / 10) == 0 )|| (iter % (config_->domain_size_ / 100) == 0)) {
        LOG_DEBUG("warm up page status: %ld / %lu, current iter: %ld", allocated, g_pagebuf_num_slots, iter);
        std::cout.flush();
      }
      // int num = ARDB::RandInteger(0, config_->domain_size_ - 1);
      int num = iter;
      uint64_t pk = num;
      // LOG_INFO("XXX warmup pk %ld", pk);
      bool is_negative = sample.find(num) == sample.end();
      RC rc = RC::ERROR;
      auto p_table = db_->GetTable(tables_["MAIN_TABLE"]);
      auto result = p_table->IndexSearch(SearchKey(pk), AccessType::READ, nullptr, rc, g_index_type, g_buf_type);
      if (result != nullptr) {
        auto tuple = ((PageBufferManager *) g_buf_mgr)->AccessTuple(p_table, *result, pk);
        ((PageBufferManager *) g_buf_mgr)->FinishAccessTuple(*result, false);
      }
      allocated = ((PageBufferManager*)g_buf_mgr)->GetAllocated();
      iter++;
    }
  } else {
    // fill in cache using Zipf TopK 
    std::unordered_set<int> seen;  // Track selected numbers
    std::vector<int> keys(config_->domain_size_);
    std::iota(keys.begin(), keys.end(), 0); 
    std::mt19937 rng(g_negative_dataset_seed);
    std::shuffle(keys.begin(), keys.end(), rng);
    // positive entries
    std::unordered_set<int> sample(keys.begin(), keys.begin() + config_->num_rows_);
      int count = 0;   
      while (allocated < g_pagebuf_num_slots && count < config_->domain_size_) {
        if ((allocated % (g_pagebuf_num_slots / 10) == 0) || (count % (config_->domain_size_ / 100) == 0)) {
          LOG_DEBUG("warm up page status: %ld / %lu, current iter: %d", allocated, g_pagebuf_num_slots, count);
          std::cout.flush();
        }
        uint64_t pk = GetZipfTopK(count);
        // LOG_INFO("XXX warmup pk %ld", pk);
        bool is_negative = sample.find(pk) == sample.end();
        RC rc = RC::ERROR;
        auto p_table = db_->GetTable(tables_["MAIN_TABLE"]);
        auto result = p_table->IndexSearch(SearchKey(pk), AccessType::READ, nullptr, rc, g_index_type, g_buf_type);
        if (result != nullptr) {
          auto tuple = ((PageBufferManager *) g_buf_mgr)->AccessTuple(p_table, *result, pk);
          ((PageBufferManager *) g_buf_mgr)->FinishAccessTuple(*result, false);
        }

        allocated = ((PageBufferManager*)g_buf_mgr)->GetAllocated(); 
        count++;
      }

  }
  LOG_INFO("Loaded %lu pages in memory with %ld slots", allocated, g_pagebuf_num_slots);

  LOG_INFO("Print Final Index after warmup");
  db_->PrintIndex(tables_["MAIN_TABLE"]);
  std::cout.flush();

  // ((PageBufferManager *)g_buf_mgr)->PrintUsage();
}

void YCSBWorkload::WarmupPageNegativeScan() {
  M_ASSERT(!IsUniformDist(config_->zipf_theta_), "WarmupPageNegativeScan only support skewed workload now");
  auto allocated = ((PageBufferManager *)g_buf_mgr)->GetAllocated();
    // fill in cache using Zipf TopK 
    std::unordered_set<int> seen;  // Track selected numbers
    std::vector<int> keys(config_->domain_size_);
    std::iota(keys.begin(), keys.end(), 0); 
    std::mt19937 rng(g_negative_dataset_seed);
    std::shuffle(keys.begin(), keys.end(), rng);
    // positive entries
    std::unordered_set<int> sample(keys.begin(), keys.begin() + config_->num_rows_);
      int count = 0;   
      auto p_table = db_->GetTable(tables_["MAIN_TABLE"]);
    std::unordered_set<OID> loaded_pages;

    auto gbm = (PageBufferManager *) g_buf_mgr;
      while (allocated < g_pagebuf_num_slots && count < config_->domain_size_) {
        if ((allocated % (g_pagebuf_num_slots / 10) == 0) || (count % (config_->domain_size_ / 100) == 0)) {
          LOG_DEBUG("warm up page status: %ld / %lu, current iter: %d", allocated, g_pagebuf_num_slots, count);
          std::cout.flush();
        }

        uint64_t start = GetZipfTopK(count);
        uint64_t end = start + g_scan_length;
        RC rc = RC::OK;
        size_t cnt = 0;

        auto result = p_table->IndexRangeSearch(SearchKey(start), SearchKey(end), 1, nullptr, rc, cnt);
        M_ASSERT(rc == RC::OK, "WarmUp: Range search failed for range %ld to %ld", start, end);

        std::unordered_set<OID> pages;
        for (size_t i = 0; i < cnt; i++) {
            auto item_tag = result[i].ReadAsPageTag();
            if (loaded_pages.find(item_tag.pg_id_) == loaded_pages.end()) {
              loaded_pages.insert(item_tag.pg_id_);
              pages.insert(item_tag.pg_id_);          
            }
        }
        for (auto page : pages) {
          auto data_pg = gbm->AccessPage(p_table->GetTableId(), page);
          gbm->FinishAccessPage(data_pg, false);
        }
        allocated = ((PageBufferManager*)g_buf_mgr)->GetAllocated(); 
        count++;
      }

  LOG_INFO("Loaded %lu pages in memory with %ld slots", allocated, g_pagebuf_num_slots);
  // LOG_INFO("Print Final Index after warmup");
  // db_->PrintIndex(tables_["MAIN_TABLE"]);
  // std::cout.flush();
}



void YCSBWorkload::WarmupCache() {
  LOG_INFO("ITuple size = %lu", sizeof(ITuple));
  if (g_buf_type == NOBUF) {
    return;
  } else if (g_buf_type == PGBUF) {
    if (g_negative_point_wl) {
      WarmupPageNegativePoint();
    } else if (g_negative_scan_wl) {
      WarmupPageNegativeScan();
    } else {
      return;
    }
  } else if (g_buf_type == OBJBUF) {
    if (g_negative_point_wl) {
      WarmupNegativePoint();
    } else if (g_negative_scan_wl) {
      WarmupNegativeScan();
    } else {
      auto sz = schemas_[0]->GetTupleSz();
      LOG_DEBUG("tuple size = %u", sz);
      char data[sz];
      strcpy(&data[schemas_[0]->GetFieldOffset(1)], "init");
      auto allocated = ((ObjectBufferManager *)g_buf_mgr)->GetAllocated();
      auto num_slots = ((ObjectBufferManager *)g_buf_mgr)->GetBufferSize();
      M_ASSERT(!(g_workingset_partition_num != 0 && g_scan_zipf_random_hotspots), "cannot set g_workingset_partition_num > 0 and g_scan_zipf_random_hotspots at the same time!");
      for (int64_t key = 0; allocated < num_slots && key < config_->num_rows_; key++) {
        if (key % (num_slots / 10) == 0) {
          LOG_DEBUG("warm up status: %ld / %lu", allocated, num_slots);
        }
        // auto node_id = 0;
        // auto g_num_nodes = 1;
        // uint64_t row_id = zipf(config_->num_rows_ - 1, config_->zipf_theta_);
        // uint64_t primary_key = row_id * g_num_nodes + node_id;
              // uint64_t pk = GetZipfTopK(key);
  
        // uint64_t pk = g_zipf_random_hotspots ? GetZipfTopK(key): key;
        uint64_t pk = 0;
        if (g_workingset_partition_num != 0) {
          if ((key/g_scan_zipf_partition_sz) > g_workingset_partition_num) {
            // all working sets are now in memory
            LOG_INFO("Finish loading for warmup at idx %ld", key/g_scan_zipf_partition_sz);
            break;
          }
          // uint64_t partition_id = GetZipfTopK(key/g_scan_zipf_partition_sz); 
          uint64_t partition_id = key/g_scan_zipf_partition_sz; 
          pk = partition_id * g_scan_zipf_partition_sz + (key % g_scan_zipf_partition_sz);
          // LOG_INFO("loading parition %ld, for pk %ld ", partition_id, pk);
        } else if (g_zipf_random_hotspots) {
          pk = GetZipfTopK(key); 
        } else if (g_scan_zipf_random_hotspots) {
          uint64_t partition_id = GetZipfTopK(key/g_scan_zipf_partition_sz); 
          pk = partition_id * g_scan_zipf_partition_sz + (key % g_scan_zipf_partition_sz);
        } else {
          pk = key;
        }
  
        schemas_[0]->SetPrimaryKey(data, pk);
        // LOG_INFO("XXX warmup pk %ld", pk);
        db_->InitInsertWithPK(tables_["MAIN_TABLE"], data, sz, pk);
        allocated = ((ObjectBufferManager *)g_buf_mgr)->GetAllocated();
      }
      db_->FinishWarmupCache();
      LOG_INFO("Loaded %lu rows in memory", allocated);
      ((ObjectBufferManager *)g_buf_mgr)->PrintUsage();
    }
  } else if (g_buf_type == HYBRBUF) {
    // M_ASSERT(false, "warmup doesn't support HYBTBUF");
    auto sz = schemas_[0]->GetTupleSz();
    LOG_DEBUG("tuple size = %u", sz);
    char data[sz];
    strcpy(&data[schemas_[0]->GetFieldOffset(1)], "init");
    auto allocated = ((ObjectBufferManager *)g_top_buf_mgr)->GetAllocated();
    auto num_slots = ((ObjectBufferManager *)g_top_buf_mgr)->GetBufferSize();
    for (int64_t key = 0; allocated < num_slots && key < config_->num_rows_; key++) {
      if (key % (num_slots / 10) == 0) {
        LOG_DEBUG("warm up status: %ld / %lu", allocated, num_slots);
      }
      // auto node_id = 0;
      // auto g_num_nodes = 1;
      // uint64_t row_id = zipf(config_->num_rows_ - 1, config_->zipf_theta_);
      // uint64_t primary_key = row_id * g_num_nodes + node_id;
            // schemas_[0]->SetPrimaryKey(data, primary_key);

      uint64_t pk = GetZipfTopK(key);
      schemas_[0]->SetPrimaryKey(data, pk);
      db_->InitInsertWithPK(tables_["MAIN_TABLE"], data, sz, pk);
      allocated = ((ObjectBufferManager *)g_top_buf_mgr)->GetAllocated();
    }
    db_->FinishWarmupCache();
    LOG_INFO("Loaded %lu rows in memory", allocated);
    ((ObjectBufferManager *)g_top_buf_mgr)->PrintUsage();
  }
}

void YCSBWorkload::BatchLoad() {
  M_ASSERT(g_buf_type != HYBRBUF, "HYBRBUF is an unexpected buffer type type for BatchLoad()");
  if (!g_load_range) {
    config_->loading_startkey = 0;
    config_->loading_endkey = config_->num_rows_;
  }
  LOG_DEBUG("Start loading data from %ld to %ld", config_->loading_startkey,
            config_->loading_endkey);

  // prepare tuple data
  auto sz = schemas_[0]->GetTupleSz();
  char data[sz];
  strcpy(&data[schemas_[0]->GetFieldOffset(1)], "init");
  // set up batch
  size_t batch_sz = 5;
  std::multimap<std::string, std::string> batch;
  auto prev_part = config_->loading_startkey / g_partition_sz;
  auto part = prev_part;
  auto num_batches = 0;
  auto num_parallel = 4;
  int64_t progress = 0;
  // start insertion
  for (int64_t key = config_->loading_startkey; key < config_->loading_endkey; key++) {
    part = key / g_partition_sz;
    if (part != prev_part || (progress % batch_sz == 0 && progress != 0)) {
      // flush batch
      db_->BatchInitInsert(tables_["MAIN_TABLE"], part, batch);
      batch.clear();
      prev_part = part;
      num_batches++;
      // block until num_parallel batches are flushed
      if (num_batches % num_parallel == 0) {
        db_->WaitForAsyncBatchLoading(num_parallel);
        LOG_DEBUG("Loading progress: %ld / %ld (current key = %ld) ",
                  progress, config_->loading_endkey - config_->loading_startkey, key);
      }
    }
    // set primary key
    schemas_[0]->SetPrimaryKey(data, key);
    // insert tuple into current batch
    batch.insert({SearchKey(key).ToString(), std::string(data, sz)});
    progress++;
  }
  if (!batch.empty()) {
    db_->BatchInitInsert(tables_["MAIN_TABLE"], part, batch);
  }
  // if not loading partial ranges, flush metadata
  if (!g_load_range) {
    db_->FinishLoadingData(tables_["MAIN_TABLE"]);
  }
  LOG_DEBUG("Finish loading data from %ld to %ld", config_->loading_startkey,
            config_->loading_endkey);
}


void YCSBWorkload::ParallelLoad(int thd_id, int num_thds, uint64_t row_cnt) {
    Worker::SetThdId(thd_id);
    auto starttime = GetSystemClock();
    auto progress = 0;
    char data[schemas_[0]->GetTupleSz()];
    strcpy(&data[schemas_[0]->GetFieldOffset(1)], "init");
    int counter = 0;
    for (int64_t key = 0; (uint64_t) key < row_cnt; key++) {
        if (key % num_thds == thd_id) {
         if (thd_id == 0 && (counter % ((row_cnt / num_thds)/ 100) == 0) && progress <= 100) {
          LOG_DEBUG("Loading progress: %3d %%", progress);
          std::cout.flush();
          progress++;
        }
        // set primary key
        schemas_[0]->SetPrimaryKey((char *) data, key);
        // LOG_DEBUG("[Thd-%d] XXX Tuple Loading key: %ld", thd_id, key);
        db_->InitInsertWithPK(tables_["MAIN_TABLE"], data,schemas_[0]->GetTupleSz(), key);
        counter++;
        }
    }
}


void YCSBWorkload::ParallelLoadForNegativeDataset(vector<int>& sample, int thd_id, int num_thds, size_t start, size_t end) {
  Worker::SetThdId(thd_id);
  auto starttime = GetSystemClock();
  auto progress = 0;
  char data[schemas_[0]->GetTupleSz()];
  strcpy(&data[schemas_[0]->GetFieldOffset(1)], "init");
  int counter = 0;
  for (size_t i = start; i < end; i++) {
    if (thd_id == 0 && ((counter % ((end - start) / 100)) == 0) && progress <= 100) {
      LOG_DEBUG("Loading progress: %3d %%", progress);
      std::cout.flush();
      progress++;
    }
    // set primary key
    uint64_t key = sample[i]; 
    schemas_[0]->SetPrimaryKey((char*)data, key);
    // LOG_DEBUG("[Thd-%d] XXX Tuple Loading key: %ld", thd_id, key);
    db_->InitInsertWithPK(tables_["MAIN_TABLE"], data, schemas_[0]->GetTupleSz(), key);
    counter++;
  }
}


void YCSBWorkload::DataGenNemoBUF() {
  // only for object buff data laoding
  char data[schemas_[0]->GetTupleSz()];
  strcpy(&data[schemas_[0]->GetFieldOffset(1)], "init");

  if (g_negative_point_wl) {
    // generate data for negative search dataset (data points with holes)
    std::vector<int> keys(config_->domain_size_);
    std::iota(keys.begin(), keys.end(), 0); 
    std::random_device rd;
    // unsigned int seed = rd();  
    unsigned int seed = (g_negative_dataset_seed == 0) ? rd() : g_negative_dataset_seed;  
    LOG_INFO("DataGenNemoBUF: Shuffle Random Seed: %u", seed);
    std::cout.flush();
    std::mt19937 rng(seed);
    std::shuffle(keys.begin(), keys.end(), rng);
    std::vector<int> sample(keys.begin(), keys.begin() + config_->num_rows_);

    std::vector<std::thread> threads;
    size_t chunk_size = config_->num_rows_ / g_num_load_thds;

    for (size_t i = 0; i < g_num_load_thds; i++) {
        size_t start = i * chunk_size;
        size_t end = (i == g_num_load_thds - 1) ? config_->num_rows_ : start + chunk_size;
        threads.emplace_back(YCSBWorkload::ExecuteLoadForNegativeDataset, this,  std::ref(sample), i, g_num_load_thds, start, end);
    }
    for (auto& t : threads) t.join();

  } else {
    // generate data for other dataset (continous data points)
    std::vector<std::thread> threads;
    // load data
    for (size_t i = 0; i < g_num_load_thds; i++) {
      threads.emplace_back(YCSBWorkload::ExecuteLoad, this, i, g_num_load_thds, config_->num_rows_);
    }
    std::for_each(threads.begin(), threads.end(), std::mem_fn(&std::thread::join));
    //   auto tid = row_cnt_.fetch_add(1);
    //   auto tuple = new (MemoryAllocator::Alloc(GetTotalTupleSize())) ITuple(tbl_id_, tid);
    //   tuple->SetData(data, sz);
  }

}

void YCSBWorkload::DataGenPGBUF() {
  // only for PGBUF data laoding
  char data[schemas_[0]->GetTupleSz()];
  strcpy(&data[schemas_[0]->GetFieldOffset(1)], "init");
  M_ASSERT(g_buf_type != HYBRBUF, "HYBRBUF is an unexpected buffer type type for load");
 
  if (g_negative_point_wl) { 
       // generate data for negative search dataset (data points with holes)
       std::vector<int> keys(config_->domain_size_);
       std::iota(keys.begin(), keys.end(), 0); 
       std::random_device rd;
       // unsigned int seed = rd();  
       unsigned int seed = (g_negative_dataset_seed == 0) ? rd() : g_negative_dataset_seed;  
       LOG_INFO("DataGenNemoBUF: Shuffle Random Seed: %u", seed);
       std::cout.flush();
       std::mt19937 rng(seed);
       std::shuffle(keys.begin(), keys.end(), rng);
       std::vector<int> sample(keys.begin(), keys.begin() + config_->num_rows_);
       std::sort(sample.begin(), sample.end());
       auto progress = 0;
       for (uint64_t key = 0; key < config_->num_rows_; key++) {
         if (key % (config_->num_rows_ / 100) == 0 && progress <= 100) {
           LOG_DEBUG("Loading progress: %3d %%", progress);
           std::cout.flush();
           progress++;
         }
         // set primary key
         uint64_t pkey = sample[key]; 
         schemas_[0]->SetPrimaryKey((char*)data, pkey);
        //  LOG_DEBUG("Page Tuple Loading key: %ld", pkey);
        //  std::cout.flush();
        //  db_->InitInsert(tables_["MAIN_TABLE"], data, schemas_[0]->GetTupleSz());
         db_->InitInsertWithPK(tables_["MAIN_TABLE"], data, schemas_[0]->GetTupleSz(), pkey);
       }
  } else {
    auto progress = 0;
    for (int64_t key = 0; (uint64_t)key < config_->num_rows_; key++) {
      if (key % (config_->num_rows_ / 100) == 0 && progress <= 100) {
        LOG_DEBUG("Loading progress: %3d %%", progress);
        std::cout.flush();
        progress++;
      }
      // set primary key
      schemas_[0]->SetPrimaryKey((char*)data, key);
      db_->InitInsert(tables_["MAIN_TABLE"], data, schemas_[0]->GetTupleSz());
    }
  }
}


// void YCSBWorkload::ParallelLoadForNegativeDataset(vector<int>& sample, int thd_id, int num_thds, size_t start, size_t end) {
//   Worker::SetThdId(thd_id);
//   auto starttime = GetSystemClock();
//   auto progress = 0;
//   char data[schemas_[0]->GetTupleSz()];
//   strcpy(&data[schemas_[0]->GetFieldOffset(1)], "init");
//   int counter = 0;
//   for (size_t i = start; i < end; i++) {
//     if (thd_id == 0 && ((counter % ((end - start) / 100)) == 0) && progress <= 100) {
//       LOG_DEBUG("Loading progress: %3d %%", progress);
//       std::cout.flush();
//       progress++;
//     }
//     // set primary key
//     uint64_t key = sample[i]; 
//     schemas_[0]->SetPrimaryKey((char*)data, key);
//     // LOG_DEBUG("[Thd-%d] XXX Tuple Loading key: %ld", thd_id, key);
//     db_->InitInsertWithPK(tables_["MAIN_TABLE"], data, schemas_[0]->GetTupleSz(), key);
//     counter++;
//   }
// }





void YCSBWorkload::CheckData() {
  if (g_buf_type == PGBUF || !g_check_loaded) return;
  if (g_negative_point_wl || g_negative_scan_wl) {
    //TODO: support data check for negative search dataset
    return;
  }
  M_ASSERT(g_buf_type != HYBRBUF, "HYBRBUF is an unexpected buffer type type for CheckData()");
  auto progress = 0;
  auto batch_size = 10;
  for (int64_t key = 0; (uint64_t) key < config_->num_rows_; key++) {
    if (key % (config_->num_rows_ / 100) == 0 && progress <= 100) {
      LOG_DEBUG("Checking progress: %3d %%", progress);
      progress++;
    }
    if (key % batch_size != 0)
      continue;
    // set primary key
    std::string data;
    auto rc = db_->GetTuple(tables_["MAIN_TABLE"], SearchKey(key), data);
    if (rc == RC::OK) {
      auto col = schemas_[0]->GetPKeyColIds()[0];
      auto val = &(data[schemas_[0]->GetFieldOffset(col)]);
      auto pkey = *((int64_t *) val);
      if (pkey != key) {
        rc = RC::ERROR;
        LOG_ERROR("check key %lu not match in storage %lu", key, pkey);
      }
    } else {
      LOG_DEBUG("checking key %ld not found", key);
    }
    if (rc != RC::OK) {
      config_->loading_startkey = key;
      config_->loading_endkey = key + batch_size;
      g_load_range = true;
      BatchLoad();
    }
  }
  LOG_INFO("Finished checking all loaded data for YCSB workload");
}

YCSBQuery *YCSBWorkload::GenRWQuery() {
  auto query = NEW(YCSBQuery);
  query->req_cnt = config_->num_req_per_query_;
  query->requests = NEW_SZ(YCSBRequest, query->req_cnt);
  size_t cnt = 0;
  query->read_only = true;
  while (cnt < query->req_cnt) {
    GenRequest(query, cnt);
  }
  return query;
}

YCSBQuery* YCSBWorkload::GenScanQuery() {
  auto g_num_nodes = 1;
  auto node_id = 0;
  auto query = NEW(YCSBQuery);
  query->req_cnt = 2;
  query->read_only = true;
  query->requests = NEW_SZ(YCSBRequest, 2);
  if (g_negative_scan_wl) {
    // generate start key and end key
    // recommended zipf for 80:20 hot-cold ratio is 0.877
    // int64_t scan_length = round(ARDB::RandDouble() * g_scan_length) + 1;
    //TODO: optimize the scan length to have a mean value of g_scan_length
    int64_t scan_length = g_scan_length;
    uint64_t row_id = zipf((config_->domain_size_ - 1 - scan_length), config_->zipf_theta_); 

    query->requests[0].key = row_id * g_num_nodes + node_id;  // inclusive
    query->requests[0].ac_type = SCAN;
    query->requests[1].key = query->requests[0].key + scan_length;  // exclusive

    // pick from scan length from 1 to 100. uniformly random.
    if (( query->requests[1].key/ g_partition_sz)  != (query->requests[0].key/g_partition_sz) ) {
      query->requests[1].key = ((query->requests[0].key/g_partition_sz) + 1) * g_partition_sz - 1;  // exclusive
    }

    query->requests[1].ac_type = SCAN;
    // LOG_INFO("gen scan query with low key %ld in parition %ld, high key %ld in partition %ld, scan length %d",
    // query->requests[0].key,query->requests[0].key/g_partition_sz , query->requests[1].key,
    // query->requests[1].key/g_partition_sz,scan_length );
  } else {
    // int64_t scan_length = round(ARDB::RandDouble() * 99) + 1;
    // int64_t scan_length = round(ARDB::RandDouble() * g_scan_length) + 1;
    // generate start key and end key
    // recommended zipf for 80:20 hot-cold ratio is 0.877
    uint64_t row_id = 0;
    int64_t scan_length = round(ARDB::RandDouble() * g_scan_length) + 1;
    if ((g_workingset_partition_num != 0) || g_scan_zipf_random_hotspots) {
      int random_int =
          (g_workingset_partition_num <= 1) ? 0 : ARDB::RandIntegerInclusive(0, g_workingset_partition_num - 1);
      if (g_workingset_partition_num != 0) {
        M_ASSERT(random_int <= g_workingset_partition_num - 1, "illegal random partition int id %d, expected <= %d",
                 random_int, g_workingset_partition_num - 1);
      }
      // uint64_t parition_id = g_scan_zipf_random_hotspots ? zipf((config_->num_rows_ - 1)/g_scan_zipf_partition_sz,
      // config_->zipf_theta_): GetZipfTopK(random_int);
      uint64_t parition_id = g_scan_zipf_random_hotspots
                                 ? zipf((config_->num_rows_ - 1) / g_scan_zipf_partition_sz, config_->zipf_theta_)
                                 : random_int;
      // LOG_INFO("scan query parition id %ld, random integer %d, working set partition num %d", parition_id,
      // random_int, g_workingset_partition_num);
      if (parition_id == (config_->num_rows_ - 1) / g_scan_zipf_partition_sz) {
        // largest partition
        row_id = parition_id * g_scan_zipf_partition_sz +
                 ARDB::RandIntegerInclusive(
                     0, config_->num_rows_ - 1 - scan_length - (parition_id * g_scan_zipf_partition_sz));
      } else {
        row_id = parition_id * g_scan_zipf_partition_sz +
                 ARDB::RandIntegerInclusive(0, g_scan_zipf_partition_sz - 10 - scan_length);
      }
      // M_ASSERT(row_id <= config_->num_rows_ - 1 - scan_length, "Illegal scan row id %d which is larger than max row
      // id %d, partition id is %d", row_id, config_->num_rows_ - 1 - scan_length, parition_id);
      if (row_id > config_->num_rows_ - 1 - scan_length) {
        LOG_INFO("Illegal scan row id %d which is larger than max row id %d, partition id is %d", row_id,
                 config_->num_rows_ - 1 - scan_length, parition_id);
        row_id = config_->num_rows_ - 1 - scan_length;
      }
    } else {
      row_id = zipf(config_->num_rows_ - 1 - scan_length, config_->zipf_theta_);
    }

    query->requests[0].key = row_id * g_num_nodes + node_id;  // inclusive
    query->requests[0].ac_type = SCAN;
    // pick from scan length from 1 to 100. uniformly random.
    query->requests[1].key = query->requests[0].key + scan_length;  // exclusive
    query->requests[1].ac_type = SCAN;
    // LOG_INFO("gen scan query with low key %ld in parition %ld, high key %ld in partition %ld, scan length %d",
    // query->requests[0].key,query->requests[0].key/g_scan_zipf_partition_sz , query->requests[1].key,
    // query->requests[1].key/g_scan_zipf_partition_sz,scan_length );
    M_ASSERT((query->requests[0].key / g_scan_zipf_partition_sz) == (query->requests[1].key / g_scan_zipf_partition_sz),
             "scan cannot span partitions: gen scan query with low key %ld in parition %ld, high key %ld in partition "
             "%ld, scan length %d",
             query->requests[0].key, query->requests[0].key / g_scan_zipf_partition_sz, query->requests[1].key,
             query->requests[1].key / g_scan_zipf_partition_sz, scan_length);
  }

  return query;
}

// insert query only goes concurrently with scan query 
YCSBQuery *YCSBWorkload::GenInsertQuery() {
  auto query = GenScanQuery();
  query->req_cnt = 1;
  query->read_only = false;
  // use update to simulate insert
  query->requests[0].ac_type = UPDATE;
  return query;
}

void YCSBWorkload::GenKey(uint64_t &row_id) {
  if (g_negative_point_wl) {
    row_id = zipf((config_->domain_size_) - 1, config_->zipf_theta_); 
  } else {
    if ((g_workingset_partition_num != 0) || g_scan_zipf_random_hotspots) {
      int random_int = (g_workingset_partition_num <= 1)? 0: ARDB::RandIntegerInclusive(0, g_workingset_partition_num - 1);
      if (g_workingset_partition_num != 0) {
         M_ASSERT(random_int <= g_workingset_partition_num - 1, "illegal random partition int id %d, expected <= %d", random_int, g_workingset_partition_num - 1);
      }
      // uint64_t parition_id = g_scan_zipf_random_hotspots ? zipf((config_->num_rows_ - 1)/g_scan_zipf_partition_sz, config_->zipf_theta_): GetZipfTopK(random_int);
      uint64_t parition_id = g_scan_zipf_random_hotspots ? zipf((config_->num_rows_ - 1)/g_scan_zipf_partition_sz, config_->zipf_theta_): random_int;
      if (parition_id == (config_->num_rows_ - 1)/g_scan_zipf_partition_sz) {
        // largest partition
        row_id = parition_id * g_scan_zipf_partition_sz + ARDB::RandIntegerInclusive(0, config_->num_rows_ - 1 - (parition_id * g_scan_zipf_partition_sz)); 
      } else {
        row_id = parition_id * g_scan_zipf_partition_sz + ARDB::RandIntegerInclusive(0, g_scan_zipf_partition_sz - 1); 
      }
      // M_ASSERT(row_id <= config_->num_rows_ - 1, "Illegal row id %d which is larger than max row id %d, parititon id is ", row_id, config_->num_rows_ - 1, parition_id);
      if (row_id > config_->num_rows_ - 1) {
        LOG_INFO("Illegal row id %d which is larger than max row id %d, parititon id is ", row_id, config_->num_rows_ - 1, parition_id);
        row_id = config_->num_rows_ - 1;
      }
    } else {
      row_id = zipf(config_->num_rows_ - 1, config_->zipf_theta_);
    }

  }

}
void YCSBWorkload::GenRequest(YCSBQuery * query, size_t &cnt) {
  // TODO: add (bool) remote and g_node_id info in the future
  YCSBRequest & req = query->requests[cnt];
  auto g_num_nodes = 1;
  auto node_id = 0;
  // uint64_t row_id = zipf(config_->num_rows_ - 1, config_->zipf_theta_);
  uint64_t row_id = 0; 
  GenKey(row_id);
  uint64_t primary_key = row_id * g_num_nodes + node_id;
  // bool readonly = row_id != 0 && (int((int32_t) row_id * config_->read_perc_) >
  //     int(((int32_t) row_id - 1) * config_->read_perc_));
  bool readonly = config_->read_perc_ == 1; 
  if (readonly)
    req.ac_type = READ;
  else {
    double r = ARDB::RandDouble();
    req.ac_type = (r < config_->read_perc_) ? READ : UPDATE;
  }
  req.key = primary_key;
  req.value = 0;
  // remove duplicates
  bool exist = false;
  for (uint32_t i = 0; i < cnt; i++)
    if (query->requests[i].key == req.key)
      exist = true;
  if (!exist) {
    if (query->requests[cnt].ac_type != READ) {
      query->read_only = false;
    }
    cnt++;
  }
}

void YCSBWorkload::CalculateDenom() {
  if (!IsUniformDist(config_->zipf_theta_)) {
    assert(the_n == 0);
    uint64_t domain_size = (!(g_negative_point_wl || g_negative_scan_wl)) ? config_->num_rows_ : config_->domain_size_;
    M_ASSERT(domain_size % g_num_worker_threads == 0, "Table size must be multiples of worker threads");
    the_n = domain_size / g_num_worker_threads - 1;
    denom = zeta(the_n, config_->zipf_theta_);
    zeta_2_theta = zeta(2, config_->zipf_theta_);
    if (g_zipf_random_hotspots) {
      // shuffle the keys to ensure the hotspots are sprinkled randomly.
      shuffle_idx = new std::vector<uint64_t>(domain_size);
      std::iota(shuffle_idx->begin(), shuffle_idx->end(), 0);
      // Randomly shuffle the permuted array
      std::random_device rd;
      std::mt19937 g(rd());
      std::shuffle(shuffle_idx->begin(), shuffle_idx->end(), g);
    } else if ((g_workingset_partition_num != 0) || g_scan_zipf_random_hotspots) {
      shuffle_idx = new std::vector<uint64_t>(((domain_size - 1) / g_scan_zipf_partition_sz) + 1);
      std::iota(shuffle_idx->begin(), shuffle_idx->end(), 0);
      // Randomly shuffle the permuted array
      std::random_device rd;
      std::mt19937 g(rd());
      std::shuffle(shuffle_idx->begin(), shuffle_idx->end(), g);
    } else {
      // by default, we don't shuffle but rotate the keys to ensure the hotspots are clustered.
      shuffle_idx = new std::vector<uint64_t>(domain_size);
      std::iota(shuffle_idx->begin(), shuffle_idx->end(), 0);
      LOG_INFO("shuffle idx size is %ld", shuffle_idx->size());

      if (g_scan_query_scatter) {
        // Randomly shuffle the permuted array
        std::random_device rd;
        size_t seed = 0;
        if (config_->skew_query_shuffle_seed_ == 0) {
          seed = rd();
        } else {
          seed = config_->skew_query_shuffle_seed_;
        }
        std::mt19937 g(seed);
        std::shuffle(shuffle_idx->begin(), shuffle_idx->end(), g);
      } else {
      //   // by default, we set the hottest key to be the key in the config_->zipf_rotate_ place of the entire domain range.
      //   hotspot_start = static_cast<uint64_t>(std::round(static_cast<double>(domain_size) * (config_->zipf_rotate_)));
      //   std::rotate(shuffle_idx->begin(), shuffle_idx->begin() + hotspot_start, shuffle_idx->end());
       
      // uint64_t half = domain_size / 2;
      // for (uint64_t i = 0; i < half; i++) {
      //     // Even positions (0, 2, 4, ...) get the second-half indices.
      //     (*shuffle_idx)[2 * i] = half + i;
      //     // Odd positions (1, 3, 5, ...) get the first-half indices.
      //     (*shuffle_idx)[2 * i + 1] = half - i;
      // }

      uint64_t half = domain_size / 2;

      constexpr double EPS = 1e-9;
      if (std::abs(config_->zipf_rotate_) < EPS) {
        // treat as zero
        LOG_INFO("domain size %ld", domain_size);     
        return;
      }
      // uint64_t half = 5119780;

      // Start with the middle element
      (*shuffle_idx)[0] = half;

      // Fill the rest of the vector with elements in increasing distance from the middle
      for (uint64_t i = 1; i <= half; i++) {
        // Position 2*i-1 gets half+i (elements to the right of middle)
        if (half + i < domain_size) {
          (*shuffle_idx)[2 * i - 1] = half + i;
        }

        // Position 2*i gets half-i (elements to the left of middle)
        if (i <= half) {
          (*shuffle_idx)[2 * i] = half - i;
        }
      }
      }

      // // by default, we set the hottest key to be the key in the 1/3 place of the entire domain range.
      // hotspot_start = static_cast<uint64_t>(std::round(static_cast<double>(domain_size) * (config_->zipf_rotate_)));

      // std::rotate(shuffle_idx->begin(), shuffle_idx->begin() +  hotspot_start, shuffle_idx->end());

      // uint64_t half = domain_size / 2;
      // for (uint64_t i = 0; i < half; i++) {
      //     // Even positions (0, 2, 4, ...) get the second-half indices.
      //     (*shuffle_idx)[2 * i] = half + i;
      //     // Odd positions (1, 3, 5, ...) get the first-half indices.
      //     (*shuffle_idx)[2 * i + 1] = i;
      // }



      LOG_INFO("domain size %ld", domain_size);     
      int top_k_firsthalf = 0;
      int top_k_lasthalf = 0;
      // for (uint64_t i = 0; i < shuffle_idx->size(); i++) {
      // for (uint64_t i = 0; i < 3000; i++) {
      //   std::cout << "item "<< i << ": " << (*shuffle_idx)[i] << " ";
      //   if ( (*shuffle_idx)[i] <= (config_->domain_size_ / 2)) {
      //     top_k_firsthalf++;
      //   } else {
      //     top_k_lasthalf++;
      //   }
      // }
      // LOG_INFO("in shuffle_idx, top k in first half %d, top k in last half %d", top_k_firsthalf, top_k_lasthalf);

    }
  }
}

uint64_t YCSBWorkload::GetZipfTopK(int k) {
  M_ASSERT(!IsUniformDist(config_->zipf_theta_), "can only call GetZipfTopK when zipf theta is not 0");
  return (*shuffle_idx)[k];
}

bool YCSBWorkload::IsUniformDist(double theta) {
  double epsilon = 1e-9; // Set a small tolerance
  return std::abs(theta - 0.0) < epsilon;
}

uint64_t YCSBWorkload::zipf(uint64_t n, double theta) {
  assert(theta == config_->zipf_theta_);
  double epsilon = 1e-9;  // Set a small tolerance
  uint64_t idx = 0;
  if (IsUniformDist(theta)) {
    idx = ARDB::RandInteger(0, n - 1);
    return idx;
  } else {
    double alpha = 1 / (1 - theta);
    double zetan = denom;
    double eta = (1 - pow(2.0 / n, 1 - theta)) / (1 - zeta_2_theta / zetan);
    double u = ARDB::RandDouble();
    double uz = u * zetan;
    if (uz < 1) {
      idx = 0;
    } else if (uz < 1 + pow(0.5, theta)) {
      idx = 1;
    } else {
      idx = (uint64_t)(n * pow(eta * u - eta + 1, alpha));
    }
  }
  return (*shuffle_idx)[idx]; 
}

// The following algorithm comes from the paper:
// Quickly generating billion-record synthetic databases
// However, it seems there is a small bug.
// The original paper says zeta(theta, 2.0). But I guess it should be
// zeta(2.0, theta).
double YCSBWorkload::zeta(uint64_t n, double theta) {
  double sum = 0;
  for (uint64_t i = 1; i <= n; i++)
    sum += pow(1.0 / (int32_t) i, theta);
  return sum;
}

} // namespace