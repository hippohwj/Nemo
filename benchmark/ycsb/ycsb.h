#ifndef ARBORETUM_BENCHMARK_YCSB_H_
#define ARBORETUM_BENCHMARK_YCSB_H_

#include "ycsb/YCSBConfig.h"
#include "ycsb/YCSBWorkload.h"
#include "common/BenchWorker.h"
#include "db/ARDB.h"

namespace arboretum {
namespace ycsb {

void ycsb(ARDB *db, YCSBConfig *config) {
  LOG_INFO("Start ycsb workload");
  auto workload = YCSBWorkload(db, config);
  std::vector<std::thread> threads;
  BenchWorker workers[config->num_workers_];
  for (size_t i = 0; i < config->num_workers_; i++) {
    workers[i].worker_id_ = i;
    threads.emplace_back(YCSBWorkload::Execute, &workload, &workers[i]);
  }
  // auto cnt = g_buf_type == PGBUF ? db->GetTotalPgCnt() : config->num_rows_;
  auto page_cnt = (g_buf_type == PGBUF || g_buf_type == HYBRBUF) ? db->GetTotalPgCnt(): 0; 
  auto row_cnt = config->num_rows_;
  // while (!g_warmup_finished) {
  //   sleep(config->warmup_time_);
  //   if (arboretum::ARDB::CheckBufferWarmedUp(row_cnt, page_cnt)) {
  //     g_warmup_finished = true;
  //     LOG_INFO("Finished warming up");
  //     arboretum::ARDB::PrintWarmedUpBuffer();
  //     break;
  //   }
  // }

  sleep(config->warmup_time_);
  g_warmup_finished = true;
  LOG_INFO("Finished warming up");
  arboretum::ARDB::PrintWarmedUpBuffer();



  
  sleep(config->runtime_);
  // join worker threads
  db->Terminate(threads);
  for (size_t i = 0; i < config->num_workers_; i++) {
    BenchWorker::sum_bench_stats_.SumUp(workers[i].bench_stats_);
  }
  BenchWorker::PrintStats(config);
  if (g_save_output) {
    g_out_file.open(g_out_fname, std::ios_base::app);
    g_out_file << "{" << g_out_str.str() << "}" << std::endl;
  }
}


} // ycsb
} // arboretum

#endif //ARBORETUM_BENCHMARK_YCSB_H_
