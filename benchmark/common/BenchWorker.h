#ifndef ARBORETUM_BENCHMARK_BENCHWORKER_H_
#define ARBORETUM_BENCHMARK_BENCHWORKER_H_

#include "common/Common.h"
#include "common/Stats.h"
#include "common/Worker.h"
#include "ycsb/YCSBConfig.h"

namespace arboretum {

#define BENCH_INT_STATS(x) x(rw_commit_cnt_, 0) x(insert_commit_cnt_, 0) \
x(scan_commit_cnt_, 0)
#define BENCH_TIME_STATS(x) x(thd_runtime_, 0) x(rw_txn_latency_, 0) \
x(scan_txn_latency_, 0) x(insert_txn_latency_, 0) x(rw_txn_user_latency_, 0) \
x(scan_txn_user_latency_, 0) x(insert_txn_user_latency_, 0)

struct BenchStats {
  BENCH_INT_STATS(DECL_INT_STATS)
  BENCH_TIME_STATS(DECL_TIME_STATS)
  BENCH_INT_STATS(DEFN_INC_INT_FUNC)
  BENCH_TIME_STATS(DEFN_INC_TIME_FUNC)
  void SumUp(BenchStats & stat) {
    BENCH_TIME_STATS(SUM_TIME_STATS)
    BENCH_INT_STATS(SUM_INT_STATS)
  };
  void Print() {
    BENCH_INT_STATS(PRINT_INT_STATS)
    BENCH_TIME_STATS(PRINT_TIME_STATS)
  };
};

class BenchWorker : public Worker {
 public:
  BenchWorker();

  void SumUp() {
    sum_bench_stats_.SumUp(bench_stats_);
  }

  // static void PrintStats(uint64_t num_worker_threads) {
static void PrintStats(YCSBConfig *config) {
    uint64_t num_worker_threads = config->num_workers_; 
    LOG_INFO("Print Bench Stats:");
    sum_bench_stats_.Print();
    auto avg_runtime = sum_bench_stats_.thd_runtime_ms_ / 1000.0 / num_worker_threads;
    std::printf("Average runtime (sec) of each worker: %.2f\n", avg_runtime);
    // auto throughput = g_stats->sum_commit_stats_.commit_cnt_ / (config->runtime_ * 1.0);
    auto throughput = g_stats->sum_db_stats_.txn_cnt_ /(config->runtime_ * 1.0);
    // std::printf("Commit Cnt: %ld \n", g_stats->sum_commit_stats_.commit_cnt_);
    // std::printf("Throughput (txn/sec): %.2f\n", g_stats->sum_commit_stats_.commit_cnt_ / (config->runtime_ * 1.0));
    std::printf("Throughput (txn/sec): %.2f\n", throughput );
    if (g_save_output) {
      g_out_str << "\"avg_per_worker_runtime_sec\": " << avg_runtime << ", ";
      g_out_str << "\"throughput_txn_per_sec\": " << throughput;
    }
  }

  OID worker_id_{};
  BenchStats bench_stats_;
  static BenchStats sum_bench_stats_;
};

} // arbotretum

#endif //ARBORETUM_BENCHMARK_BENCHWORKER_H_
