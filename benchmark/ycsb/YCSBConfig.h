#ifndef ARBORETUM_BENCHMARK_YCSB_YCSBCONFIG_H_
#define ARBORETUM_BENCHMARK_YCSB_YCSBCONFIG_H_

#include "common/BenchConfig.h"

namespace arboretum {

#define YCSB_CONFIG(x) x(uint64_t, runtime_, 20) x(uint64_t, warmup_time_, 10) x(uint64_t, warmup_scan_max_iter_, 1000000) x(size_t, skew_query_shuffle_seed_, 0) \
x(uint64_t, num_rows_, 1024) x(size_t, num_req_per_query_, 16) x(uint64_t, domain_size_, 2048) x(uint64_t, dataload_domain_size_, 2048)                 \
x(double, zipf_theta_, 0) x(double, read_perc_, 0.5)  x(double, zipf_rotate_, 0.4)                           \
x(double, rw_txn_perc_, 1.0) x(double, insert_txn_perc_, 0.0)                  \
x(int64_t, loading_startkey, 0) x(int64_t, loading_endkey, 0)                  \
BENCH_CONFIG(x)

class YCSBConfig : public BenchConfig {
 public:
  YCSBConfig(int argc, char * argv[]) : BenchConfig() {
    if (argc > 1) {
      std::strncpy(g_config_fname, argv[1], 100);
    }
    LOG_INFO("Loading YCSB Config from file: %s", g_config_fname);
    read_json(g_config_fname, config_);
    auto config_name = config_.get<std::string>("config_name");
    LOG_INFO("Loading YCSB Config: %s", config_name.c_str());
    for (ptree::value_type &item : config_.get_child("ycsb_config")){
      YCSB_CONFIG(IF_GLOBAL_CONFIG3)
    }
    num_workers_ = g_num_worker_threads;
    YCSB_CONFIG(PRINT_BENCH_CONFIG)
    M_ASSERT(g_pagebuf_num_slots >= g_num_worker_threads * num_req_per_query_,
             "Please increase buffer size to be at least "
             "g_num_worker_threads * num_req_per_query_ * page size");
  };
  YCSB_CONFIG(DECL_BENCH_CONFIG)
};

}

#endif //ARBORETUM_BENCHMARK_YCSB_YCSBCONFIG_H_
