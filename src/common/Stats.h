#ifndef README_MD_SRC_COMMON_STATS_H_
#define README_MD_SRC_COMMON_STATS_H_

#include "common/Common.h"

namespace arboretum {

#define DB_INT_STATS(x) x(accesses_, 0) x(misses_, 0) x(tuple_accesses_, 0)  x(false_positive_, 0) x(true_negative_, 0)  x(negative_requests_, 0) x(positive_requests_, 0)      \
x(idx_misses_, 0) x(idx_accesses_, 0) x(bufferd_idx_pgs_, 0)  \
x(remote_rds_, 0) x(remote_wrs_, 0) x(remote_rws_, 0)         \
x(remote_scans_, 0) x(abort_cnt_, 0) x(scan_abort_cnt_, 0) x(rw_abort_cnt_, 0) x(insert_abort_cnt_, 0) x(more_than_one_scan_txns_, 0) x(txn_cnt_, 0) x(scan_txn_cnt_, 0) x(rw_txn_cnt_, 0) x(insert_txn_cnt_, 0) 
#define DB_TIME_STATS(x) x(log_time_, 0) x(cc_time_, 0) x(phantom_insert_time_, 0) x(idx_time_, 0) x(abort_time_, 0) x(commit_time_, 0) x(scan_abort_time_, 0) x(rw_abort_time_, 0) x(insert_abort_time_, 0)  x(rw_await_evict_time_, 0) \
x(remote_io_time_, 0) x(remote_rd_time_, 0)                   \
x(remote_wr_time_, 0) x(remote_rw_time_, 0) x(remote_scan_time_, 0)
#define DB_VEC_STATS(x) x(remote_scan_sz_, 0) x(txn_scans_, 0)
#define DB_VEC_TIME_STATS(x) x(txn_latency_, 0) x(rw_txn_latency_, 0) x(scan_txn_latency_, 0) x(insert_txn_latency_, 0)
#define COMMIT_INT_STATS(x) x(log_group_sz_, 0) x(num_flushes_, 0) x(log_group_payload_sz_, 0)
#define COMMIT_TIME_STATS(x) x(log_latency_, 0) x(log_flush_interval_, 0) 
#define COMMIT_VEC_STATS(x) x(log_flush_latency_, 0) 

#define DECL_INT_STATS(var, val) uint64_t var{val};
#define SUM_INT_STATS(var, val) var += stat.var;
#define RESET_INT_STATS(var, val) var = val;
#define DEFN_INC_INT_FUNC(var, val) void incr_##var(uint64_t inc) {(var) += inc;};
#define DEFN_SET_INT_FUNC(var, val) void set_##var(uint64_t inc) {(var) = inc;};
#define PRINT_INT_STATS(var, val)                                              \
std::cout << "Total " << #var << ": " << (var) << std::endl;                   \
if (g_save_output) { g_out_str << "\"" << #var << "\": " << (var) << ", "; }

#define DECL_TIME_STATS(var, val) uint64_t var##ns_{val}; double var##ms_{val};
#define SUM_TIME_STATS(var, val) var##ms_ += stat.var##ns_ / 1000000.0;
#define RESET_TIME_STATS(var, val) var##ns_ = val; var##ms_ = val;
#define DEFN_INC_TIME_FUNC(var, val) void incr_##var(uint64_t inc) { var##ns_ += inc; };
#define PRINT_TIME_STATS(var, val)                                             \
std::cout << "Total " << #var << " (ms): " << var##ms_ << std::endl;       \
if (g_save_output) { g_out_str << "\"" << #var << "ms\": " << (var##ms_) << ", "; }

#define DECL_VEC_INT_STATS(var, val) std::vector<uint64_t> var;
#define DEFN_INC_VEC_INT_FUNC(var, val) void incr_##var(uint64_t inc) { var.push_back(inc); };
#define RESET_VEC_INT_STATS(var, val) var.clear();
#define SUM_VEC_INT_STATS(var, val) var.insert(var.end(), stat.var.begin(), stat.var.end());
#define PRINT_VEC_INT_STATS(var, val)                                \
std::sort(var.begin(), var.end());                                   \
if (!var.empty()) {                                                  \
std::cout << #var << " ( 0%): "                                      \
<< var[0] << std::endl;                                              \
std::cout << #var << " ( 25%): "                                     \
<< var[var.size() * 0.25] << std::endl;                              \
std::cout << #var << " (50%): "                                      \
<< var[var.size() * 0.50] << std::endl;                              \
std::cout << #var << " (75%): "                                      \
<< var[var.size() * 0.75] << std::endl;                              \
std::cout << #var << " (100%): "                                     \
<< var[var.size() - 1] << std::endl;                                 \
if (g_save_output) {                                                 \
  g_out_str << "\"" << #var << "perc0\": " << var[0] << ", ";        \
  g_out_str << "\"" << #var << "perc25\": "                          \
  << var[var.size() * 0.25] << ", ";                                 \
  g_out_str << "\"" << #var << "perc50\": "                          \
  << var[var.size() * 0.50] << ", ";                                 \
  g_out_str << "\"" << #var << "perc75\": "                          \
  << var[var.size() * 0.75] << ", ";                                 \
  g_out_str << "\"" << #var << "perc100\": "                         \
  << var[var.size() - 1] << ", ";                                    \
}}

#define DECL_VEC_STATS(var, val) std::vector<uint64_t> var##ns_{};             \
double sum_##var##us{val};
#define SUM_VEC_STATS(var, val) var##ns_.insert(                               \
var##ns_.end(), stat.var##ns_.begin(), stat.var##ns_.end());                   \
uint64_t sum_##var##ns = 0; for (auto num : stat.var##ns_) sum_##var##ns += num;\
sum_##var##us += sum_##var##ns / 1000.0;
#define RESET_VEC_STATS(var, val) var##ns_.clear(); sum_##var##us = val;
#define DEFN_INC_VEC_FUNC(var, val) void incr_##var(uint64_t inc) { var##ns_.push_back(inc); };
#define PRINT_VEC_STATS(var, val)                                              \
std::sort(var##ns_.begin(), var##ns_.end());                                   \
if (!var##ns_.empty()) {                                                    \
std::cout << #var << " ( 0% in us): "   \
<< var##ns_[0] / 1000.0 << std::endl;       \
std::cout << #var << " (50% in us): "   \
<< var##ns_[var##ns_.size() * 0.50] / 1000.0 << std::endl;       \
std::cout << #var << " (99% in us): "                \
<< var##ns_[var##ns_.size() * 0.99] / 1000.0 << std::endl;       \
std::cout << #var << " (max in us): "                \
<< var##ns_[var##ns_.size() - 1] / 1000.0 << std::endl;       \
std::cout << #var << " (avg in us): "                \
<< sum_##var##us / (var##ns_.size() *1.0) << std::endl;       \
std::cout << #var << " (sum ): "                \
<< sum_##var##us << ", size: " << (var##ns_.size() *1.0) << std::endl;       \
uint64_t new_sum_##var##ns = 0; \
for (auto num : var##ns_) new_sum_##var##ns += num;\
std::cout << #var << " (new avg in us): "                \
<< (new_sum_##var##ns / 1000.0) / (var##ns_.size() *1.0) << std::endl;       \
if (g_save_output) {                                 \
  g_out_str << "\"" << #var << "perc0_us\": " << var##ns_[0] / 1000.0 << ", "; \
  g_out_str << "\"" << #var << "perc50_us\": "      \
  << var##ns_[var##ns_.size() * 0.50] / 1000.0 << ", ";        \
  g_out_str << "\"" << #var << "perc99_us\": "      \
  << var##ns_[var##ns_.size() * 0.99] / 1000.0 << ", ";        \
 g_out_str << "\"" << #var << "avg_us\": "      \
  <<  sum_##var##us / (var##ns_.size() *1.0) << ", ";        \
}}

#define PRINT_TIME_VEC_STATS(var)                  \
  std::sort(var##ns_.begin(), var##ns_.end());     \
  if (!var##ns_.empty()) {                         \
    if (g_save_output) {                           \
      g_out_str << "\"" << #var << "dist_ms\": ["; \
      int count = 0;                               \
      for (auto num : var##ns_) {                  \
        if (count != var##ns_.size() - 1) {         \
          g_out_str << num / 1000000.0 << ", ";    \
        } else {                                   \
          g_out_str << num / 1000000.0 << " ";     \
        }                                          \
        count++;                                   \
      }                                            \
      g_out_str << " ], ";                         \
    }                                              \
  }

#define PRINT_AVG_STATS(stat, var, cnt) {                                 \
    auto avg_##var = (stat.cnt != 0) ? (stat.var * 1.0 / stat.cnt) : 0.0; \
    printf("Average %s: %.2f\n", #var, avg_##var);                        \
    if (g_save_output) {                                                  \
      g_out_str << "\"avg_" << #var << "\": " << avg_##var << ",";        \
    }                                                                     \
  }
#define PRINT_AVG_US_STATS(stat, var, suffix, cnt, scale) {                    \
  auto avg_##var = (stat.cnt != 0)? (stat.var##suffix * (scale) * 1.0 / stat.cnt):0.0;                \
  printf("Average %s (us): %.2f, (sum %d, cnt %d)\n", #var, avg_##var, stat.var##suffix, stat.cnt);                          \
  if (g_save_output) {                                                         \
    g_out_str << "\"avg_" << #var << " (us)\": " << avg_##var << ",";         \
  }                                                                            \
}

#define PRINT_AVG_US_STATS_PER_OP(stat, var, suffix, cnt, scale) {                    \
  auto avg_##var = (stat.cnt != 0)? (stat.var##suffix * (scale) * 1.0 / stat.cnt):0.0;                \
  printf("Average %s (us) Per Op: %.2f, (sum %d, cnt %d)\n", #var, avg_##var, stat.var##suffix, stat.cnt);                          \
  if (g_save_output) {                                                         \
    g_out_str << "\"avg_" << #var << "_per_op (us)\": " << avg_##var << ",";         \
  }                                                                            \
}


struct DBStats {
  DB_INT_STATS(DECL_INT_STATS)
  DB_TIME_STATS(DECL_TIME_STATS)
  DB_INT_STATS(DEFN_INC_INT_FUNC)
  DB_INT_STATS(DEFN_SET_INT_FUNC)
  DB_TIME_STATS(DEFN_INC_TIME_FUNC)
  DB_VEC_STATS(DECL_VEC_INT_STATS)
  DB_VEC_STATS(DEFN_INC_VEC_INT_FUNC)
  DB_VEC_TIME_STATS(DECL_VEC_STATS)
  DB_VEC_TIME_STATS(DEFN_INC_VEC_FUNC)

  void Reset() {
    DB_INT_STATS(RESET_INT_STATS)
    DB_TIME_STATS(RESET_TIME_STATS)
    DB_VEC_STATS(RESET_VEC_INT_STATS)
    DB_VEC_TIME_STATS(RESET_VEC_STATS)

  };
  void SumUp(DBStats & stat) {
    DB_INT_STATS(SUM_INT_STATS)
    DB_TIME_STATS(SUM_TIME_STATS)
    DB_VEC_STATS(SUM_VEC_INT_STATS)
    DB_VEC_TIME_STATS(SUM_VEC_STATS)
  }
  void Print() {
    DB_INT_STATS(PRINT_INT_STATS)
    DB_TIME_STATS(PRINT_TIME_STATS)
    DB_VEC_STATS(PRINT_VEC_INT_STATS)
    DB_VEC_TIME_STATS(PRINT_VEC_STATS)
    if (g_enable_latency_distr_log) {
       PRINT_TIME_VEC_STATS(txn_latency_);
    }
  };
};

struct CommitStats {
  COMMIT_INT_STATS(DECL_INT_STATS)
  COMMIT_TIME_STATS(DECL_TIME_STATS)
  COMMIT_VEC_STATS(DECL_VEC_STATS)
  COMMIT_INT_STATS(DEFN_INC_INT_FUNC)
  COMMIT_TIME_STATS(DEFN_INC_TIME_FUNC)
  COMMIT_VEC_STATS(DEFN_INC_VEC_FUNC)
  void Reset() {
    COMMIT_INT_STATS(RESET_INT_STATS)
    COMMIT_TIME_STATS(RESET_TIME_STATS)
    COMMIT_VEC_STATS(RESET_VEC_STATS)
  };
  void SumUp(CommitStats & stat) {
    COMMIT_INT_STATS(SUM_INT_STATS)
    COMMIT_TIME_STATS(SUM_TIME_STATS)
    COMMIT_VEC_STATS(SUM_VEC_STATS)
  }
  void Print() {
    COMMIT_INT_STATS(PRINT_INT_STATS)
    COMMIT_TIME_STATS(PRINT_TIME_STATS)
    COMMIT_VEC_STATS(PRINT_VEC_STATS)
  }
};

class Stats {
 public:
  Stats() {
    for (size_t i = 0; i < g_num_worker_threads; i++) {
      db_stats_.emplace_back();
      for (size_t j = 0; j < g_commit_pool_sz; j++) {
        commit_stats_.emplace_back();
      }
    }
  };
  void SumUp() {
    for (size_t i = 0; i < g_num_worker_threads; i++) {
      sum_db_stats_.SumUp(db_stats_[i]);
      for (size_t j = 0; j < g_commit_pool_sz; j++) {
        sum_commit_stats_.SumUp(commit_stats_[i * g_commit_pool_sz + j]);
      }
    }
  }



  void Print() {
    LOG_INFO("Print DB stats: ");
    sum_db_stats_.Print();
    sum_commit_stats_.Print();
    PRINT_AVG_US_STATS(sum_db_stats_, sum_txn_latency, _us, txn_cnt_, 1)
    if (sum_db_stats_.scan_txn_cnt_ != 0) {
      PRINT_AVG_US_STATS(sum_db_stats_, sum_scan_txn_latency, _us, scan_txn_cnt_, 1)
    }
    if (sum_db_stats_.rw_txn_cnt_ != 0) {
      PRINT_AVG_US_STATS(sum_db_stats_, sum_rw_txn_latency, _us, rw_txn_cnt_, 1)
    }
     if (sum_db_stats_.insert_txn_cnt_ != 0) {
      PRINT_AVG_US_STATS(sum_db_stats_, sum_insert_txn_latency, _us, insert_txn_cnt_, 1)
    }
    PRINT_AVG_US_STATS(sum_db_stats_, abort_time_, ms_, txn_cnt_, 1000)
    PRINT_AVG_US_STATS(sum_db_stats_, scan_abort_time_, ms_, scan_txn_cnt_, 1000)
    PRINT_AVG_US_STATS(sum_db_stats_, insert_abort_time_, ms_, insert_txn_cnt_, 1000)
    PRINT_AVG_US_STATS(sum_db_stats_, rw_abort_time_, ms_, rw_txn_cnt_, 1000)
    PRINT_AVG_US_STATS(sum_db_stats_, cc_time_, ms_, txn_cnt_, 1000)
    PRINT_AVG_US_STATS(sum_db_stats_, idx_time_, ms_, txn_cnt_, 1000)
    PRINT_AVG_US_STATS(sum_db_stats_, phantom_insert_time_, ms_, txn_cnt_, 1000)
    PRINT_AVG_US_STATS(sum_db_stats_, log_time_, ms_, txn_cnt_, 1000)
    PRINT_AVG_US_STATS(sum_db_stats_, remote_io_time_, ms_, txn_cnt_, 1000)
    PRINT_AVG_US_STATS(sum_db_stats_, remote_rd_time_, ms_, txn_cnt_, 1000)
    PRINT_AVG_US_STATS(sum_db_stats_, remote_wr_time_, ms_, txn_cnt_, 1000)
    PRINT_AVG_US_STATS(sum_db_stats_, remote_rw_time_, ms_, txn_cnt_, 1000)
    PRINT_AVG_US_STATS(sum_db_stats_, remote_scan_time_, ms_, txn_cnt_, 1000)
    PRINT_AVG_US_STATS_PER_OP(sum_db_stats_, remote_scan_time_, ms_, remote_scans_, 1000)
    PRINT_AVG_US_STATS(sum_db_stats_, rw_await_evict_time_, ms_, txn_cnt_, 1000)

    PRINT_AVG_STATS(sum_commit_stats_, log_group_sz_, num_flushes_)
    PRINT_AVG_STATS(sum_commit_stats_, log_group_payload_sz_, num_flushes_)
    PRINT_AVG_US_STATS(sum_commit_stats_, log_flush_interval_, ms_, num_flushes_, 1000)

    PRINT_AVG_US_STATS(sum_commit_stats_, sum_log_flush_latency, _us, num_flushes_, 1)
    auto hit_rate = g_buf_type == NOBUF ? 0 : (g_buf_type == PGBUF ?  (1- (sum_db_stats_.misses_ * 1.0 / g_stats->sum_db_stats_.txn_cnt_)): (1 - (sum_db_stats_.misses_ * 1.0 / sum_db_stats_.tuple_accesses_)));
        // 1 - (sum_db_stats_.misses_ * 1.0 / sum_db_stats_.tuple_accesses_);
    auto idx_hit_rate = g_buf_type == NOBUF ? 0 : ((sum_db_stats_.idx_accesses_ == 0)? 0: (1 - (sum_db_stats_.idx_misses_ * 1.0 / sum_db_stats_.idx_accesses_)));
    auto false_positive_rate = (!(g_negative_point_wl || g_negative_scan_wl) ) ? 0 : ( (sum_db_stats_.false_positive_  + sum_db_stats_.true_negative_ == 0)? 1 : (sum_db_stats_.false_positive_ * 1.0 / (sum_db_stats_.false_positive_  + sum_db_stats_.true_negative_)));

    // sum_db_satats.abort_cnt_
    printf("Idx Hit Rate: %.2f\n", idx_hit_rate);
    // printf("Access Hit Rate: %.2f\n", hit_rate);
    printf("Tuple Hit Rate: %.2f\n", hit_rate);
    if (g_negative_point_wl || g_negative_scan_wl) {
      printf("False Positive Rate: %.2f (false positive: %d, true negative: %d)\n", false_positive_rate, sum_db_stats_.false_positive_, sum_db_stats_.true_negative_);
    }
    if (g_save_output) {
      g_out_str << "\"tuple_hit_rate_\": " << hit_rate << ",";
      g_out_str << "\"idx_hit_rate_\": " << idx_hit_rate << ",";
      g_out_str << "\"false_positive_rate_\": " << false_positive_rate << ",";
    }
  }
  std::vector<CommitStats> commit_stats_;
  std::vector<DBStats> db_stats_;
  CommitStats sum_commit_stats_;
  DBStats sum_db_stats_;
  std::mutex commit_latch_{};
  std::mutex db_latch_{};
};
} // namespace arboretum

#endif //README_MD_SRC_COMMON_STATS_H_
