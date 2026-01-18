#include "ILogStore.h"
#include "IDataStore.h"
#include "common/Worker.h"
#include "common/Stats.h"

namespace arboretum {

ILogStore::ILogStore() {
  storage_id_ = (g_index_type == TWOTREE)? g_bottom_dataset_id:g_dataset_id;
  storage_id_ += "-log";
  LOG_INFO("Log store name %s", storage_id_.c_str());
#if REMOTE_STORAGE_TYPE != REMOTE_STORAGE_AZURE_TABLE
  std::ifstream in(redis_config_fname);
  std::string line;
  auto cnt = 0;
  std::vector<std::string> lines;
  while (getline(in, line)) {
    if (line[0] == '#') continue;
    cnt++;
    lines.push_back(line);
  }
  client_ = NEW(RedisClient)(lines[3], std::stol(lines[4]), lines[0]);
#else
  std::ifstream in(azure_config_fname);
  std::string azure_conn_str;
  azure_conn_str.resize(500);
  getline(in, azure_conn_str);
  client_ = NEW(AzureTableClient)(azure_conn_str);
  client_->CreateTable(storage_id_);
#endif
}

void ILogStore::PeriodicLog(ILogStore * log_store) {
  while (!g_terminate_exec) {
    std::unique_lock<std::mutex> lk(log_store->buffer_latch_);
    // LOG_DEBUG("waiting for log buffer to be filled;");
    log_store->buffer_cv_.wait_for(lk, std::chrono::microseconds(g_log_freq_us),
                 [log_store] { return log_store->txns_.size() >= g_commit_group_sz; });
    if (g_terminate_exec)
      break;
    log_store->Flush(lk);
  }
  log_store->flush_cv_.notify_all();
}



void ILogStore::Flush(std::unique_lock<std::mutex> & lk) {
  if (txns_.size() == 0)
    return;
  auto sz = txns_.size();
  size_t log_sz = 0;
  // prepare log
  azure::storage::table_batch_operation batch_op;
  for (size_t i = 0; i < sz; i++) {
    azure::storage::table_entity tuple(U(std::to_string(flush_lsn_ % 1000)),
                                       U(std::to_string(cnt_)));
    auto& col = tuple.properties();
    col.reserve(1);
    col[U("Data")] = azure::storage::entity_property(
        std::vector<uint8_t>(data_[i].begin(), data_[i].end()));
    batch_op.insert_or_replace_entity(tuple);
    log_sz += data_[i].size();
  }
  // remove flushed items
  txns_.erase(txns_.begin(), txns_.begin() + sz);
  data_.erase(data_.begin(), data_.begin() + sz);
  lk.unlock();
  uint64_t starttime = GetSystemClock();
  uint64_t flush_interval = starttime - flush_ts_;
  flush_ts_ = starttime;
  // flush log
#if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_AZURE_TABLE
  client_->BatchStoreSync(storage_id_, batch_op);
  cnt_++;
#endif
  flush_lsn_ += sz;
  flush_cv_.notify_all();
  // collect stats
  if (g_warmup_finished) {
    auto latency = GetSystemClock() - starttime;
    g_stats->commit_stats_[0].incr_num_flushes_(1);
    g_stats->commit_stats_[0].incr_log_group_sz_(sz);
    g_stats->commit_stats_[0].incr_log_group_payload_sz_(log_sz);
    g_stats->commit_stats_[0].incr_log_flush_interval_(flush_interval);
    g_stats->commit_stats_[0].incr_log_flush_latency_(latency);
  }
}

uint64_t ILogStore::AppendToLogBuffer(OID txn_id, std::string &log) {
  // add log to log buffer
  std::unique_lock<std::mutex> lk(buffer_latch_);
  txns_.push_back(txn_id);
  data_.push_back(log);
  auto lsn = ++log_lsn_;
  buffer_cv_.notify_all();
  return lsn;
}

bool ILogStore::WaitForFlush(uint64_t lsn) {
  if (g_enable_log) {
      std::unique_lock<std::mutex> lk2(flush_latch_);
  // check every ms in case db is terminated
      while (!g_terminate_exec) {
        if (flush_cv_.wait_for(lk2, std::chrono::microseconds(1000),
                     [this, lsn]{ return this->flush_lsn_ >= lsn; })) {
          return true;
        }
      }
      return false;
  } else {
    // if logging is disabled, we can return immediately
    return true;
  }

}

}