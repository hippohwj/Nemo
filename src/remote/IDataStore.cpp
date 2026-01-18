#include "IDataStore.h"
#include "common/Worker.h"
#include "common/Stats.h"

namespace arboretum {

IDataStore::IDataStore() {
#if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_REDIS
  std::ifstream in(redis_config_fname);
  std::string line;
  auto cnt = 0;
  std::vector<std::string> lines;
  while (getline(in, line)) {
    if (line[0] == '#') continue;
    cnt++;
    lines.push_back(line);
  }
  client_ = NEW(RedisClient)(lines[1], std::stol(lines[2]), lines[0]);
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_TIKV
  client_ = NEW(TiKVClient)();
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_AZURE_TABLE
  std::ifstream in(azure_config_fname);
  std::string azure_conn_str;
  azure_conn_str.resize(500);
  getline(in, azure_conn_str);
  client_ = NEW(AzureTableClient)(azure_conn_str);
#endif
}

int64_t IDataStore::CheckSize() {
#if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_REDIS
  return client_->CheckSize(g_granule_id);
#endif
  return -1;
}

RC IDataStore::Read(const std::string &tbl_name, SearchKey &key, std::string &data) {
  auto starttime = GetSystemClock();
#if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_REDIS
  client_->LoadSync(tbl_name, key.ToString(), data);
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_TIKV
  client_->LoadSync(tbl_name, key.ToString(), data);
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_AZURE_TABLE
  auto partition = std::to_string(key.ToUInt64() / g_partition_sz);
  // LOG_INFO("Read from Azure Table: tbl_name: %s, partition: %s, key: %s", tbl_name.c_str(), partition.c_str(), key.ToString().c_str());
  // std::cout.flush();
  auto rc = client_->LoadSync(tbl_name, partition, key.ToString(), data);
  // LOG_INFO("Read Finished from Azure Table: tbl_name: %s, partition: %s, key: %s, data: %s", tbl_name.c_str(), partition.c_str(), key.ToString().c_str(), data.c_str());
  // std::cout.flush();
#endif
  if (g_warmup_finished) {
    auto latency = GetSystemClock() - starttime;
    g_stats->db_stats_[Worker::GetThdId()].incr_remote_io_time_(latency);
    g_stats->db_stats_[Worker::GetThdId()].incr_remote_rd_time_(latency);
    g_stats->db_stats_[Worker::GetThdId()].incr_remote_rds_(1);
  }
  return rc;
}


// only used in page-based B+Tree for range experiment
RC IDataStore::ReadQuery(const std::string &tbl_name, SearchKey &key, std::string &data) {
  //TODO: change tbl id to str for all functions and change accordingly in redis & tikv.
  auto starttime = GetSystemClock();
#if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_REDIS
  client_->LoadSync(tbl_name, key.ToString(), data);
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_TIKV
  client_->LoadSync(tbl_name, key.ToString(), data);
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_AZURE_TABLE
  auto partition = std::to_string(key.ToUInt64() / g_partition_sz);
  // LOG_INFO("Read from Azure Table: tbl_name: %s, partition: %s, key: %s", tbl_name.c_str(), partition.c_str(), key.ToString().c_str());
  // std::cout.flush();
  auto rc = client_->LoadSyncQuery(tbl_name, partition,  key.ToString(), data);
  // LOG_INFO("Read Finished from Azure Table: tbl_name: %s, partition: %s, key: %s, data: %s", tbl_name.c_str(), partition.c_str(), key.ToString().c_str(), data.c_str());
  // std::cout.flush();
#endif
  if (g_warmup_finished) {
    auto latency = GetSystemClock() - starttime;
    g_stats->db_stats_[Worker::GetThdId()].incr_remote_io_time_(latency);
    g_stats->db_stats_[Worker::GetThdId()].incr_remote_rd_time_(latency);
    g_stats->db_stats_[Worker::GetThdId()].incr_remote_rds_(1);
  }
  return rc;
}

int64_t IDataStore::Read(const std::string &tbl_name, std::string part_key, std::string key) {
  return client_->LoadNumericSync(tbl_name, part_key, key);
}

// only used in page-based B+Tree
RC IDataStore::Read(const std::string &tbl_name, std::string part_key, std::string key, std::string &data) {
  return client_->LoadSync(tbl_name, part_key, key, data);
}


void IDataStore::Write(const std::string &tbl_name, std::string part_key, std::string key, int64_t num) {
  client_->StoreNumericSync(tbl_name, part_key, key, num);
}

void IDataStore::Write(const std::string &tbl_name, std::string part_key, std::string key, char *data, size_t sz) {
  client_->StoreSync(tbl_name, part_key, key, data, sz);
}

void IDataStore::Write(const std::string &tbl_name, SearchKey &key, char *data, size_t sz) {
  auto starttime = GetSystemClock();
#if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_REDIS
  client_->StoreSync(tbl_name, key.ToString(), data, sz);
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_TIKV
  std::string value(data, sz);
  client_->StoreSync(tbl_name, key.ToString(), value);
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_AZURE_TABLE
  auto partition = std::to_string(key.ToUInt64() / g_partition_sz);
  client_->StoreSync(tbl_name, partition, key.ToString(), data, sz);
#endif
  if (g_warmup_finished) {
    auto latency = GetSystemClock() - starttime;
    g_stats->db_stats_[Worker::GetThdId()].incr_remote_io_time_(latency);
    g_stats->db_stats_[Worker::GetThdId()].incr_remote_wr_time_(latency);
    g_stats->db_stats_[Worker::GetThdId()].incr_remote_wrs_(1);
  }
}

void IDataStore::WriteAndRead(const std::string &tbl_name, SearchKey &key, char *data, size_t sz,
                              SearchKey &load_key, std::string &load_data) {
  auto starttime = GetSystemClock();
#if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_REDIS
  client_->StoreAndLoadSync(tbl_id, key, data, sz, load_key, load_data);
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_TIKV
  std::string value(data, sz);
  client_->StoreSync(tbl_id, key, value);
  client_->LoadSync(load_tbl_id, load_key, load_data);
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_AZURE_TABLE
  auto store_part = std::to_string(key.ToUInt64() / g_partition_sz);
  auto load_part = std::to_string(load_key.ToUInt64() / g_partition_sz);
  client_->StoreAndLoadSync(tbl_name, store_part, key.ToString(), data, sz,
                            load_part, load_key.ToString(), load_data);
#endif
  if (g_warmup_finished) {
    auto latency = GetSystemClock() - starttime;
    g_stats->db_stats_[Worker::GetThdId()].incr_remote_io_time_(latency);
    g_stats->db_stats_[Worker::GetThdId()].incr_remote_rw_time_(latency);
    g_stats->db_stats_[Worker::GetThdId()].incr_remote_rws_(1);
  }
}

void IDataStore::WriteBatch(const std::string &tbl_name, std::string part,
                            std::multimap<std::string, std::string> &map) {
  // used only when data forcing is enabled
#if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_REDIS
  client_->BatchStoreSync(tbl_name, std::move(map));
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_TIKV
  client_->BatchStoreSync(tbl_name, std::move(map));
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_AZURE_TABLE
  client_->BatchStoreSync(tbl_name, part, map);
#endif
}

void IDataStore::WriteBatchAsyncCallback(const std::string &tbl_name, std::string partition, const std::multimap<std::string, std::string> &data, std::function<void()> callback) {
#if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_AZURE_TABLE
  client_->WriteBatchAsync(tbl_name, std::move(partition), data, callback);
#else
  // Implementations for other storage types can be added here.
  LOG_ERROR("WriteBatchAsync is not implemented for the current storage type.");
#endif
}

void IDataStore::WriteBatchAsync(const std::string &tbl_name, std::string part,
                            std::multimap<std::string, std::string> &map,
                            volatile bool& is_done) {
  // used only when data forcing is enabled
#if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_REDIS
  client_->BatchStoreASync(tbl_name, std::move(map));
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_TIKV
  client_->BatchStoreASync(tbl_name, std::move(map));
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_AZURE_TABLE
  client_->BatchStoreAsync(tbl_name, std::move(part), map, is_done);
#endif
}

void IDataStore::WriteAsync(const std::string &tbl_name, SearchKey &key,
                           char *data, size_t sz, volatile RC *rc) {
  auto partition = std::to_string(key.ToUInt64() / g_partition_sz);
  return client_->StoreAsync(tbl_name, partition, key.ToString(), data, sz, rc);
}

void IDataStore::ReadRange(std::string table_name, SearchKey low_key, SearchKey high_key,  char** tuples, size_t total_tuple_sz,
                           size_t& res_cnt) {
  auto starttime = GetSystemClock();
  auto low_part = std::to_string(low_key.ToUInt64() / g_partition_sz);
  auto high_part = std::to_string(high_key.ToUInt64() / g_partition_sz);
  if (low_part == high_part) {
    // ReadRangeHelper(cnt, table_name, low_part, low_key, high_key, data_ptrs);
    client_->LoadRangePython(table_name, low_part, low_key.ToString(), high_key.ToString(), tuples, total_tuple_sz, res_cnt);
  } else {
    M_ASSERT(false, "TODO: to support range scan across partitions");
    // split into two half (will not exceed two currently
    // as partition size is larger than max scan length)
    auto boundary_val = high_key.ToUInt64() / g_partition_sz * g_partition_sz;
    auto pos = boundary_val - low_key.ToUInt64();
    // TODO: make the two queries async
    //  [low_key, boundary - 1], [boundary, high_key]
    auto boundary = SearchKey(boundary_val - 1);
    size_t count1 = 0;
    client_->LoadRangePython(table_name, low_part, low_key.ToString(), boundary.ToString(), tuples, total_tuple_sz, count1);
    // ReadRangeHelper(pos, table_name, low_part, low_key, boundary, data_ptrs);
    boundary.SetValue(boundary_val, BIGINT);
    size_t count2 = 0;
    M_ASSERT(false, "TODO: to support the second half of the range scan , current tuples is only a placeholder")
    // client_->LoadRangePython(table_name, high_part, boundary.ToString(), high_key.ToString(), &data_ptrs[pos], total_tuple_sz, count2);
    client_->LoadRangePython(table_name, high_part, boundary.ToString(), high_key.ToString(), tuples, total_tuple_sz, count2);
    res_cnt = count1 + count2;
    // ReadRangeHelper(cnt - pos, table_name, high_part, boundary, high_key, &data_ptrs[pos]);
  }
  if (g_warmup_finished) {
    auto latency = GetSystemClock() - starttime;
    LOG_XXX("ReadRange from %s, low_key: %s, high_key: %s, scan_sz: %d, latency: %.2f us",
              table_name.c_str(), low_key.ToString().c_str(), high_key.ToString().c_str(), res_cnt,  (latency*1.0)/1000);
    g_stats->db_stats_[Worker::GetThdId()].incr_remote_io_time_(latency);
    g_stats->db_stats_[Worker::GetThdId()].incr_remote_scan_time_(latency);
    g_stats->db_stats_[Worker::GetThdId()].incr_remote_scans_(1);
    g_stats->db_stats_[Worker::GetThdId()].incr_remote_scan_sz_(res_cnt);
  }
}

} // arboretum