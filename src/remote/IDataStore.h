#ifndef ARBORETUM_DISTRIBUTED_SRC_TRANSPORT_DATASTORE_H_
#define ARBORETUM_DISTRIBUTED_SRC_TRANSPORT_DATASTORE_H_

#include "common/Common.h"
#if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_REDIS
#include "RedisClient.h"
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_TIKV
#include "TiKVClient.h"
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_AZURE_TABLE
#include "AzureTableClient.h"
#endif

namespace arboretum {

class IDataStore {
 public:
  IDataStore();
  RC Read(const std::string &tbl_name, SearchKey &key, std::string &data);
  RC ReadQuery(const std::string &tbl_name, SearchKey &key, std::string &data);
  void ReadRange(std::string table_name, SearchKey low_key, SearchKey high_key,  char** tuples, size_t total_tuple_sz,
    size_t& res_cnt);
  // void ReadRange(std::string table_name, SearchKey low_key, SearchKey high_key, char **data_ptrs, size_t &res_cnt);
  void Write(const std::string &tbl_name, SearchKey &key, char * data, size_t sz);
  void WriteAsync(const std::string &tbl_name, SearchKey &key, char * data, size_t sz, volatile RC *rc);
  void WriteAndRead(const std::string &tbl_name, SearchKey &key, char *data, size_t sz,
                    SearchKey &load_key, std::string &load_data);
  void WriteBatch(const std::string &tbl_name, std::string part, std::multimap<std::string, std::string> &map);
  void WriteBatchAsync(const std::string &tbl_name, std::string part, std::multimap<std::string, std::string> &map,
                       volatile bool& is_done);
  void WriteBatchAsyncCallback(const std::string &tbl_name, std::string partition, const std::multimap<std::string, std::string> &data, std::function<void()> callback);
  void CreateTable(const std::string &tbl_name) {
#if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_AZURE_TABLE
    client_->CreateTable(tbl_name);
#endif
  };
  // for populating data in remote
  void WaitForAsyncBatchLoading(int cnt) {
#if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_AZURE_TABLE
    client_->WaitForAsync(cnt);
#endif
  }
  // for metadata
  int64_t Read(const std::string &tbl_name, std::string part_key, std::string key);
  RC Read(const std::string &tbl_name, std::string part_key, std::string key, std::string &data);
  void Write(const std::string &tbl_name, std::string part_key, std::string key, int64_t num);
  void Write(const std::string &tbl_name, std::string part_key, std::string key, char * data, size_t sz);
  int64_t CheckSize();

 private:
#if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_REDIS
  RedisClient * client_;
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_TIKV
  TiKVClient * client_;
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_AZURE_TABLE
  AzureTableClient * client_;
#endif
};

} // arboretum

#endif //ARBORETUM_DISTRIBUTED_SRC_TRANSPORT_DATASTORE_H_
