#ifndef ARBORETUM_SRC_REMOTE_ILOGSTORE_H_
#define ARBORETUM_SRC_REMOTE_ILOGSTORE_H_

#include "common/Common.h"
#include "RedisClient.h"
#include "AzureTableClient.h"

namespace arboretum {

class ILogStore {
 public:
  ILogStore();
  // worker thread
  uint64_t AppendToLogBuffer(OID txn_id, std::string &log);
  bool WaitForFlush(uint64_t lsn);
  // log thread
  void Flush(std::unique_lock<std::mutex> & lk);
  static void PeriodicLog(ILogStore * log_store);
  void StartLogThd() {
    flush_thd_ = new std::thread(ILogStore::PeriodicLog, this);
  }

  std::vector<OID> txns_;
  std::vector<std::string> data_;

  uint64_t log_lsn_{0};
  std::mutex buffer_latch_;
  std::condition_variable buffer_cv_;
  std::atomic<size_t> cnt_{0};

  std::atomic<uint64_t> flush_lsn_{0};
  std::mutex flush_latch_;
  std::condition_variable flush_cv_;
  std::string storage_id_;
  std::atomic<uint64_t> flush_ts_{0};

  std::thread *flush_thd_;

 private:
#if REMOTE_STORAGE_TYPE != REMOTE_STORAGE_AZURE_TABLE
  RedisClient * client_;
  std::vector<std::string> streams_;
  std::vector<std::string> keys_;
  std::vector<std::multimap<std::string, std::string>> args_;
#else
  AzureTableClient * client_;
  std::multimap<std::string, std::string> logs_;
#endif
};

}

#endif //ARBORETUM_SRC_REMOTE_ILOGSTORE_H_
