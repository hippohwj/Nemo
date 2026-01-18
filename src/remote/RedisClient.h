#ifndef ARBORETUM_SRC_REMOTE_REDISDATASTORE_H_
#define ARBORETUM_SRC_REMOTE_REDISDATASTORE_H_

#include "Common.h"

#if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_REDIS
#include <cpp_redis/cpp_redis>

namespace arboretum {

class RedisClient {
 public:
  RedisClient(std::string host, size_t port, std::string pwd);

  // data storage
  RC StoreSync(uint64_t granule_id, const std::string &key, char *value, size_t sz);
  void BatchStoreSync(uint64_t i, std::multimap<std::string, std::string> map);
  void LoadSync(OID granule_id, const std::string & key, std::string &basic_string);
  void StoreAndLoadSync(uint64_t granule_id, const std::string &key, char *value, size_t sz,
                        const std::string &load_key, std::string &load_data);
  int64_t CheckSize(uint64_t granule_id);
  void LogSync(std::string &stream, OID txn_id, std::multimap<std::string, std::string> &log);
  void BatchLogSync(std::vector<std::string> &streams,
                    std::vector<std::string> &keys,
                    std::vector<std::multimap<std::string, std::string>> &args);

 private:
  cpp_redis::client *client_;
};
}

#endif

#endif //ARBORETUM_SRC_REMOTE_REDISDATASTORE_H_
