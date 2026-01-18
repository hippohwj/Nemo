//
// check exception code at:
// https://github.com/tikv/client-c/blob/93ee25af7ba5000625948de92c9de402f0522c05/include/pingcap/Exception.h
//

#ifndef ARBORETUM_SRC_REMOTE_TIKVCLIENT_H_
#define ARBORETUM_SRC_REMOTE_TIKVCLIENT_H_

#include "common/Common.h"

#if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_TIKV
#include <pingcap/kv/Cluster.h>
#include <pingcap/kv/RegionClient.h>
#include <pingcap/Exception.h>
#include <kvproto/tikvpb.grpc.pb.h>
using namespace pingcap::kv;

namespace arboretum {
class TiKVClient {
 public:
  TiKVClient() {
    std::vector<std::string> pd_addrs;
    std::string line;
    std::ifstream in(tikv_config_fname);
    size_t cnt = 0;
    while (getline(in, line)) {
      if (line[0] == '#') {
        continue;
      }
      cnt++;
      if (cnt == 1) {
        // key path
        config_.key_path = line;
      } else {
        pd_addrs.push_back(line);
      }
    }
    config_.api_version = ::kvrpcpb::APIVersion::V1;
    cluster_ = std::make_shared<Cluster>(pd_addrs, config_);
  }

  RC StoreSync(OID granule_id, const std::string &key, const std::string &data);
  RC BatchStoreSync(OID granule_id, std::multimap<std::string, std::string> map);
  void LoadSync(OID granule_id, const std::string & key, std::string &basic_string);

 private:
  inline static std::string CreateStorageKey(OID granule_id, const std::string& key) {
    return "G-" + std::to_string(granule_id) + "|" + key;
  }

  pingcap::ClusterConfig config_;
  std::shared_ptr<Cluster> cluster_;

};

}

#endif
#endif //ARBORETUM_SRC_REMOTE_TIKVCLIENT_H_
