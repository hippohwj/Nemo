#include "TiKVClient.h"

#if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_TIKV
namespace arboretum {

RC TiKVClient::StoreSync(OID granule_id, const std::string &key, const std::string &data) {
  Backoffer bo(10);
  auto storage_key = CreateStorageKey(granule_id, key);
  int num_retries = 3;
  while (num_retries > 0) {
    auto loc = cluster_->region_cache->locateKey(bo, storage_key);
    // LOG_DEBUG("locate region %s for key %s", loc.region.toString().c_str(), storage_key.c_str());
    try {
      auto region_client = RegionClient(cluster_.get(), loc.region);
      auto put_req = std::make_shared<::kvrpcpb::RawPutRequest>();
      put_req->set_key(storage_key);
      put_req->set_value(data);
      auto region_resp = region_client.sendReqToRegion(bo, put_req).get();
      return region_resp->has_region_error() ? ERROR : OK;
    } catch (const pingcap::Exception &e) {
      num_retries--;
      if (num_retries != 0) {
        LOG_DEBUG("failed to send put request for key %s：%s retry;", storage_key.c_str(), e.message().c_str());
      } else {
        LOG_ERROR("failed to send TiKV put request for key %s: %s", storage_key.c_str(), e.message().c_str());
      }
    }
  }
}

RC TiKVClient::BatchStoreSync(OID granule_id, std::multimap<std::string, std::string> map) {
  Backoffer bo(100);
  // split the map into multiple regions and call separately.
  int num_retries = 3;
  auto status = OK;
  while (num_retries > 0) {
    for (auto &item: map) {
      StoreSync(granule_id, item.first, item.second);
    }
  }
  return status;
}

void TiKVClient::LoadSync(OID granule_id, const std::string &key, std::string &data) {
  Backoffer bo(10);
  auto loc = cluster_->region_cache->locateKey(bo, key);
  auto region_client = RegionClient(cluster_.get(), loc.region);
  auto get_req = std::make_shared<::kvrpcpb::RawGetRequest>();
  get_req->set_key(CreateStorageKey(granule_id, key));
  try {
    auto region_resp = region_client.sendReqToRegion(bo, get_req).get();
    data = region_resp->value();
  } catch (const pingcap::Exception & e) {
    LOG_ERROR("failed to send TiKV put request: %s", e.message().c_str());
  }
}

}

#endif