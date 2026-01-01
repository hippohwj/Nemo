#include "RedisClient.h"

#if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_REDIS
namespace arboretum {

RedisClient::RedisClient(std::string host, size_t port, std::string pwd) {
  LOG_INFO("Connecting to redis server at %s:%zu", host.c_str(), port);
  client_ = NEW(cpp_redis::client);
  client_->connect(host, port,
                      [](const std::string &host,
                         std::size_t port,
                         cpp_redis::connect_state status) {
                        if (status == cpp_redis::connect_state::dropped) {
                          LOG_DEBUG("Disconnect from redis server at %s:%zu", host.c_str(), port);
                        }
                      });
  client_->auth(pwd, [](cpp_redis::reply &response) {
    LOG_INFO("Redis auth response: %s", response.as_string().c_str());
  });
  LOG_INFO("Successfully connected to redis server at %s:%zu", host, port);
}

RC RedisClient::StoreSync(uint64_t granule_id, const std::string &key, char *value, size_t sz) {
  std::string s(value, sz);
  client_->hset("G-" + std::to_string(granule_id), key, s);
  client_->sync_commit();
  return RC::OK;
}

void RedisClient::BatchStoreSync(uint64_t granule_id, std::multimap<std::string, std::string> map) {
  for (auto & it : map) {
    client_->hset("G-" + std::to_string(granule_id), it.first, it.second);
  }
  client_->sync_commit();
}

void RedisClient::LoadSync(OID granule_id, const std::string & key,
                           std::string &data) {
  auto future_reply = client_->hget("G-" + std::to_string(granule_id), key);
  client_->sync_commit();
  auto reply = future_reply.get();
  if (reply.is_error())
    LOG_ERROR("Redis receives invalid reply: %s", reply.error().c_str());
  if (reply.is_null()) {
    LOG_ERROR("Redis receives null reply for key %s.", key.c_str());
  }
  data = reply.as_string();
}
void RedisClient::StoreAndLoadSync(uint64_t granule_id,
                                   const std::string &key,
                                   char *value,
                                   size_t sz,
                                   const std::string &load_key,
                                   std::string &load_data) {
  auto script = R"(
      redis.call('hset', KEYS[1], KEYS[2], ARGV[1])
      return redis.call('hget', KEYS[3], KEYS[4]);
  )";
  std::string s(value, sz);
  // TODO: currently assume two keys are from same granule
  std::vector<std::string> keys = {"G-" + std::to_string(granule_id), key,
                         "G-" + std::to_string(granule_id), load_key};
  std::vector<std::string> args = {s};
  cpp_redis::reply rp;
  client_->eval(script, keys, args, [&rp](cpp_redis::reply &reply) {
    rp = reply;
  });
  client_->sync_commit();
  if (!rp.is_string()) {
    LOG_ERROR("key %s not found in redis", load_key.c_str());
  }
  load_data = rp.as_string();
}

int64_t RedisClient::CheckSize(uint64_t granule_id) {
  auto future_reply = client_->hlen("G-" + std::to_string(granule_id));
  client_->sync_commit();
  auto reply = future_reply.get();
  return reply.as_integer();
}

void RedisClient::LogSync(std::string &stream, OID txn_id,
                          std::multimap<std::string, std::string> &log) {
  client_->xadd(stream, std::to_string(txn_id), log);
  client_->sync_commit();
}

void RedisClient::BatchLogSync(std::vector<std::string> &streams,
                               std::vector<std::string> &keys,
                               std::vector<std::multimap<std::string,
                               std::string>> &args) {
  std::ostringstream os;
  for(int i = 0; i < args.size(); i++) {
    client_->xadd(streams[i], keys[i], args[i]);
  }
  client_->sync_commit();
}

}

#endif
