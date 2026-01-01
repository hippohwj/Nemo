//
// Created by Zhihan Guo on 5/10/23.
//
#include "gtest/gtest.h"
#include "common/Common.h"
#include "remote/AzureTableClient.h"

using namespace arboretum;

#define GET_AZURE_LATENCY {                                                    \
  char data[payload];                                                          \
  auto key = std::to_string(payload);                                          \
  starttime = GetSystemClock();                                                \
  for (int i = 0; i < trials; i++) {                                           \
    client->StoreSync(tbl_name, "0", key, data, payload);                      \
  }                                                                            \
  auto latency = (GetSystemClock() - starttime) / (trials * 1.0) / 1000.0;     \
  g_out_file << "write," << payload << "," << latency << ",us" << std::endl;   \
  std::cout << "write," << payload << "," << latency << ",us" << std::endl;    \
  std::string response;                                                        \
  starttime = GetSystemClock();                                                \
  for (int i = 0; i < trials; i++) {                                           \
    client->LoadSync(tbl_name, "0", key, response);                            \
  }                                                                            \
  latency = (GetSystemClock() - starttime) / (trials * 1.0) / 1000.0;          \
  g_out_file << "read," << payload << "," << latency << ",us" << std::endl;    \
  std::cout << "read," << payload << "," << latency << ",us" << std::endl;     \
}

TEST(AzureTableTest, PayloadTest) {
  g_remote_req_retries = 0;
  std::ifstream in(azure_config_fname);
  std::string azure_conn_str;
  azure_conn_str.resize(500);
  getline(in, azure_conn_str);
  auto client = new AzureTableClient(azure_conn_str);
  auto tbl_name = "test";
  client->CreateTable(tbl_name);
  // 1, 10, 100, 1000, 10000, 1000
  int trials = 3;
  uint64_t starttime;
  g_out_file.open("../output/microbench_latency.txt", std::ios_base::app);
  uint64_t payload = 8192;
  GET_AZURE_LATENCY;
  for (payload = 1; payload <= 100 * 1000; payload *= 10) {
    GET_AZURE_LATENCY;
  }
}
TEST(AzureTableTest, DeleteTest) {
  std::ifstream in(azure_config_fname);
  std::string azure_conn_str;
  azure_conn_str.resize(500);
  getline(in, azure_conn_str);
  auto client = new AzureTableClient(azure_conn_str);
  auto tbl_name = "test";
  client->DeleteTable(tbl_name);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  ::testing::GTEST_FLAG(filter) = "AzureTableTest.PayloadTest";
  return RUN_ALL_TESTS();
}
