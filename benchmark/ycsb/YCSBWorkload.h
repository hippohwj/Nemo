#ifndef ARBORETUM_BENCHMARK_YCSB_YCSBWORKLOAD_H_
#define ARBORETUM_BENCHMARK_YCSB_YCSBWORKLOAD_H_

#include "common/Workload.h"
#include "YCSBConfig.h"

namespace arboretum {

struct YCSBRequest {
  uint64_t key;
  uint32_t value;
  AccessType ac_type;
};

struct YCSBQuery {
  YCSBRequest * requests;
  size_t req_cnt;
  bool read_only;
};

class ARDB;
class BenchWorker;
class YCSBWorkload : public Workload {

 public:
  enum QueryType { YCSB_RW, YCSB_INSERT, YCSB_SCAN };
  explicit YCSBWorkload(ARDB *db, YCSBConfig *config);
  void WarmupCache();
  void LoadData();
  void BatchLoad();
  void CheckData();
  YCSBQuery * GenRWQuery();
  YCSBQuery * GenInsertQuery();
  YCSBQuery * GenScanQuery();
  void GenRequest(YCSBQuery * query, size_t &cnt);
  void GenScanRequest(YCSBQuery * query, size_t &cnt);
  static void Execute(YCSBWorkload * workload, BenchWorker * worker);


  static void ExecuteLoad(YCSBWorkload * wl, int thd_id, int num_thds, uint64_t row_cnt) {
    wl->ParallelLoad(thd_id, num_thds, row_cnt);
  };
  void ParallelLoad(int thd_id, int num_thds, uint64_t row_cnt);


  static void ExecuteLoadForNegativeDataset(YCSBWorkload * wl, vector<int>& sample, int thd_id, int num_thds, size_t start, size_t end) {
    wl->ParallelLoadForNegativeDataset(sample, thd_id, num_thds, start, end);
  };
  void ParallelLoadForNegativeDataset(vector<int>& sample, int thd_id, int num_thds, size_t start, size_t end);


  uint64_t GetZipfTopK(int k);

 private:
  RC RWTxn(YCSBQuery * query, ITxn *txn);
  RC InsertTxn(YCSBQuery * query, ITxn *txn);
  RC ScanTxn(YCSBQuery * query, ITxn *txn);

  // for generating queries
  void GenKey(uint64_t &row_id);

  // for fast cache warmup
  void WarmupNegativePoint();
  void WarmupNegativeScan();
  void WarmupNegativeScanGapbitOnly();
  void WarmupNegativeScanGapbitPhantom();
  void WarmupPageNegativePoint();
  void WarmupPageNegativeScan();
  void DataGenNemoBUF();
  
  void DataGenPGBUF();
  bool IsUniformDist(double theta);



  struct YCSBtuple {
    int64_t pkey;
    char data1[100];
  };
  YCSBConfig *config_;

  // query generation and key distribution
  uint64_t zipf(uint64_t n, double theta);
  uint64_t the_n{0};
  double denom{0};
  double zeta_2_theta{0};
  std::vector<uint64_t> * shuffle_idx;
  uint64_t hotspot_start{0};



  // schema
  std::string YCSB_schema_string =
      "//size, type, name, pkey\n"
      "TABLE=MAIN_TABLE\n"
      "    8,int64_t,KEY,1\n"
      "    100,string,F0,0\n"
      "    100,string,F1,0\n"
      "    100,string,F2,0\n"
      "    100,string,F3,0\n"
      "    100,string,F4,0\n"
      "    100,string,F5,0\n"
      "    100,string,F6,0\n"
      "    100,string,F7,0\n"
      "    100,string,F8,0\n"
      "    100,string,F9,0\n"
      "\n"
      "INDEX=MAIN_INDEX\n"
      "MAIN_TABLE,0";
  void CalculateDenom();
  static double zeta(uint64_t n, double theta);
};

}
#endif //ARBORETUM_BENCHMARK_YCSB_YCSBWORKLOAD_H_
