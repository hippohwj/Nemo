#ifndef ARBORETUM_BENCHMARK_WORKLOAD_H_
#define ARBORETUM_BENCHMARK_WORKLOAD_H_

#include "db/ARDB.h"
#include "local/ISchema.h"

namespace arboretum {

class Workload {
 public:
  explicit Workload(ARDB * db) : db_(db) {};
  void InitSchema(std::istream &in);

 protected:
  std::vector<ISchema *> schemas_{};
  std::unordered_map<std::string, OID> tables_{};
  std::vector<OID> indexes_{};
  ARDB * db_;
};

}

#endif //ARBORETUM_BENCHMARK_WORKLOAD_H_
