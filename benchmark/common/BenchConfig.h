#ifndef ARBORETUM_BENCHMARK_CONFIGURATION_H_
#define ARBORETUM_BENCHMARK_CONFIGURATION_H_

#include "common/Common.h"

namespace arboretum {

#define DECL_BENCH_CONFIG(tpe, var, val) tpe var{val};
#define BENCH_CONFIG(x) x(uint64_t, num_workers_, 1)
#define PRINT_BENCH_CONFIG(tpe, name, val) { \
std::cout << #name << ": " << (name) << std::endl; \
if (g_save_output) g_out_str << "\"" << #name << "\"" << ": " << (name) << ", "; }

using boost::property_tree::ptree;

class BenchConfig {
 public:
  BenchConfig() = default;;
  BENCH_CONFIG(DECL_BENCH_CONFIG)
  ptree config_;
};

} // arboretum

#endif //ARBORETUM_BENCHMARK_CONFIGURATION_H_
