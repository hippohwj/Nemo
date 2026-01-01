#include "ycsb/ycsb.h"

using namespace arboretum;

int main(int argc, char* argv[]) {
  // Note: must load db config first.
  ARDB::LoadConfig(argc, argv);
  auto bench_config = new(MemoryAllocator::Alloc(sizeof(YCSBConfig)))
      YCSBConfig(argc, argv);
  M_ASSERT(g_buf_type != HYBRBUF, "doesn't support data loading for hybrid buffer, using pgbuf or objbuf instead");
  std::string recordsz_prefix = (g_record_size == 1008) ? "" : "-r" + std::to_string(g_record_size);
  std::string negative_prefix = (g_negative_point_wl || g_negative_scan_wl) ?  "-n" + std::to_string(bench_config->domain_size_/1000000)+"g": "";
  std::string dataset_id = (g_buf_type == PGBUF ?
      "page"+ std::to_string(g_idx_btree_fanout) + "-": "obj-")  +
      std::to_string(bench_config->num_rows_ / 1000000) + "g" + recordsz_prefix + negative_prefix;
  std::strcpy(g_dataset_id, dataset_id.c_str());
  LOG_DEBUG("dataset id: %s", g_dataset_id);
  auto db = new (MemoryAllocator::Alloc(sizeof(ARDB))) ARDB;
  auto workload = YCSBWorkload(db, bench_config);
  return 0;
}

