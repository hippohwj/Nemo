#include "ycsb/ycsb.h"
using namespace arboretum;

int main(int argc, char* argv[]) {
  // Note: must load db config first.
  ARDB::LoadConfig(argc, argv);
  auto bench_config = new (MemoryAllocator::Alloc(sizeof(YCSBConfig))) YCSBConfig(argc, argv);
  if (g_enable_partition_covering_lock) {
    // partition_covering_lock doesn't need next-key locking
    g_enable_phantom_protection = false;
  }

  std::string recordsz_prefix = (g_record_size == 1008) ? "" : "-r" + std::to_string(g_record_size);

  if (g_buf_type == PGBUF) {
    // std::string dataset_id = "page" + std::to_string(g_idx_btree_fanout) + "-" +
    //                          std::to_string(bench_config->num_rows_ / 1000000) + "g" + recordsz_prefix;
    std::string negative_prefix = (g_negative_point_wl || g_negative_scan_wl) ?  "-n" + std::to_string(bench_config->dataload_domain_size_ / 1000000) + "g": "";
    std::string dataset_id = "page" + std::to_string(g_idx_btree_fanout) + "-"  + std::to_string(bench_config->num_rows_ / 1000000) + "g" + recordsz_prefix + negative_prefix;
    std::strcpy(g_dataset_id, dataset_id.c_str());
    LOG_INFO("dataset id: %s", g_dataset_id);
  } else if (g_buf_type == OBJBUF || g_buf_type == NOBUF) {
    std::string negative_prefix = (g_negative_point_wl || g_negative_scan_wl) ?  "-n" + std::to_string(bench_config->dataload_domain_size_ / 1000000) + "g": "";
    // std::string negative_prefix = (g_negative_point_wl || g_negative_scan_wl) ?  "-n" + std::to_string(bench_config->num_rows_): "";
    // std::string negative_prefix = "";
    std::string dataset_id = "obj-" + std::to_string(bench_config->num_rows_ / 1000000) + "g" + recordsz_prefix + negative_prefix;
    std::strcpy(g_dataset_id, dataset_id.c_str());
    LOG_INFO("dataset id: %s", g_dataset_id);
  } else if (g_buf_type == HYBRBUF) {
    std::string top_dataset_id = "obj-" + std::to_string(bench_config->num_rows_ / 1000000) + "g" + recordsz_prefix;
    std::strcpy(g_dataset_id, top_dataset_id.c_str());
    std::strcpy(g_top_dataset_id, top_dataset_id.c_str());
    LOG_INFO("two-tree top dataset id: %s", g_top_dataset_id);
    std::string bottom_dataset_id = "page" + std::to_string(g_idx_btree_fanout) + "-" +
                                    std::to_string(bench_config->num_rows_ / 1000000) + "g" + recordsz_prefix;
    std::strcpy(g_bottom_dataset_id, bottom_dataset_id.c_str());
    LOG_INFO("two-tree bottom dataset id: %s", g_bottom_dataset_id);
  }
  auto db = new (MemoryAllocator::Alloc(sizeof(ARDB))) ARDB;
  ycsb::ycsb(db, bench_config);
  return 0;
}