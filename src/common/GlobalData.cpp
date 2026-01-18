#include "GlobalData.h"

namespace arboretum {

char g_config_fname[100] = "configs/sample.cfg";
char tikv_config_fname[100] = "configs/ifconfig_tikv.txt";
char redis_config_fname[100] = "configs/ifconfig_redis.txt";
char azure_config_fname[100] = "configs/ifconfig_azure.txt";

char g_dataset_id[16] = "default";
char g_top_dataset_id[16] = "default";
char g_bottom_dataset_id[16] = "default";
std::stringstream g_out_str;
std::ofstream g_out_file;
char g_out_fname[100] = "sample.out";
volatile bool g_terminate_exec = false;
volatile bool g_terminate_evict = false;

DB_CONFIGS(DEFN_GLOBAL_CONFIG2, DEFN_GLOBAL_CONFIG3,
           DEFN_GLOBAL_CONFIG4, DEFN_GLOBAL_CONFIG5)


} // arboretum