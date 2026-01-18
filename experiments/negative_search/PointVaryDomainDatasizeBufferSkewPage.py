from pandas.core.frame import dataclasses_to_dicts
from exp_helper import *


if __name__ == "__main__":
    # set configurations
    # cmake_dir = "../cmake-build-debug-azure/"
    cmake_dir = "../../build/"
    os.chdir(cmake_dir)
    out_dir = "../output/negative_search/"
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    exp_name = "negativepoint_page_varydomain_datasizebuffer_skew"
    config = {
        "config_name": exp_name,
    }
    db_config = {
        "g_out_fname": out_dir + "%s.txt" % exp_name,
        "g_save_output": "true",
        "g_retain_idx_page": True,
        "g_idx_btree_fanout": 250,
        "g_num_worker_threads": 32,
        "g_enable_group_commit": True,
        "g_commit_queue_limit": 100,
        "g_commit_group_sz": 0,
        "g_commit_pool_sz": 1,
        "g_log_freq_us": 100,
        "g_remote_req_retries": 2000,
        "g_num_restore_thds": 8,
        "g_early_lock_release": True,
        "g_zipf_random_hotspots": False,
        "g_batch_eviction": False,
        "g_eviction_simple_clock": False,
        "g_negative_point_wl": True,
        "g_negative_search_op_enable": False,
        "g_num_evict_thds": 1,
        "g_batch_eviction_period_ms": 0,
        "g_evict_threshold": 1,
        "g_negative_dataset_seed": 2522572442, 
         "g_idx_btree_split_ratio": 1.0,
        "g_record_size": 1024
    }

    ycsb_config = {
        "num_rows_": 1024000,
        "runtime_": 15,
        "warmup_time_": 0,
        # 2g domain
        "domain_size_": 2048000,
        "dataload_domain_size_": 2048000, 
        "num_req_per_query_": 1, 
    }

    executable = "ExpYCSB"

    exp_config = {
        "idx_": [0, 1, 2, 3, 4],
        "domain_size_": [1024000, 1024000*2, 1024000*5, 1024000*10, 1024000*20],
        "dataload_domain_size_": [1024000, 1024000*2, 1024000*5, 1024000*10, 1024000*20],
        "num_rows_": [1024000, 1024000, 1024000, 1024000, 1024000],
    }


    # buf sz, read ratio, zipf theta
    exps = []
    ycsb_config["read_perc_"] = 1
    ycsb_config["zipf_theta_"] = 0.9

    for idx in exp_config["idx_"]:
        buf_sz = exp_config["domain_size_"][idx]
        domain_size = exp_config["domain_size_"][idx]
        dataload_domain_size = exp_config["dataload_domain_size_"][idx]
        num_rows = exp_config["num_rows_"][idx]
        # for negative_optimization in [1, 0]:
        exps.append({"domain_size": exp_config["domain_size_"][idx], "dataload_domain_size": exp_config["dataload_domain_size_"][idx], "num_rows": exp_config["num_rows_"][idx]})

    for exp in exps:     
        ycsb_config["domain_size_"] = exp["domain_size"]
        ycsb_config["dataload_domain_size_"] = exp["dataload_domain_size"]
        ycsb_config["num_rows_"] = exp["num_rows"]

        # working_set = db_config["g_record_size"] * ycsb_config["domain_size_"]
        working_set = db_config["g_record_size"] * ycsb_config["num_rows_"]
        db_config["g_total_buf_sz"] = round(working_set)


        output_filename = out_dir + "{}.txt".format(exp_name) 
        print("Output Filename: " + output_filename, flush=True)
        db_config["g_out_fname"] = output_filename

        trial_id = "azure16k-tuple-zipf{}-domain{}g-negative{}".format(ycsb_config["zipf_theta_"], exp["domain_size"]/1024000, exp["negative_optimization"])
        db_config["g_index_type"] = "BTREE"
        db_config["g_buf_type"] = "PGBUF"
        config_file = set_config(config, db_config, ycsb_config,
                                 "configs/sample.cfg")
        try_exec(executable, config_file, trial_id)
        print(trial_id, flush=True)

