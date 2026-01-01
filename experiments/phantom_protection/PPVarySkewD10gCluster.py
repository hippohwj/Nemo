from exp_helper import *


if __name__ == "__main__":
    # set configurations
    # cmake_dir = "../cmake-build-debug-azure/"
    cmake_dir = "../../build/"
    os.chdir(cmake_dir)
    out_dir = "../output/phantom_protection/"
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    exp_name = "pp_varyskew_d10g_cluster"
    config = {
        "config_name": exp_name,
    }
    db_config = {
        "g_out_fname": out_dir + "%s.txt" % exp_name,
        "g_save_output": "true",
        "g_idx_btree_fanout": 250,
        # "g_num_worker_threads": 32,
        "g_num_worker_threads": 32,
        "g_scan_workload_rw_thd_num": 10,
        "g_enable_group_commit": True,
        "g_commit_queue_limit": 100,
        "g_commit_group_sz": 0,
        "g_commit_pool_sz": 1,
        "g_log_freq_us": 100,
        "g_remote_req_retries": 2000,
        "g_num_restore_thds": 8,
        "g_early_lock_release": True,
        "g_zipf_random_hotspots": False,
        # "g_zipf_random_hotspots": True,
        "g_batch_eviction": True,
        "g_negative_scan_wl": True,
        "g_num_evict_thds": 0,
        "g_batch_eviction_period_ms": 0,
        "g_evict_threshold": 1,
        "g_negative_dataset_seed": 2522572442, 
        "g_record_size": 1024,
        "g_negative_search_op_type": "GAPPHANTOM",
        "g_phantom_protection_type": "PHANTOM_NEXT", 
        # "g_phantom_protection_type": "NEXT_KEY", 
        "g_scan_length": 20,
        "g_scan_query_scatter": False,
        "g_enable_log": False,
    }

    ycsb_config = {
        "num_rows_": 1024000,
        "runtime_": 5,
        "warmup_time_": 0,
        #10g domain
        "domain_size_": 10240000, 
        "dataload_domain_size_": 10240000, 
        # "dataload_domain_size_": 102400, 
        # hint to make all query range queries
        "read_perc_": 0,
        "insert_txn_perc_": 0.7,
        "rw_txn_perc_": 0,
        "num_req_per_query_": 1, 
    }
    # working_set = 12000000000 # in bytes
    # working_set = 120000000 # in bytes
    executable = "ExpYCSB"
    # try_compile(executable)

    # buf sz, read ratio, zipf theta
    exps = []
    for queue in [100]:
        for buf_sz in [0.5]:
            for read_perc in [0]:
                # for zipf in [0.9]:
                for zipf in [0.99, 0.8, 0.6, 0.4, 0.2, 0.0000001]:
                    for record_sz in [1024]:
                        # for negative_scan_opt in [0, 1, 2]:
                        for negative_scan_opt in [1]:
                            for phantom_type in [0, 1, 2]:
                                exps.append([buf_sz, read_perc, zipf, queue, record_sz, negative_scan_opt, phantom_type])
        break
# set cosmos capacity at 32000 
    for exp in exps:
        # output_filename = out_dir + "%s.txt" % exp_name 
     
        ycsb_config["read_perc_"] = exp[1]
        ycsb_config["zipf_theta_"] = exp[2]
        db_config["g_commit_queue_limit"] = exp[3]
        # db_config["g_num_worker_threads"] = 16
        db_config["g_record_size"] = exp[4]
        if exp[5] == 0:
            db_config["g_negative_search_op_type"] = "GAPPHANTOM"
            db_config["g_index_type"] = "BTREE"
            db_config["g_buf_type"] = "OBJBUF"
        elif exp[5] == 1:
            db_config["g_negative_search_op_type"] = "GAPBIT"
            db_config["g_index_type"] = "BTREE"
            db_config["g_buf_type"] = "OBJBUF"
        elif exp[5] == 2:
            db_config["g_negative_search_op_type"] = "NOOP"
            db_config["g_index_type"] = "REMOTE"
            db_config["g_buf_type"] = "NOBUF"

        if exp[6] == 0:
            db_config["g_phantom_protection_type"] = "PHANTOM_NEXT"
        elif exp[6] == 1:
            db_config["g_phantom_protection_type"] = "NEXT_KEY"
        elif exp[6] == 2:
            db_config["g_phantom_protection_type"] = "PCL"
            db_config["g_enable_partition_covering_lock"] = True
        working_set = db_config["g_record_size"] * ycsb_config["num_rows_"]
        # working_set = db_config["g_record_size"] * ycsb_config["domain_size_"]
        db_config["g_total_buf_sz"] = round(working_set * exp[0])
        output_filename = out_dir + "{}.txt".format(exp_name) 
        print("Output Filename: " + output_filename, flush=True)
        db_config["g_out_fname"] = output_filename

        trial_id = "azure16k-tuple-buf{}-zipf{}-phantom{}".format(exp[0], exp[2], exp[6])
        config_file = set_config(config, db_config, ycsb_config,
                                 "configs/sample.cfg")
        try_exec(executable, config_file, trial_id)
        print(trial_id, flush=True)

