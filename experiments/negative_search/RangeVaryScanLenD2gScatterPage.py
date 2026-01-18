from exp_helper import *


if __name__ == "__main__":
    # set configurations

    cmake_dir = "../../build/"
    os.chdir(cmake_dir)
    out_dir = "../output/negative_search/"
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    exp_name = "negativerange_page_varyscanlen_d2g_scatter"
    
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
        "g_negative_point_wl": False,
        "g_negative_scan_wl": True,
        "g_negative_search_op_enable": False,
        "g_enable_phantom_protection": False,
        "g_num_evict_thds": 1,
        "g_batch_eviction_period_ms": 0,
        "g_evict_threshold": 1,
        "g_negative_dataset_seed": 2522572442, 
        "g_record_size": 1024,
        "g_negative_search_op_type": "NOOP",
        "g_scan_length": 100,
        "g_idx_btree_split_ratio": 1.0,
        "g_scan_query_scatter": True,
    }

    ycsb_config = {
        "num_rows_": 1024000,
        "runtime_": 15,
        "warmup_time_": 0,
        "warmup_scan_max_iter_": 1000000,
        "domain_size_": 2048000,
        "dataload_domain_size_": 2048000, 
        # hint to make all query range queries
        "rw_txn_perc_": 0,
        "skew_query_shuffle_seed_": 3420089334,
        "num_req_per_query_": 1, 
    }
    executable = "ExpYCSB"

    # buf sz, read ratio, zipf theta
    exps = []
    for queue in [100]:
        # for buf_sz in [0.1, 0.3, 0.5, 0.7, 0.9]:
        for buf_sz in [0.3]:
            for read_perc in [1]:
                for zipf in [0.9]:
                    for record_sz in [1024]:
                        # for scan_len in [5, 10, 20, 50, 100]:
                        for scan_len in [4, 8, 16, 32, 64, 128]:
                            exps.append([buf_sz, read_perc, zipf, queue, record_sz, scan_len])
        break

    # set cosmos capacity at 4000
    for exp in exps:     
        ycsb_config["read_perc_"] = exp[1]
        ycsb_config["zipf_theta_"] = exp[2]
        db_config["g_commit_queue_limit"] = exp[3]
        db_config["g_record_size"] = exp[4]
        db_config["g_index_type"] = "BTREE"
        db_config["g_buf_type"] = "PGBUF"
        db_config["g_scan_length"] = exp[5]

        working_set = db_config["g_record_size"] * ycsb_config["num_rows_"]
        # working_set = db_config["g_record_size"] * ycsb_config["domain_size_"]
        db_config["g_total_buf_sz"] = round(working_set * exp[0])
        output_filename = out_dir + "{}.txt".format(exp_name) 
        print("Output Filename: " + output_filename, flush=True)
        db_config["g_out_fname"] = output_filename

        # mark each trial for better tracking
        trial_id = "azure16k-tuple-buf{}-zipf{}-negative{}-scanlen{}".format(exp[0], exp[2], 0, exp[5])
        config_file = set_config(config, db_config, ycsb_config,
                                 "configs/sample.cfg")
        try_exec(executable, config_file, trial_id)
        print(trial_id, flush=True)

