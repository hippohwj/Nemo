from exp_helper import *


if __name__ == "__main__":
    # set configurations
    # cmake_dir = "../cmake-build-debug-azure/"
    cmake_dir = "../build/"
    os.chdir(cmake_dir)
    out_dir = "../output/"
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    exp_name = "exp_partial_negative_range_100m_varybuf_debug"
    config = {
        "config_name": exp_name,
    }
    db_config = {
        "g_out_fname": out_dir + "%s.txt" % exp_name,
        "g_save_output": "true",
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
        # "g_zipf_random_hotspots": True,
        "g_batch_eviction": True,
        "g_negative_scan_wl": True,
        "g_num_evict_thds": 0,
        "g_batch_eviction_period_ms": 0,
        "g_evict_threshold": 1,
        "g_negative_dataset_seed": 3643679940, 
        "g_record_size": 1008,
        "g_negative_search_op_type": "GAPPHANTOM",
        "g_scan_length": 2,
    }

    ycsb_config = {
        "num_rows_": 102400,
        "runtime_": 30,
        "warmup_time_": 5,
        "domain_size_": 204800, 
        # "domain_size_": 409600, 
        "dataload_domain_size_": 204800, 
        # hint to make all query range queries
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
        # for buf_sz in [1.25, 1, 0.5, 0.25]:
        # for buf_sz in [1]:
        # for buf_sz in [0.01, 0.1, 0.25, 0.5, 0.75, 1]:
        for buf_sz in [0.01]:
            # for read_perc in [1, 0.9, 0.5]:
            # for read_perc in [1]:
            for read_perc in [1]:
                for zipf in [0.9]:
                # for zipf in [0.99, 0.8, 0.6, 0.4, 0.2, 0]:
                    for record_sz in [1024]:
                        # for negative_scan_opt in [0, 1, 2]:
                        for negative_scan_opt in [0, 1]:
                            exps.append([buf_sz, read_perc, zipf, queue, record_sz, negative_scan_opt])
        break
# set cosmos capacity at 4000 
# set cosmos capacity at 20000 when read_prec = 0.5 and buf_sz = 0.1 for page baseline
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

        # working_set = db_config["g_record_size"] * ycsb_config["num_rows_"]
        working_set = db_config["g_record_size"] * ycsb_config["domain_size_"]
        db_config["g_total_buf_sz"] = round(working_set * exp[0])
        output_filename = out_dir + "{}-recordsz{}-read{}-negative{}.txt".format(exp_name, exp[4], exp[1], exp[5]) 
        print("Output Filename: " + output_filename, flush=True)
        db_config["g_out_fname"] = output_filename


        trial_id = "azure16k-tuple-buf{}-read{}-zipf{}-recordsz{}-negative{}".format(exp[0], exp[1], exp[2], exp[4], exp[5])
        config_file = set_config(config, db_config, ycsb_config,
                                 "configs/sample.cfg")
        try_exec(executable, config_file, trial_id)
        print(trial_id, flush=True)

        # trial_id = "azure16k-page-buf{}-read{}-zipf{}".format(exp[0], exp[1], exp[2])
        # db_config["g_index_type"] = "BTREE"
        # db_config["g_buf_type"] = "PGBUF"
        # if exp[0] == 1 and exp[1] == 0.5 and exp[2] == 0.9:
        #     db_config["g_num_worker_threads"] = 8
        # else:
        #     db_config["g_num_worker_threads"] = 16
        # config_file = set_config(config, db_config, ycsb_config,
        #                          "configs/sample.cfg")
        # # if exp[0] == 0.1 and exp[1] == 0.5 and exp[2] == 0.9:
        # #     print("skip")
        # # else: 
        # #     try_exec(executable, config_file, trial_id)
        # try_exec(executable, config_file, trial_id)
        # print(trial_id, flush=True)

