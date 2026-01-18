from exp_helper import *


if __name__ == "__main__":
    # set configurations

    cmake_dir = "../../build/"
    os.chdir(cmake_dir)
    out_dir = "../output/negative_search/"
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    exp_name = "negativerange_varydomain_halfbuffer_skew"
    
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
        "g_batch_eviction": True,
        "g_negative_scan_wl": True,
        "g_num_evict_thds": 0,
        "g_batch_eviction_period_ms": 0,
        "g_evict_threshold": 1,
        "g_negative_dataset_seed": 2522572442, 
        "g_record_size": 1024,
        "g_negative_search_op_type": "GAPPHANTOM",
        "g_scan_length": 20,
        "g_scan_query_scatter": True,
    }

    ycsb_config = {
        "num_rows_": 1024000,
        "runtime_": 15,
        "warmup_time_": 0,
        "warmup_scan_max_iter_": 1000000,
        #10g domain
        "domain_size_": 10240000, 
        "dataload_domain_size_": 10240000, 
        # hint to make all query range queries
        "rw_txn_perc_": 0,
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
        for negative_scan_opt in [0, 1, 2]:
            exps.append({"domain_size": exp_config["domain_size_"][idx], "dataload_domain_size": exp_config["dataload_domain_size_"][idx], "num_rows": exp_config["num_rows_"][idx], "negative_scan_opt": negative_scan_opt})
     # set cosmos capacity at 4000 

    for exp in exps:     
        if exp["negative_scan_opt"] == 0:
            db_config["g_negative_search_op_type"] = "GAPPHANTOM"
            db_config["g_index_type"] = "BTREE"
            db_config["g_buf_type"] = "OBJBUF"
        elif exp["negative_scan_opt"] == 1:
            db_config["g_negative_search_op_type"] = "GAPBIT"
            db_config["g_index_type"] = "BTREE"
            db_config["g_buf_type"] = "OBJBUF"
        elif exp["negative_scan_opt"] == 2:
            db_config["g_negative_search_op_type"] = "NOOP"
            db_config["g_index_type"] = "REMOTE"
            db_config["g_buf_type"] = "NOBUF"

        ycsb_config["domain_size_"] = exp["domain_size"]
        ycsb_config["dataload_domain_size_"] = exp["dataload_domain_size"]
        ycsb_config["num_rows_"] = exp["num_rows"]

        # working_set = db_config["g_record_size"] * ycsb_config["domain_size_"]
        working_set = db_config["g_record_size"] * ycsb_config["num_rows_"] * 0.5
        db_config["g_total_buf_sz"] = round(working_set)


        output_filename = out_dir + "{}.txt".format(exp_name) 
        print("Output Filename: " + output_filename, flush=True)
        db_config["g_out_fname"] = output_filename

        trial_id = "azure16k-tuple-zipf{}-domain{}g-negative{}".format(ycsb_config["zipf_theta_"], exp["domain_size"]/1024000, exp["negative_scan_opt"])
        db_config["g_index_type"] = "BTREE"
        db_config["g_buf_type"] = "OBJBUF"
        config_file = set_config(config, db_config, ycsb_config,
                                 "configs/sample.cfg")
        try_exec(executable, config_file, trial_id)
        print(trial_id, flush=True)
