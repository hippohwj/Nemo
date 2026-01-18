from exp_helper import *


if __name__ == "__main__":
    # set configurations
    # cmake_dir = "../cmake-build-debug-azure/"
    cmake_dir = "../../build/"
    os.chdir(cmake_dir)
    out_dir = "../output/negative_search/"
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    exp_name = "negativepoint_varybuf_d2g_uniform"
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
        "g_negative_point_wl": True,
        "g_negative_search_op_enable": True,
        "g_num_evict_thds": 1,
        "g_batch_eviction_period_ms": 0,
        "g_evict_threshold": 1,
        "g_negative_dataset_seed": 2522572442, 
        "g_record_size": 1008
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

    # buf sz, read ratio, zipf theta
    exps = []
    for queue in [100]:
        # for buf_sz in [0.01, 0.1, 0.25, 0.5, 0.75, 1]:
        for buf_sz in [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7]:
            for read_perc in [1]:
                for zipf in [0]:
                    for record_sz in [1024]:
                        for negative_optimization in [1, 0]:
                            exps.append([buf_sz, read_perc, zipf, queue, record_sz, negative_optimization])
        break
     # set cosmos capacity at 4000 

    for exp in exps:     
        ycsb_config["read_perc_"] = exp[1]
        ycsb_config["zipf_theta_"] = exp[2]
        db_config["g_commit_queue_limit"] = exp[3]
        db_config["g_record_size"] = exp[4]
        if exp[5] == 0:
            db_config["g_negative_search_op_enable"] = False
        else:
            db_config["g_negative_search_op_enable"] = True
        # working_set = db_config["g_record_size"] * ycsb_config["num_rows_"]
        working_set = db_config["g_record_size"] * ycsb_config["domain_size_"]
        db_config["g_total_buf_sz"] = round(working_set * exp[0])
        output_filename = out_dir + "{}.txt".format(exp_name) 
        print("Output Filename: " + output_filename, flush=True)
        db_config["g_out_fname"] = output_filename

        trial_id = "azure16k-tuple-zipf{}-buf{}-negative{}".format(exp[2], exp[0], exp[5])
        db_config["g_index_type"] = "BTREE"
        db_config["g_buf_type"] = "OBJBUF"
        config_file = set_config(config, db_config, ycsb_config,
                                 "configs/sample.cfg")
        try_exec(executable, config_file, trial_id)
        print(trial_id, flush=True)

