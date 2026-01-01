from exp_helper import *


if __name__ == "__main__":
    # set configurations
    cmake_dir = "../cmake-build-debug-azure/"
    os.chdir(cmake_dir)
    out_dir = "../output/"
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    exp_name = "exp_mix_ro_10g_fanout250_analysis"
    config = {
        "config_name": exp_name,
    }
    db_config = {
        "g_out_fname": out_dir + "%s.txt" % exp_name,
        "g_save_output": "true",
        "g_idx_btree_fanout": 250,
        "g_num_worker_threads": 16,
        "g_enable_group_commit": True,
        "g_commit_queue_limit": 100,
        "g_commit_group_sz": 0,
        "g_commit_pool_sz": 1,
        "g_log_freq_us": 100,
        "g_remote_req_retries": 3,
        "g_num_restore_thds": 8,
        "g_early_lock_release": True,
        "g_enable_phantom_protection": True,
    }
    ycsb_config = {
        "num_rows_": 10240000,
        "runtime_": 60,
        "zipf_theta_": 0.877,
        "read_perc_": 1,
        "rw_txn_perc_": 0.05,
        "num_req_per_query_": 1,
    }
    working_set = 12000000000 # in bytes
    executable = "ExpYCSB"
    try_compile(executable)

    # buf sz, read ratio, zipf theta
    exps = []
    for buf_sz in [1.25, 0.75, 0.5, 0.25]:
        exps.append([buf_sz])

    for exp in exps:
        db_config["g_total_buf_sz"] = round(working_set * exp[0])
        db_config["g_num_worker_threads"] = 16

        trial_id = "azure16k-tuple-buf{}".format(exp[0])
        db_config["g_index_type"] = "BTREE"
        db_config["g_buf_type"] = "OBJBUF"
        for phantom_protect in [True]:
            db_config["g_enable_phantom_protection"] = phantom_protect
            config_file = set_config(config, db_config, ycsb_config,
                                     "configs/sample_scan.cfg")
            try_exec(executable, config_file, trial_id)
            print(trial_id, flush=True)

        trial_id = "azure16k-page-buf{}".format(exp[0])
        db_config["g_index_type"] = "BTREE"
        db_config["g_buf_type"] = "PGBUF"
        db_config["g_num_worker_threads"] = 16
        config_file = set_config(config, db_config, ycsb_config,
                                 "configs/sample_scan.cfg")
        try_exec(executable, config_file, trial_id)
        print(trial_id, flush=True)

    # no buffer case has no need to sweep buf size
    trial_id = "azure16k-no-buf"
    db_config["g_index_type"] = "REMOTE"
    db_config["g_buf_type"] = "NOBUF"
    db_config["g_num_worker_threads"] = 16
    config_file = set_config(config, db_config, ycsb_config,
                             "configs/sample_scan.cfg")
    try_exec(executable, config_file, trial_id)
    print(trial_id, flush=True)

