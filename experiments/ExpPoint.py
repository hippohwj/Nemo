from exp_helper import *


if __name__ == "__main__":
    # set configurations
    cmake_dir = "../cmake-build-debug-azure/"
    os.chdir(cmake_dir)
    out_dir = "../output/"
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    exp_name = "microbench_zipf_10g_fanout250"
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
    }
    ycsb_config = {
        "num_rows_": 10240000,
        "runtime_": 60,
    }
    working_set = 12000000000 # in bytes
    executable = "ExpYCSB"
    try_compile(executable)

    # buf sz, read ratio, zipf theta
    exps = []
    for queue in [100]:
        for buf_sz in [1.25, 1, 0.5, 0.25]:
            for read_perc in [1, 0.9, 0.5]:
                for zipf in [0.3, 0.5, 0.7, 0.9]:
                    exps.append([buf_sz, read_perc, zipf, queue])
        break

    for exp in exps:
        db_config["g_total_buf_sz"] = round(working_set * exp[0])
        ycsb_config["read_perc_"] = exp[1]
        ycsb_config["zipf_theta_"] = exp[2]
        db_config["g_commit_queue_limit"] = exp[3]
        db_config["g_num_worker_threads"] = 16

        trial_id = "azure16k-tuple-buf{}-read{}-zipf{}".format(exp[0], exp[1], exp[2])
        db_config["g_index_type"] = "BTREE"
        db_config["g_buf_type"] = "OBJBUF"
        config_file = set_config(config, db_config, ycsb_config,
                                 "configs/sample.cfg")
        try_exec(executable, config_file, trial_id)
        print(trial_id, flush=True)

        trial_id = "azure16k-page-buf{}-read{}-zipf{}".format(exp[0], exp[1], exp[2])
        db_config["g_index_type"] = "BTREE"
        db_config["g_buf_type"] = "PGBUF"
        if exp[0] == 1 and exp[1] == 0.5 and exp[2] == 0.9:
            db_config["g_num_worker_threads"] = 8
        else:
            db_config["g_num_worker_threads"] = 16
        config_file = set_config(config, db_config, ycsb_config,
                                 "configs/sample.cfg")
        try_exec(executable, config_file, trial_id)
        print(trial_id, flush=True)

