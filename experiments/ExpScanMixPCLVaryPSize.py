from exp_helper import *


if __name__ == "__main__":
    # set configurations
    # cmake_dir = "../cmake-build-debug-azure/"
    cmake_dir = "../build/"
    os.chdir(cmake_dir)
    out_dir = "../output/"
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    #exp_name = "exp_ycsbe_10g_fanout250_analysis"
    # exp_name = "exp_mix_scan_partition_lock_Psize_debug"
    # exp_name = "exp_mix_pcl_varying_psize_high_contention"
    exp_name = "exp_mix_pcl_varying_psize_low_contention"
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
        # "g_commit_group_sz": 300,
        "g_commit_group_sz": 0,
        "g_commit_pool_sz": 1,
        # "g_log_freq_us": 50,
        "g_log_freq_us": 100,
        "g_remote_req_retries": 3,
        "g_num_restore_thds": 8,
        "g_early_lock_release": True,
        "g_enable_phantom_protection": False,
        "g_zipf_random_hotspots": False,
        "g_enable_partition_covering_lock": True,
        "g_scan_zipf_random_hotspots": False,
        "g_scan_length": 99,
        "g_scan_workload_rw_thd_num": 0,
    }
    ycsb_config = {
        "num_rows_": 10240000,
        "runtime_": 15,
        # "zipf_theta_": 0.877,
        "zipf_theta_": 0.9,
        # "zipf_theta_": 0,
        "read_perc_": 0,
        "rw_txn_perc_": 0.25,
        "num_req_per_query_": 1,
    }
    working_set = 12000000000 # in bytes
    executable = "ExpYCSB"
    # try_compile(executable)

    # buf sz, read ratio, zipf theta
    exps = []
    # for buf_sz in [1.25, 0.75, 0.5, 0.25]:
    # for buf_sz in [1.25]:
    # for buf_sz in [1.25]:
    # for pt_sz in [1000000, 10000, 100]:
    # for pt_sz in [25000]:
    # for pt_sz in [100000, 50000, 25000, 10000, 1000, 1]:
    for pt_sz in [1]:
    #   for buf_sz in [1.25, 0.75, 0.25]:
    #   for buf_sz in [0.75, 0.25]:
      for buf_sz in [0.75]:
        # for zipf in [0.2, 0.4, 0.6, 0.8, 0.99]:
        # for zipf in [0.99, 0.4]:
        for zipf in [0.4]:
          exps.append([buf_sz, pt_sz, zipf])

    for exp in exps:
        max_attempts = 3
        attempts = 0
        while attempts < max_attempts:
          db_config["g_total_buf_sz"] = round(working_set * exp[0])
          db_config["g_partition_covering_lock_unit_sz"] = exp[1]
          db_config["g_num_worker_threads"] = 16

          trial_id = "azure16k-tuple-buf{}".format(exp[0])
          # ycsb_config["rw_txn_perc_"] = exp[2]
          ycsb_config["zipf_theta_"] = exp[2]
          db_config["g_index_type"] = "BTREE"
          db_config["g_buf_type"] = "OBJBUF"
          config_file = set_config(config, db_config, ycsb_config,
                                   "configs/sample_scan.cfg")
          res = try_exec(executable, config_file, trial_id)
          if res: 
             break
          else: 
             attempts += 1
        if res == False:
           print("failed")
        else: 
           print("success after {} attempts".format(attempts))
        print(trial_id, flush=True)

        # trial_id = "azure16k-page-buf{}".format(exp[0])
        # db_config["g_index_type"] = "BTREE"
        # db_config["g_buf_type"] = "PGBUF"
        # db_config["g_num_worker_threads"] = 16
        # config_file = set_config(config, db_config, ycsb_config,
        #                          "configs/sample_scan.cfg")
        # try_exec(executable, config_file, trial_id)
        # print(trial_id, flush=True)
