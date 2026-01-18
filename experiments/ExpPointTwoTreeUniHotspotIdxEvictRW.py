from exp_helper import *


if __name__ == "__main__":
    # set configurations
    # cmake_dir = "../cmake-build-debug-azure/"
    cmake_dir = "../build/"
    os.chdir(cmake_dir)
    out_dir = "../output/"
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    exp_name = "exp_point_10g_twotree_unihotspot_idx_evict_rw"
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
        "g_remote_req_retries": 2000,
        "g_num_restore_thds": 8,
        "g_early_lock_release": True,
        "g_retain_idx_page": False,
        "g_zipf_random_hotspots": True,
    }
    ycsb_config = {
        "num_rows_": 10240000,
        "runtime_": 60,
    }
    working_set = 12000000000 # in bytes
    executable = "ExpYCSB"
    # try_compile(executable)

    # buf sz, read ratio, zipf theta
    exps = []
    for queue in [100]:
        # for buf_sz in [1.25, 1, 0.5, 0.25]:
        # for buf_sz in [0.25]:
        for buf_sz in [0.25]:
            # for read_perc in [1, 0.9, 0.5]:
            # for read_perc in [1, 0.9]:
            for read_perc in [0.8]:
                # for zipf in [0.3, 0.5, 0.7, 0.9]:
                # for zipf in [0.99, 0.9, 0.8, 0.7]:
                for zipf in [0.99, 0.8, 0.6, 0.4, 0.2, 0]:
                # for zipf in [0.9]:
                    exps.append([buf_sz, read_perc, zipf, queue])
        break
# set cosmos capacity at 20000 when read_prec = 0.5 and buf_sz = 0.1 for page baseline
    for exp in exps:
        max_attempts = 3
        attempts = 0
        while attempts < max_attempts:
          db_config["g_total_buf_sz"] = round(working_set * exp[0])
          bottom_size = 1000000000 # in bytes
          db_config["g_top_tree_buf_sz"] = round(working_set * exp[0]) - bottom_size 
          # db_config["g_top_tree_buf_sz"] = round(working_set * exp[0])
          db_config["g_bottom_tree_buf_sz"] = bottom_size
          ycsb_config["read_perc_"] = exp[1]
          ycsb_config["zipf_theta_"] = exp[2]
          db_config["g_commit_queue_limit"] = exp[3]
          db_config["g_num_worker_threads"] = 16

          trial_id = "azure16k-tuple-buf{}-read{}-zipf{}".format(exp[0], exp[1], exp[2])
          db_config["g_index_type"] = "TWOTREE"
          db_config["g_buf_type"] = "HYBRBUF"
          # db_config["g_index_type"] = "BTREE"
          # db_config["g_buf_type"] = "PGBUF"


          config_file = set_config(config, db_config, ycsb_config,
                                   "configs/sample.cfg")
          res = try_exec(executable, config_file, trial_id)
          print(trial_id, flush=True)
          if res: 
             break
          else: 
             attempts += 1


