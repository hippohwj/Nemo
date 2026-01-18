from exp_helper import *
import sys

if __name__ == "__main__":
    # set configurations
    # cmake_dir = "../cmake-build-debug-azure/"
    cmake_dir = "../build/"
    os.chdir(cmake_dir)
    out_dir = "../output/"
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    exp_name = "exp_point_cdf"
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
        "g_enable_latency_distr_log": True,
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
    # 1 tuple 2 page 3 two-tree
    for baseline in [1, 2, 3]:
        for buf_sz in [0.25]:
            for read_perc in [1, 0.8]:
                for zipf in [0.9]:
                  if baseline in [2, 3]:
                     #for page index or two-tree
                     for idx_retain in [True, False]: 
                        exps.append([buf_sz, read_perc, zipf, baseline, idx_retain])
                  else:
                     exps.append([buf_sz, read_perc, zipf, baseline])


# set cosmos capacity at 20000 when read_prec = 0.5 and buf_sz = 0.1 for page baseline
    for exp in exps:
        max_attempts = 3
        attempts = 0
        baseline = exp[3]
        buf_sz = exp[0]
        read_perc = exp[1]
        zipf = exp[2]
        if len(exp) == 5:
          idx_retain = exp[4]
        else:
          idx_retain = False
           
        while attempts < max_attempts:
          if baseline == 1:
            db_config["g_index_type"] = "BTREE"
            db_config["g_buf_type"] = "OBJBUF"
          elif baseline == 2:
            db_config["g_index_type"] = "BTREE"
            db_config["g_buf_type"] = "PGBUF"
          elif baseline == 3:
            db_config["g_index_type"] = "TWOTREE"
            db_config["g_buf_type"] = "HYBRBUF"
            bottom_size = 1000000000 # in bytes
            db_config["g_top_tree_buf_sz"] = round(working_set * buf_sz) - bottom_size 
            db_config["g_bottom_tree_buf_sz"] = bottom_size
          else:
             sys.exit("Unknown baseline!")

          db_config["g_total_buf_sz"] = round(working_set * buf_sz)
          ycsb_config["read_perc_"] = read_perc
          ycsb_config["zipf_theta_"] = zipf
          db_config["g_retain_idx_page"] = idx_retain 
          trial_id = "azure-idxtype{}-buf{}-read{}-zipf{}-idxretain{}".format(baseline, buf_sz, read_perc, zipf, idx_retain)
          config_file = set_config(config, db_config, ycsb_config,
                                   "configs/sample.cfg")
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



