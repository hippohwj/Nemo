#ifndef README_MD_SRC_COMMON_THREADPOOL_H_
#define README_MD_SRC_COMMON_THREADPOOL_H_


#include "Common.h"
#include "Worker.h"
#include "Stats.h"

namespace arboretum {
template <class F, class Args>
class ThreadPool {
 public:
  ThreadPool() = default;;

  struct Task {
    F func;
    Args * args;
    Task (F f, Args * a) {
      func = f;
      args = a;
    };
  };

  struct ThreadCtl {
    OID thd_id_;
    std::queue<Task> tasks_;
    std::mutex latch_;
    std::condition_variable cv_;
    explicit ThreadCtl(OID thd_id) : thd_id_(thd_id) {};
  };

  static void Execute(ThreadCtl * ctl) {
    Worker::SetCommitThdId(ctl->thd_id_);
    // LOG_INFO("CommitThread-%u starts", ctl->thd_id_);
    while (!g_terminate_exec) {
      std::unique_lock<std::mutex> lk(ctl->latch_);
      ctl->cv_.wait(lk, [ctl] { return !ctl->tasks_.empty() || g_terminate_exec; });
      if (g_terminate_exec) {
        ctl->cv_.notify_all();
        lk.unlock();
        // LOG_DEBUG("Terminate commit thread %u", ctl->thd_id_);
        break;
      }
      // collect queuing size
      if (g_warmup_finished) {
        // g_stats->commit_stats_[Worker::GetCommitThdId()].incr_num_commit_queue_samples_(1);
        // g_stats->commit_stats_[Worker::GetCommitThdId()].incr_commit_queue_sz_(ctl->tasks_.size());
      }
      auto task = ctl->tasks_.front();
      ctl->tasks_.pop();
      ctl->cv_.notify_all();
      lk.unlock();
      task.func(task.args);
    }
  }

  void Init(size_t init_num_threads = 4) {
    LOG_DEBUG("init thread pool");
    for (size_t i = 0; i < init_num_threads; i++) {
      ctls_.push_back(new ThreadCtl(i));
      thds_.emplace_back(ThreadPool::Execute, ctls_[i]);
    }
  }

  void SubmitTask(OID thd_id, F func, Args * args) {
    std::unique_lock<std::mutex> lk(ctls_[thd_id]->latch_);
    ctls_[thd_id]->cv_.wait(lk, [this, thd_id]() {
      if (g_terminate_exec || this->ctls_[thd_id]->tasks_.size() <= g_commit_queue_limit) {
        return true;
      }
      // LOG_DEBUG("waiting to submit to commit queue %u", thd_id);
      return false;
    });
    ctls_[thd_id]->tasks_.emplace(func, args);
    ctls_[thd_id]->cv_.notify_all();
  }

  void Join() {
    for (auto &ctl : ctls_) {
      ctl->cv_.notify_all();
    }
    std::for_each(thds_.begin(), thds_.end(), std::mem_fn(&std::thread::join));
  }

  std::vector<std::thread> thds_;
  std::vector<ThreadCtl *> ctls_;
};
}

#endif //README_MD_SRC_COMMON_THREADPOOL_H_
