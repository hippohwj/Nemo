#ifndef ARBORETUM_SRC_COMMON_WORKER_H_
#define ARBORETUM_SRC_COMMON_WORKER_H_

#include "common/Common.h"

namespace arboretum {

class Worker {
 public:

  static OID GetThdId() { return thd_id_; };
  static void SetThdId(OID i) { thd_id_ = i; last_assigned_commit_thd_ = 0;};
  static OID GetCommitThdId() { return commit_thd_id_; };
  static void SetCommitThdId(OID i) { commit_thd_id_ = i; };
  static OID AssignCommitThd() {
    last_assigned_commit_thd_ = (last_assigned_commit_thd_ + 1) % g_commit_pool_sz;
    return last_assigned_commit_thd_ + thd_id_ * g_commit_pool_sz;
  }

  static size_t GetMaxThdTxnId() { return max_txn_id_; };
  static void SetMaxThdTxnId(OID i) { max_txn_id_ = i; };
  static void IncrMaxThdTxnId() { max_txn_id_++; };

 private:
  static __thread OID thd_id_;
  static __thread OID commit_thd_id_;
  static __thread OID last_assigned_commit_thd_;
  static __thread OID max_txn_id_;

};
}
#endif //ARBORETUM_SRC_COMMON_WORKER_H_
