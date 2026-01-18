#include "Worker.h"

namespace arboretum {

__thread OID Worker::thd_id_;
__thread OID Worker::commit_thd_id_;
__thread OID Worker::max_txn_id_;
__thread OID Worker::last_assigned_commit_thd_;

}


