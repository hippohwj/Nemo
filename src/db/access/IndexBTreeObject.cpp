#include "IndexBTreeObject.h"
#include "ITuple.h"
#include "db/buffer/ObjectBufferManager.h"
#include "db/CCManager.h"
#include "db/ITxn.h"
#include "local/ITable.h"
#include "common/CountDownLatch.h"
#include "IDataStore.h"


using namespace std;

namespace arboretum {

IndexBTreeObject::IndexBTreeObject(OID tbl_id, ITable *tbl) : IIndex(tbl_id, tbl) {
  fanout_ = g_idx_btree_fanout;
  split_bar_ = std::min((size_t)(fanout_ * g_idx_btree_split_ratio), fanout_ - 1);
  auto root_node = AllocNewNode();
  root_.Init(root_node);
  node_sz_ = sizeof(BTreeObjNode) + sizeof(SearchKey) * fanout_ +
      sizeof(SharedPtr) * (fanout_ + 1);
  LOG_INFO("Created btree index with fanout = %zu and split_threshold = %zu",
           fanout_, split_bar_);
}


#define OBJ_BTREE_RANGE_SEARCH_HELPER(lock, fetch_remote, left_bound, right_bound) { \
  M_ASSERT(right_bound.ToUInt64() > 0, "right bound should > 0");                 \
  uint64_t cc_starttime = GetSystemClock(); \
  rc = g_enable_phantom_protection ? CCManager::AcquirePhantomIntentionLock(*lock, false) : OK; \
  if (g_warmup_finished) { \
       uint64_t cctime =  GetSystemClock() - cc_starttime; \
       txn->AddCCTime(cctime); \
  }\
  if (rc != RC::OK) break;                                                        \
  if (g_enable_phantom_protection) idx_ac.locks_.push_back(lock);                 \
  if (fetch_remote) {                                                             \
    auto from = std::max(SearchKey(start.ToUInt64() - 1), left_bound);            \
    auto to = std::min(SearchKey(end.ToUInt64() + 1), right_bound);                   \
    fetch_req.emplace_back(from, to);                                             \
  }                                                                               \
}

RC IndexBTreeObject::PhantomProtectionHelper(std::atomic<uint32_t> * lock, ITxn* txn, IdxAccess& idx_ac, bool is_exclusive) {
  if (!NeedNextKeyLock(g_phantom_protection_type)) {
      // if g_phantom_protection_type is NO_PRO/PCL, no need to acquire the next-key lock
    return RC::OK;
  }
  // if g_phantom_protection_type is PHANTOM_NEXT, NEXT_KEY
  uint64_t cc_starttime = GetSystemClock(); 
  auto rc = CCManager::AcquirePhantomIntentionLock(*lock, is_exclusive); 
  if (g_warmup_finished) { 
       uint64_t cctime =  GetSystemClock() - cc_starttime; 
       txn->AddCCTime(cctime); 
  }

  if (rc != RC::OK) {
    // Abort
     return rc;
  }                                                        
  // if successfully acquired the lock
  idx_ac.locks_.push_back(lock);   
  return rc;
}

// process each entry in the range [the largest entry <= start, the largest entry <= end]
// acquire a gap lock for each entry itered. 
// if there is a gap bit set, add the range [current_key, end] to the fetch_req

RC IndexBTreeObject::RangeEntryIter(SearchKey key, ITuple* entry, SearchKey start, SearchKey end,  std::vector<std::pair<SearchKey, SearchKey>>& fetch_req,  ITxn* txn, IdxAccess& idx_ac) {
  if (NeedNextKeyLock(g_phantom_protection_type)){
    LOG_XXX("XXX Try to Acq %s next-key lock for entry %ld in range %ld to %ld", (false ? "IX" : "S"), entry->GetTID(), start.ToUInt64(), end.ToUInt64());
  }
  auto rc = PhantomProtectionHelper(&(entry->next_key_lock_), txn, idx_ac, false);
  if (NeedNextKeyLock(g_phantom_protection_type)){
    LOG_XXX("XXX Acq %s next-key lock for entry %ld, rc is %s, in range %ld to %ld", (false ? "IX" : "S"), entry->GetTID(), RCToString(rc).c_str(), start.ToUInt64(), end.ToUInt64());
  }

  if (rc != RC::OK) {
    // M_ASSERT(false, "current doesn't support phantom protection");
    LOG_XXX("RangeEntryIter current rc is %s", RCToString(rc).c_str());
    return rc;
  } else {
    // LOG_INFO("Read Lock for key %ld in range query: start %ld, end %ld", key.ToUInt64(), start.ToUInt64(), end.ToUInt64());
    if (!fetch_req.empty()) {
         fetch_req.back().second = key;
    }
    if (key != end && entry->IsGapBitSet()) {
      fetch_req.emplace_back(std::max(start, key), end);
    } 
    return rc;
  }
}

SharedPtr* IndexBTreeObject::RangeSearch(SearchKey start, SearchKey end, ITxn* txn,
                                         std::vector<std::pair<SearchKey, SearchKey>>& fetch_req, RC& rc, size_t& cnt, BTreeObjNode* hint_parent, BTreeObjNode* hint_child) {
  // RC::ERROR - key not found
  // RC::OK - key is found
  // RC::ABORT - lock conflict with insert operations.

  rc = RC::OK;


  auto result = NEW_SZ(SharedPtr, end.ToUInt64() - start.ToUInt64() + 1);
  for (size_t i = 0; i < end.ToUInt64() - start.ToUInt64() + 1; i++) {
    result[i].Init(nullptr);
  }
  txn->scan_array = result;

  IdxAccess idx_ac = {SCAN, tbl_id_};

  BTreeObjNode* parent;
  BTreeObjNode* child;
  bool valid_hint = (hint_child != nullptr) && (child->right_bound_ > start) && (child->left_bound_ <= start);

  if (valid_hint) {
    child = hint_child;
    parent = hint_parent; 
    child->latch_.LockSH();
  } else {
    // traverse the tree to find the leaf node whose range covers the start key
    Search(start, AccessType::READ, parent, child);
  }
  // find the largest entry that has a key less than or equal to the start key
  // LOG_INFO("Range search start from key %ld, located leaf node (left bound %ld, right bound %ld)", start.ToUInt64(), child->left_bound_.ToUInt64(), child->right_bound_.ToUInt64());
  M_ASSERT(child->right_bound_ > start, "right bound %ld should > start %ld", child->right_bound_.ToUInt64(), start.ToUInt64());
  M_ASSERT(child->left_bound_ <= start, "left bound %ld should <= start %ld", child->left_bound_.ToUInt64(), start.ToUInt64());

  int next_idx = -1;

  // assume the tree is protected by a smallest key (A) and a largest key (B) and the start > A and end < B
  // find the largest tuple (X) that is <= start and then iterate to the largest tuple (Y) that is <= end
  // During the iteration:
  // 1) if any entry has a gap bit set, then should fetch the range [current_key, end] from the remote storage and stop
  // tracking the fetch_req 
  // 2) acquire a gap lock for each entry itered until reach Y.

  // find the largest entry that has a key less than or equal to the start key
  if (start < child->keys_[0]) {
    // the tuple X must the the last entry in the prior leaf node.
    M_ASSERT(child->prev_ != nullptr, "the previous leaf node should exist");
    M_ASSERT(child->prev_->key_cnt_ > 0, "the previous leaf node should have at least one key");

    auto prev = const_cast<BTreeObjNode*>(child->prev_);
    prev->latch_.LockSH();
    ITuple* prev_entry = reinterpret_cast<ITuple*>(prev->children_[prev->key_cnt_].Get());
    rc = RangeEntryIter(prev->keys_[prev->key_cnt_-1], prev_entry, start, end, fetch_req, txn, idx_ac);
    prev->latch_.Unlock();
    next_idx = 0;
  } else {
    for (int i = 0; i < child->key_cnt_; i++) {
      if (child->keys_[i] > start) {
        next_idx = i - 1;
        break;
      }
    }
    if (next_idx == -1) {
      // the start key is larger than or equal to the largest key in the current leaf node
      //  and the first_item_idx should be the first item of the next leaf node
      next_idx = child->key_cnt_ - 1;
    }
  }

 

  // iterate till reaching the largest key that is <= end (by finding the first key that is > end)
  while (rc == RC::OK) {
    bool reach_end = false;
    for (int i = next_idx; i < child->key_cnt_; i++) {
      if (child->keys_[i] <= end) {
        // Iter child->children_[i + 1]
        auto entry = reinterpret_cast<ITuple*>(child->children_[i + 1].Get());
        rc = RangeEntryIter(child->keys_[i], entry, start, end, fetch_req, txn, idx_ac);
        if (rc == ABORT) {
          reach_end = true;
          break;
        }
      } else {
        // child->keys_[i] > end
        reach_end = true;
        break;
      }
    }

    if (reach_end) {
      break;
    }

    if (child->next_ != nullptr) {
      // M_ASSERT(child->right_bound_ <= end, "the last leaf node's right bound (%ld) should <= end (%ld)",
      // child->right_bound_.ToUInt64(), end.ToUInt64()); continue to next leaf.
      auto next = const_cast<BTreeObjNode*>(child->next_);
      next->latch_.LockSH();
      child->latch_.Unlock();
      child = next;
      next_idx = 0;
    } else {
      // reach end
      break;
    }
  }

  child->latch_.Unlock();

  // enable phantom protection later
  if (rc == RC::ABORT) {
    // release previously held index locks
    for (auto& lock : idx_ac.locks_) {
      CCManager::ReleasePhantomIntentionLock(*lock, false);
    }
    return nullptr;
  } else if (txn) {
    // M_ASSERT(cnt > 0 || !fetch_req.empty(), "if not abort, result should not be empty.");
    // LOG_INFO("Range query read cnt rows = %ld, fetch_req size = %ld", cnt, fetch_req.size());
    if (NeedNextKeyLock(g_phantom_protection_type))
      // track next-key locks
      txn->idx_accesses_.push_back(idx_ac);
  }

  // check the correctness of the result
  if (!g_negative_scan_wl) {
    for (size_t i = 0; i < end.ToUInt64() - start.ToUInt64() + 1; i++) {
      if (result[i].Get() && reinterpret_cast<ITuple*>(result[i].Get())->GetTID() != start.ToUInt64() + i) {
        LOG_ERROR("RangeSearch fetch error data, item index:%ld, size:%ld", i, end.ToUInt64() - start.ToUInt64() + 1);
      }
    }
  } 

  if (!fetch_req.empty()) {
    LOG_XXX("RangeSearch fetch_req size = %ld, for range query (start %ld, end %ld)", fetch_req.size(), start.ToUInt64(), end.ToUInt64());
      // produce the merged fetch_req (to make sure one range query only trigger one remote scan with miminized size)
    SearchKey min = end;
    SearchKey max = start;
    for (auto& req : fetch_req) {
      LOG_XXX("RangeSearch fetch_req item: %ld - %ld", req.first.ToUInt64(), req.second.ToUInt64());
      min = std::min(min, req.first);
      max = std::max(max, req.second);
    }
    min = std::max(min, start);
    max = std::min(max, end);
    fetch_req.clear();
    fetch_req.emplace_back(min, max);
  }

  
  return result;
}



SharedPtr* IndexBTreeObject::Search(SearchKey key, AccessType ac_type, ITxn *txn, RC &rc) {
  // RC::ERROR - key not found
  // RC::OK - key is found: 
  //          if normal record/tombstone, return the shared ptr to the tuple
  //          if hit negative gap bit, return nullptr
  // RC::ABORT - lock conflict with insert operations.
  auto result = NEW(SharedPtr);
  BTreeObjNode * parent;
  BTreeObjNode * child;
  // LOG_INFO("XXX Search key %ld", key.ToUInt64());
  uint64_t search_starttime = GetSystemClock();
  Search(key, AccessType::READ, parent, child);
  if (g_warmup_finished) {
    uint64_t stime = GetSystemClock() - search_starttime;
    LOG_XXX("XXX Search time: %ld us", stime);
  }
  // check if key in child
  int i = 0;
  rc = RC::ERROR;
  auto acq_rc = RC::OK;

  // whether this search operation is a hack for insert
  // when phantom protection is enabled, we use Search(update) to simulate the insert
  bool is_exclusive_phantom_protection = g_enable_phantom_protection && (ac_type == AccessType::UPDATE);

  IdxAccess idx_ac = {is_exclusive_phantom_protection? UPDATE: READ, tbl_id_};
  // bool found = false;
  for (i = 0; i < child->key_cnt_; i++) {
    if (child->keys_[i] == key) {

      if (is_exclusive_phantom_protection) {
        // if use search(update) to simulate insertion, need to lock the prior tuple's next_key_lock_ to prevent phantom
        auto prev_tuple = reinterpret_cast<ITuple*>(child->children_[i].Get());
        if (prev_tuple == nullptr) {
        // TODO: Deal with node bounds (get prev_tuple for left bound)
          prev_tuple = reinterpret_cast<ITuple*>(child->prev_->children_[child->prev_->key_cnt_].Get());
          M_ASSERT(prev_tuple != nullptr, "prev_tuple should not be null");
        }
        uint64_t cc_starttime = GetSystemClock();
        // acq_rc = CCManager::AcquireLock(prev_tuple->next_key_lock_, is_exclusive_phantom_protection);
        acq_rc = CCManager::AcquirePhantomIntentionLock(prev_tuple->next_key_lock_, is_exclusive_phantom_protection);
        if (g_warmup_finished) {
          uint64_t cctime = GetSystemClock() - cc_starttime;
          txn->AddCCTime(cctime);
          LOG_XXX("XXX CC time for target key %ld: %ld us", key.ToUInt64(), cctime);
        }
        // record access info in the txn on success
        if (acq_rc == RC::OK && txn) {
          // LOG_INFO("Write Lock for key %ld in insert query with key %ld", prev_tuple->GetTID(), key.ToUInt64());

          idx_ac.locks_.push_back(&prev_tuple->next_key_lock_);
          LOG_XXX("exclusive phantom protection insert key %ld, target key %ld", prev_tuple->GetTID(), key.ToUInt64());
          result->InitFrom(child->children_[i + 1]);
          rc = RC::OK;
        } else {
          LOG_XXX("failed to exclusive phantom protection insert key %ld, target key %ld",  prev_tuple->GetTID(), key.ToUInt64());
          rc = RC::ABORT;
        }
        break;
      } else {

      // for single key lookup or insertion alread exist, we don't need to acquire the next key lock
      auto tuple = reinterpret_cast<ITuple *>(child->children_[i + 1].Get());
      M_ASSERT(txn != nullptr, "txn should not be null");
      result->InitFrom(child->children_[i + 1]);
      rc = RC::OK;
      break; 
      }


    } else if (child->keys_[i] > key) {
      // no key found
      if (is_exclusive_phantom_protection) {
        // if use search(update) to simulate insertion, need to lock the prior tuple's next_key_lock_ to prevent phantom
        auto prev_tuple = reinterpret_cast<ITuple*>(child->children_[i].Get());
        if (prev_tuple == nullptr) {
          // TODO: Deal with node bounds (get prev_tuple for right bound)
          prev_tuple = reinterpret_cast<ITuple*>(child->prev_->children_[child->prev_->key_cnt_].Get());
          M_ASSERT(prev_tuple != nullptr, "prev_tuple should not be null");
        }

        uint64_t cc_starttime = GetSystemClock();
        // acq_rc = CCManager::AcquireLock(prev_tuple->next_key_lock_, is_exclusive_phantom_protection);
        acq_rc = CCManager::AcquirePhantomIntentionLock(prev_tuple->next_key_lock_, is_exclusive_phantom_protection);
        LOG_XXX("XXX Acq %s lock for key %ld on prior tuple %ld, rc is %s", (is_exclusive_phantom_protection ? "IX" : "S"), key.ToUInt64(), prev_tuple->GetTID(), RCToString(acq_rc).c_str());
        if (g_warmup_finished) {
          uint64_t cctime = GetSystemClock() - cc_starttime;
          txn->AddCCTime(cctime);
        }
        // record access info in the txn on success
        if (acq_rc == RC::OK && txn) {
          // LOG_INFO("Write Lock for key %ld in insert query with key %ld", prev_tuple->GetTID(), key.ToUInt64()); 
          // a hack way to use update to simulate insert
          result->InitFrom(child->children_[i + 1]);
          // TODO(Hippo): XXX figure out whether need this for partition_covering_lock
          idx_ac.locks_.push_back(&prev_tuple->next_key_lock_);
          rc = RC::OK;
          // LOG_INFO("exclusive phantom protection insert key %ld, target key %ld", prev_tuple->GetTID(), key.ToUInt64());
        } else {
          // LOG_INFO("failed to exclusive phantom protection insert key %ld, target key %ld",  prev_tuple->GetTID(), key.ToUInt64());
          rc = RC::ABORT;
        }
        break;
      } else if (g_enable_partition_covering_lock){
        // if partition covering lock is enabled, we directly return the next key to simulate the insert
        result->InitFrom(child->children_[i + 1]);
        rc = RC::OK;
        break;  
      }
    }
  }

  child->latch_.Unlock();

  if (acq_rc == RC::ABORT) {
    rc = RC::ABORT;
    LOG_XXX("XXX search set status to abort");
    // release previously held index locks
    for (auto & lock : idx_ac.locks_) {
      CCManager::ReleasePhantomIntentionLock(*lock, idx_ac.ac_);
    }
    DEALLOC(result);
    return nullptr;
  } else if (txn) {
    txn->idx_accesses_.push_back(idx_ac);
  }
  return result;
}


/*
 * Three types of inserts:
 *   !create_sharedptr && txn: user index insert
 *   !create_sharedptr && !txn: system index insert during warm up
 *   create_sharedptr && !txn: user reads triggering insert
 */
RC IndexBTreeObject::Insert(SearchKey key, ITuple * tuple, AccessType ac,
                                    ITxn *txn, SharedPtr *tag) {
  // if ((!g_negative_search_op_enable) && tuple->IsTombstone()) {
  //   M_ASSERT(false, "Not possible to insert tombstone");
  // }
  // M_ASSERT(false, "differentiate user insert and cache insert")
  InsertType insert_type = InsertType::CACHE_INSERT;
  BTreeObjNode * parent;
  BTreeObjNode * child;
  Search(key, AccessType::INSERT,parent, child);
  RC rc;
  // the last two layers are EX locked, see if child and parent will split
  if (!SplitSafe(child) && !SplitSafe(parent)) {
    // restart with all exclusive lock from non-split safe point.
    child->latch_.Unlock();
    parent->latch_.Unlock();
    // LOG_DEBUG("may split more than 2 levels, start pessimistic search!");
    auto stack = PessimisticSearch(key, parent, child, AccessType::INSERT);
    // insert bottom-up recursively with a stack.
    rc = RecursiveInsert(insert_type, stack, key, tuple, child, stack, ac, txn, tag);
    child->latch_.Unlock();
  } else {
    auto stack = NEW(StackData);
    stack->parent_ = nullptr;
    stack->node_ = parent;
    rc = RecursiveInsert(insert_type, stack, key, tuple, child, stack, ac, txn, tag);
    child->latch_.Unlock();
  }
  return rc;
}

/*
 * phantom insert is only used for range query phantom protection
 */
RC IndexBTreeObject::PhantomInsert(SearchKey key, ITuple* tuple, AccessType ac, ITxn* txn, SharedPtr* tag, BTreeObjNode** hint_parent, BTreeObjNode** hint_child) {
  BTreeObjNode* parent;
  BTreeObjNode* child;
  InsertType insert_type = InsertType::PHANTOM_INSERT;
  Search(key, AccessType::INSERT, parent, child);
  // judge if the key exits
  for (size_t i = 0; i < child->key_cnt_; i++) {
    if (child->keys_[i] == key) {
      // key already exists, no need to insert
      child->latch_.Unlock();
      if (parent) {
        parent->latch_.Unlock();
      }
      *hint_child = child;
      *hint_parent = parent;
      return RC::OK;
    }
  }

  RC rc;
  // the last two layers are EX locked, see if child and parent will split
  if (!SplitSafe(child) && !SplitSafe(parent)) {
    M_ASSERT(false, "Doesn't support split more than 2 levels for phantom insertions for now");
  } else {
    auto stack = NEW(StackData);
    stack->parent_ = nullptr;
    stack->node_ = parent;
    rc = RecursiveInsert(insert_type, stack, key, tuple, child, stack, ac, txn, tag);
    child->latch_.Unlock();
    *hint_child = child;
    *hint_parent = parent;
  }
  return rc;
}

RC IndexBTreeObject::EndPhantomInsert(SearchKey key, ITuple* tuple, AccessType ac, ITxn* txn, SharedPtr* tag, BTreeObjNode* hint_parent, BTreeObjNode* hint_child) {
  InsertType insert_type = InsertType::PHANTOM_INSERT;
  BTreeObjNode* parent;
  BTreeObjNode* child;
  if (hint_child && hint_parent && key < hint_child->right_bound_) {
    parent = hint_parent;
    child = hint_child;
    parent->latch_.LockEX();
    child->latch_.LockEX();
  } else {
    Search(key, AccessType::INSERT, parent, child);
  }
  // judge if the key exits
  for (size_t i = 0; i < child->key_cnt_; i++) {
    if (child->keys_[i] == key) {
      // key already exists, no need to insert
      child->latch_.Unlock();
      if (parent) {
        parent->latch_.Unlock();
      }
      return RC::OK;
    }
  }

  RC rc;
  // the last two layers are EX locked, see if child and parent will split
  if (!SplitSafe(child) && !SplitSafe(parent)) {
    M_ASSERT(false, "Doesn't support split more than 2 levels for phantom insertions for now");
  } else {
    auto stack = NEW(StackData);
    stack->parent_ = nullptr;
    stack->node_ = parent;
    rc = RecursiveInsert(insert_type, stack, key, tuple, child, stack, ac, txn, tag);
    child->latch_.Unlock();
  }
  return rc;
}



void
IndexBTreeObject::Search(SearchKey key, AccessType ac,
                         BTreeObjNode *&parent, BTreeObjNode *&child) {
  // top-down traverse, acquire ex lock on second last level if ac == WRITE
  auto is_update = false;
  parent = nullptr;
  child = AccessNode(root_, (ac == INSERT || ac == DELETE)
  && (height_ <= 1));
  // In case root is out of date
  while (!IsRoot(child)) {
    child->latch_.Unlock();
    child = AccessNode(root_, (ac == INSERT || ac == DELETE)
        && (height_ <= 1));
    // LOG_ERROR("root out of date");
  }
  while (true) {
    if (child->IsLeaf()) {
      if (ac != INSERT && ac != DELETE)
        if (parent) parent->latch_.Unlock();
      break;
    }
    // search in the keys, find the offset of child
    auto offset = ScanBranchNode(child, key);
    // no need to use parent which will become grandparent
    if (parent) parent->latch_.Unlock();
    if (ac == UPDATE) {
      // TODO: currently assuming update does not change index key.
      if (child->level_ == 0) is_update = true;
    } else if (ac == INSERT || ac == DELETE) {
      if (child->level_ <= 2) is_update = true;
    }
    parent = child;
    child = AccessNode(child->children_[offset], is_update);
  }
}

  // only for concurrent buffer eviction 
void IndexBTreeObject::RandomPickLeafNode(AccessType ac, BTreeObjNode * &parent, BTreeObjNode * &child) {
  // top-down traverse, acquire ex lock on second last level if ac == WRITE
  auto is_update = false;
  parent = nullptr;
  M_ASSERT(ac == DELETE, "RandomPickLeafNode is only used for buffer eviction");
  child = AccessNode(root_, (ac == INSERT || ac == DELETE)
  && (height_ <= 1));
  // In case root is out of date
  while (!IsRoot(child)) {
    child->latch_.Unlock();
    child = AccessNode(root_, (ac == INSERT || ac == DELETE)
        && (height_ <= 1));
    // LOG_ERROR("root out of date");
  }
  while (true) {
    if (child->IsLeaf()) {
      if (ac != INSERT && ac != DELETE)
        if (parent) parent->latch_.Unlock();
      break;
    }
    // randomly pick a branch
    auto offset = RandomBranchNode(child);
    // no need to use parent which will become grandparent
    if (parent) parent->latch_.Unlock();
    if (ac == INSERT || ac == DELETE) {
      if (child->level_ <= 2) is_update = true;
    }
    parent = child;
    child = AccessNode(child->children_[offset], is_update);
  }
}


IndexBTreeObject::Stack
IndexBTreeObject::PessimisticSearch(SearchKey key, BTreeObjNode *&parent,
                                    BTreeObjNode *&child, AccessType ac) {
  // top-down traverse, acquire ex lock on second last level if ac == WRITE
  Stack stack = nullptr;
  auto is_update = true;
  parent = nullptr;
  child = AccessNode(root_, is_update);
  assert(height_ != 0);
  while (true) {
    if (child->IsLeaf()) {
      break;
    }
    // search in the keys, find the offset of child
    auto offset = ScanBranchNode(child, key);
    // save stack if it is pessimistic and may split
    auto new_stack = NEW(StackData);
    new_stack->node_ = child;
    new_stack->parent_ = stack;
    stack = new_stack;
    // no need to use parent which will become grandparent
    // TODO: check delete safe for delete operations
    bool safe = (ac == INSERT && SplitSafe(child)) ||
        (ac == DELETE && DeleteSafe(child) );
    if (safe && parent) {
      // free entire stack recursively
      assert(stack->parent_->node_ == parent);
      FreeStack(stack->parent_);
      stack->parent_ = nullptr;
    }
    parent = child;
    child = AccessNode(child->children_[offset], is_update);
  }
  return stack;
}

RC
IndexBTreeObject::RecursiveInsert(InsertType& insert_type, Stack stack, SearchKey key, void * val,
                                  BTreeObjNode *child, Stack full_stack,
                                  AccessType ac, ITxn *txn, SharedPtr *tag) {
  auto parent = stack ? stack->node_ : nullptr;
  RC rc = RC::OK;
  if (SplitSafe(child)) {
    // insert into child and return
    auto ptr = child->SafeInsert(insert_type, key, val, ac, txn, this);
    if (child->IsLeaf()) {
      if (!ptr) {
        rc = RC::ABORT;
      } else if (tag) {
        tag->InitFrom(*ptr);
      }
    }
    // delete remaining stack
    FreeStack(full_stack);
  } else {
    auto ptr = child->SafeInsert(insert_type, key, val, ac, txn, this);
    if (child->IsLeaf() && !ptr) {
      rc = RC::ABORT;
      FreeStack(full_stack);
      return rc;
    }
    if (child->IsLeaf()) {
      if (ptr->Get() != val) {
        // entry already exists, no new entry inserted; therefore, no need to split the child
        if (tag) {
          tag->InitFrom(*ptr);
        }
        FreeStack(full_stack);
        return rc;
      }
    }
    // split child and insert new key separator into parent
    auto split_pos = child->key_cnt_ / 2;
    auto split_key = child->keys_[split_pos];
    assert(split_pos - 1 >= 0);
    assert(split_pos + 1 < child->key_cnt_);
    auto new_node = AllocNewNode();
    new_node->level_ = child->level_;
    new_node->left_bound_ = split_key;
    new_node->right_bound_ = child->right_bound_;
    child->right_bound_ = split_key;
    // move from split pos to new node
    // non leaf: [1, 3, 5], [(,1), [1, 3), [3,5), [5, )], split pos = 1
    // key: [5] moved to new node (offset=2),
    // value: [(, 1], [1, 3)] moved to new node (offset = 2)
    // leaf: [1,2,3], [-, 1, 2, 3], split pos = 1, 2 is separator key
    // key: [2,3] moved to new node (offset = 1)
    auto move_start = child->IsLeaf() ? split_pos : split_pos + 1;
    auto num_ele = child->key_cnt_ - move_start;
    // LOG_INFO("Splitting node: move_start %zu, num_ele %zu",  move_start, num_ele)
    // LOG_INFO("Splitting node: before split, node is: ");
    // IndexBTreeObject::PrintNode(child);
    memmove(&new_node->keys_[0], &child->keys_[move_start],
            num_ele * sizeof(SearchKey));
    if(child->IsLeaf()) {
      memmove(&new_node->children_[1], &child->children_[split_pos + 1],
              num_ele * sizeof(SharedPtr));
      new_node->left_bound_in_storage_ = false;
      new_node->next_ = child->next_;
      new_node->prev_ = child;
      child->next_ = new_node;
      auto next = const_cast<BTreeObjNode *>(new_node->next_);
      if (next) {
        next->latch_.LockEX();
        next->prev_ = new_node;
        next->latch_.Unlock();
      }
    } else {
      memmove(&new_node->children_[0], &child->children_[split_pos + 1],
              (num_ele + 1) * sizeof(SharedPtr));
    }
    child->key_cnt_ = split_pos;
    new_node->key_cnt_ = num_ele;
    // LOG_INFO("Splitting node: after splitting nodes are: ");
    // IndexBTreeObject::PrintNode(child);
    // IndexBTreeObject::PrintNode(new_node);
    // std::cout.flush();

    if (tag && child->IsLeaf()) {
      auto ptr = ScanLeafNode(key < new_node->keys_[0] ? child : new_node, key);
      tag->InitFrom(*ptr);
    }
    if (IsRoot(child)) {
      // split root by adding a new root
      auto new_root = AllocNewNode();
      new_root->level_ = child->level_ + 1;
      new_root->children_[0].Init(child);
      new_root->latch_.LockEX();
      new_root->SafeInsert(insert_type, split_key, new_node, ac, txn, this);
      new_root->latch_.Unlock(); // debug
      height_++;
      // LOG_INFO("Splitting root: ");
      // LOG_INFO("Splitting root: old root ");
      // PrintTree();
      // this line must happen in the end to avoid others touching new root
      root_.Init(new_root);
      // // LOG_INFO("Splitting root: new root ");
      // // PrintTree();
      // std::cout.flush();
      FreeStack(full_stack);
      return rc;
    }
    // insert into parent, parent may still be null even child cannot be root
    // in case it is split safe and not added!
    if (stack) {
      stack = stack->parent_;
      RecursiveInsert(insert_type, stack, split_key, new_node, parent, full_stack, ac,
                      txn, tag);
      // LOG_INFO("Adding spitted new node into parent:");
      // LOG_INFO("new parent is:");
      // IndexBTreeObject::PrintNode(parent);
      // PrintTree();
      // std::cout.flush();
    }
  }
  return rc; // only leaf node can abort so no need to check rc for recursive calls
}

OID IndexBTreeObject::ScanBranchNode(BTreeObjNode *node, SearchKey key) {
  // TODO: optimize to use binary search or interpolation search
  // if internal node, return the offset of the pointer to child
  // e.g. [1,3,5], [p0,p1,p2,p3]
  for (size_t i = 0; i < node->key_cnt_; i++) {
    if (key < node->keys_[i]) {
      return i;
    } else if (key == node->keys_[i]) {
      return i + 1;
    } else if (key > node->keys_[i]) {
      continue;
    }
  }
  // out of bound
  return node->key_cnt_;
}

void IndexBTreeObject::FindEvictionCandidates(BTreeObjNode *node, std::vector<SearchKey> &candidates_keys, std::vector<ITuple *> &candidates) {
  for (size_t i = 0; i < node->key_cnt_; i++) {
    auto tuple = reinterpret_cast<ITuple *>(node->children_[i + 1].Get());
    // no need to lock anything because the entire leaf node is locked for deleting traverse
    if (!ObjectBufferManager::ClockSweep(tuple)) {
      //need to evict
      candidates.push_back(tuple);
      candidates_keys.push_back(node->keys_[i]);
    }
  }
}


OID IndexBTreeObject::RandomBranchNode(BTreeObjNode *node) {
  //get a random branch 
  return ARDB::RandIntegerInclusive(0, node->key_cnt_);
}

BTreeObjNode * IndexBTreeObject::AllocNewNode() {
  if (node_sz_ == 0) {
    node_sz_ = sizeof(BTreeObjNode) + sizeof(SearchKey) * fanout_ +
        sizeof(SharedPtr) * (fanout_ + 1);
    size_t key_sz = ARBORETUM_KEY_SIZE;
    size_t pt_sz = sizeof(BTreeObjNode *);
    size_t meta_sz = MIN_NODE_META_SZ;
    minimized_node_sz_ = fanout_ * key_sz + (fanout_ + 1) * pt_sz;
    LOG_INFO("Node size is set to %zu: size of BTreeObjNode is %zu, size of SearchKey is %zu(%zu each) , size of SharedPtr is %zu (%zu each)", node_sz_, sizeof(BTreeObjNode), sizeof(SearchKey) * fanout_, sizeof(SearchKey), sizeof(SharedPtr) * (fanout_ + 1), sizeof(SharedPtr));
    LOG_INFO("Minimized node size is %zu", minimized_node_sz_);
  }
  // object buffer track the memory usage
  // ((ObjectBufferManager *) ((g_buf_type == HYBRBUF)? g_top_buf_mgr:g_buf_mgr))->AllocSpace(node_sz_);
  ((ObjectBufferManager *) ((g_buf_type == HYBRBUF)? g_top_buf_mgr:g_buf_mgr))->AllocSpace(minimized_node_sz_);
  // ((ObjectBufferManager *) g_buf_mgr)->AllocSpace(node_sz_);
  auto new_node = new (MemoryAllocator::Alloc(node_sz_)) BTreeObjNode();
  new_node->keys_ = (SearchKey *) ((char *) new_node + sizeof(BTreeObjNode));
  new_node->children_ = (SharedPtr *) ((char *) new_node->keys_ +
      sizeof(SearchKey) * fanout_);
  for (int i = 0; i < fanout_ + 1; i++) {
    new (&new_node->children_[i]) SharedPtr();
  }
  new_node->id_ = num_nodes_.fetch_add(1);
  return new_node;
}


bool BTreeObjNode::IsPriorGapSet(uint32_t key_idx) {
  return !IsPriorGapUnset(key_idx);
}

bool BTreeObjNode::IsPriorGapUnset(uint32_t idx) {
  bool is_prior_gap_unset = false;
  SharedPtr* prior_entry = nullptr;
  if (idx != 0) {
    // the prior key index is idx - 1, the corresponding entry index in childer is (idx - 1) + 1
    prior_entry = &children_[idx];
    is_prior_gap_unset = !reinterpret_cast<ITuple *>(prior_entry->Get())->IsGapBitSet();
  } else {
    // TODO: check correctness under concurrent eviction 
    if (prev_ == nullptr) {
      // no prior leaf node
      is_prior_gap_unset = false;
    } else {
      auto prev = const_cast<BTreeObjNode*>(prev_);
      prev->latch_.LockSH();
      prior_entry = &(prev->children_[prev->key_cnt_]);
      is_prior_gap_unset = !reinterpret_cast<ITuple *>(prior_entry->Get())->IsGapBitSet();
      prev->latch_.Unlock();
    }
  }
  return is_prior_gap_unset;
}

// return the pointer to the entry if insert success
// return the pointer to the existing entry if key already exist
// return nullptr if Abort
SharedPtr* BTreeObjNode::SafeUserInsert(SearchKey& key, void* val, AccessType ac, ITxn* txn, IndexBTreeObject* tree) {
  M_ASSERT(latch_.IsEXLocked(), "ex lock not hold!");
  ITuple* tuple_to_insert = reinterpret_cast<ITuple*>(val);
  // if insert to the leaf node
  // find the right insert position
  size_t idx;
  for (idx = 0; idx < key_cnt_; idx++) {
    if (keys_[idx] > key) {
      // found the right place to insert
      break;
    } else if (keys_[idx] == key) {
      // the key already exist
      if (tuple_to_insert->IsNegativeGap()) {
        reinterpret_cast<ITuple *>(children_[idx + 1].Get())->UnsetGapBit();
      }
      return &children_[idx + 1];
    }
  }

  // set up gap bit, and check gap lock
  if (IsLeaf()) {
    // get prior gap bit and check gap lock
    bool is_prior_gap_unset = false;
    SharedPtr* prior_entry = nullptr;
    RC rc = RC::OK;
    if (idx != 0) {
      // the prior key index is idx - 1, the corresponding entry index in childer is (idx - 1) + 1
      prior_entry = &children_[idx];
      ITuple* prior_tuple = reinterpret_cast<ITuple*>(prior_entry->Get());
      is_prior_gap_unset = !(prior_tuple->IsGapBitSet());
      rc = CCManager::CheckLocked(prior_tuple->next_key_lock_);
    } else {
      // TODO: check correctness under concurrent eviction
      if (prev_ == nullptr) {
        // no prior leaf node
        is_prior_gap_unset = false;
      } else {
        auto prev = const_cast<BTreeObjNode*>(prev_);
        prev->latch_.LockSH();
        prior_entry = &(prev->children_[prev->key_cnt_]);
        ITuple* prior_tuple = reinterpret_cast<ITuple*>(prior_entry->Get());
        is_prior_gap_unset = !(prior_tuple->IsGapBitSet());
        rc = CCManager::CheckLocked(prior_tuple->next_key_lock_);
        prev->latch_.Unlock();
      }
    }
    if (rc != RC::OK) {
      // ABORT
      return nullptr;
    }

    if (is_prior_gap_unset) {
      // if prior gap bit is unset, insert the phantom entry with gap bit unset
      tuple_to_insert->UnsetGapBit();
      if (tuple_to_insert->IsTombstone()) {
        // if prior_gap_unset and the tuple is tombstone, no need to insert the tuple
        return prior_entry;
      }
    }
  }

  // proceed to insert in position idx
  if (idx != key_cnt_) {
    // need to shift
    auto shift_num = key_cnt_ - idx;
    memmove(&keys_[idx + 1], &keys_[idx], sizeof(SearchKey) * shift_num);
    memmove(&children_[idx + 2], &children_[idx + 1], sizeof(SharedPtr) * shift_num);
  }
  keys_[idx] = key;
  // init ref count = 1.
  children_[idx + 1].Init(tuple_to_insert);
  key_cnt_++;
  return &children_[idx + 1];
}

// if key exist, return the existing tuple (no update on the existing tuple)
// if key not exist, insert the phantom entry no matter what the prior key's gap bit is and the phantom entry inherits the prior key's gap bit
SharedPtr* BTreeObjNode::SafePhantomInsert(SearchKey& key, void* val, AccessType ac, ITxn* txn,
                                           IndexBTreeObject* tree) {
  M_ASSERT(latch_.IsEXLocked(), "ex lock not hold!");
  ITuple* tuple_to_insert = reinterpret_cast<ITuple*>(val);
  // if insert to the leaf node
  // find the right insert position
  size_t idx;
  for (idx = 0; idx < key_cnt_; idx++) {
    if (keys_[idx] > key) {
      // found the right place to insert
      break;
    } else if (keys_[idx] == key) {
      // the key already exist
      return &children_[idx + 1];
    }
  }

  // set up gap bit
  if (IsLeaf()) {
    bool prior_gapbit_unset = IsPriorGapUnset(idx);
    if (prior_gapbit_unset) {
      // if prior gap bit is unset, insert the phantom entry with gap bit unset
      tuple_to_insert->UnsetGapBit();
    }
    // //whether need to acuqire next-key lock for phantom insert?
    // M_ASSERT(false, "to acquire next-key lock for phantom insert?");
  }

  // proceed to insert in position idx
  if (idx != key_cnt_) {
    // need to shift
    auto shift_num = key_cnt_ - idx;
    memmove(&keys_[idx + 1], &keys_[idx], sizeof(SearchKey) * shift_num);
    memmove(&children_[idx + 2], &children_[idx + 1], sizeof(SharedPtr) * shift_num);
  }

  keys_[idx] = key;
  // init ref count = 1.
  children_[idx + 1].Init(tuple_to_insert);
  key_cnt_++;
  return &children_[idx + 1];
}

SharedPtr* BTreeObjNode::SafeCacheInsert(SearchKey &key, void * val, AccessType ac,
  ITxn *txn, IndexBTreeObject *tree) {
    M_ASSERT(latch_.IsEXLocked(), "ex lock not hold!");
    // find the right insert position
    OID idx;
    for (idx = 0; idx < key_cnt_; idx++) {
      if (keys_[idx] > key) {
        if (!txn && IsLeaf()) {
          // cache triggered insert, check prior entry's gap bit
          bool is_prior_gap_unset = false;
          SharedPtr* prior_entry = nullptr;
          if (idx != 0) {
            // the prior key index is idx - 1, the corresponding entry index in childer is (idx - 1) + 1
            prior_entry = &children_[idx];
            is_prior_gap_unset = !reinterpret_cast<ITuple *>(prior_entry->Get())->IsGapBitSet();
          } else {
            // TODO: check correctness under concurrent eviction 
            if (this->prev_ == nullptr) {
              // no prior leaf node
              is_prior_gap_unset = false;
            } else {
              prior_entry = &(this->prev_->children_[this->prev_->key_cnt_]);
              is_prior_gap_unset = !reinterpret_cast<ITuple *>(prior_entry->Get())->IsGapBitSet();
            }
          }
          if (is_prior_gap_unset) {
            // no key should eixst 
            // TODO: only allow this branch for fast warm up, not for normal executions
            // M_ASSERT(false, "key %lu should not exist because the gapbit is unset in its prior key", key.ToUInt64());
            return prior_entry;
          }
  
        }
        break;
      }
      if (!txn && level_ == 0 && keys_[idx] == key) {
        // if not insert due to txn,
        // do not insert on duplicated tuple and return the existing tuple
        // LOG_DEBUG("key %lu already exist, do not insert", key.ToUInt64());
  
        if (g_negative_scan_wl) {
          auto tuple = reinterpret_cast<ITuple *>(val);
          if (!tuple->IsGapBitSet()) {
            reinterpret_cast<ITuple *>(children_[idx + 1].Get())->UnsetGapBit();
          }
        }
        return &children_[idx + 1];
      }
    }
    // if leaf node, acquire lock and register idx access in txn
    if (IsLeaf() && txn) {
      // each tuple lock from self (inclusive) to next key
      IdxAccess idx_ac = {ac, tree->GetTableId()};
      auto & lock = idx != 0 ?
          reinterpret_cast<ITuple *>(children_[idx].Get())->next_key_lock_ :
          left_bound_lock_;
      uint64_t cc_starttime = GetSystemClock(); 
      // auto rc = CCManager::AcquireLock(lock, ac != AccessType::READ && ac != SCAN);
      auto rc = CCManager::AcquirePhantomIntentionLock(lock, ac != AccessType::READ && ac != SCAN);
      if (g_warmup_finished) {
         uint64_t cctime =  GetSystemClock() - cc_starttime;
         txn->AddCCTime(cctime);
      }
      // record access info in the txn on success
      if (rc == RC::OK) {
        //TODO(Hippo): XXX figure out whether need this for partition_covering_lock
        idx_ac.locks_.push_back(&lock); // TODO: check address reference correctness.
        txn->idx_accesses_.push_back(idx_ac);
      } else {
        return nullptr; // TODO: handle abort in upper level.
      }
    }
    if (idx != key_cnt_) {
      // need to shift
      auto shift_num = key_cnt_ - idx;
      memmove(&keys_[idx + 1], &keys_[idx],
              sizeof(SearchKey) * shift_num);
      memmove(&children_[idx + 2], &children_[idx + 1],
              sizeof(SharedPtr) * shift_num);
    }
    keys_[idx] = key;
    // init ref count = 1.
    if (IsLeaf()) {
      auto tuple = reinterpret_cast<ITuple *>(val);
      // insert at idx, then need to check keys_[idx - 1]'s ptr
      // which is at children[idx].
      tuple->next_key_in_storage_ = idx == 0 ? left_bound_in_storage_ :
          reinterpret_cast<ITuple *>(children_[idx].Get())->next_key_in_storage_;
      children_[idx + 1].Init(tuple);
    } else {
      children_[idx + 1].Init(reinterpret_cast<BTreeObjNode *>(val));
    }
    key_cnt_++;
    return &children_[idx + 1];
}

SharedPtr* BTreeObjNode::SafeInsert(InsertType& insert_type, SearchKey& key, void* val, AccessType ac, ITxn* txn,
                                    IndexBTreeObject* tree) {
  switch (insert_type) {
    case USER_INSERT:
      return SafeUserInsert(key, val, ac, txn, tree);
    case CACHE_INSERT:
      return SafeCacheInsert(key, val, ac, txn, tree);
    case PHANTOM_INSERT:
      return SafePhantomInsert(key, val, ac, txn, tree);
    default:
      M_ASSERT(false, "Invalid insert type");
      return nullptr;
  }
}



void IndexBTreeObject::Delete(SearchKey key, ITuple * tuple, bool* deleted) {
  BTreeObjNode * parent;
  BTreeObjNode * child;
  Search(key, AccessType::DELETE, parent, child);

  if (deleted) *deleted = false;

  if (!DeleteSafe(child) && !DeleteSafe(parent)) {
    child->latch_.Unlock();
    parent->latch_.Unlock();
    auto stack = PessimisticSearch(key, parent, child, DELETE);
    RecursiveDelete(stack, key, child, tuple, stack, deleted);
  } else {
    auto stack = NEW(StackData);
    stack->parent_ = nullptr;
    stack->node_ = parent;
    RecursiveDelete(stack, key, child, tuple, stack, deleted);
  }
  child->latch_.Unlock();
}

void IndexBTreeObject::Delete(SearchKey key, ITuple * tuple) {
  Delete(key, tuple, nullptr);
}

bool BTreeObjNode::SafeDelete(SearchKey &key, ITuple *tuple, bool* deleted) {
  M_ASSERT(latch_.IsEXLocked(), "ex lock not hold!");
  bool empty = false;
  OID idx;
  if (IsLeaf()) {
    for (idx = 0; idx < key_cnt_; idx++) {
      if (key == keys_[idx]) {
        if (tuple != nullptr) {
          M_ASSERT(children_[idx + 1].Get() == tuple, "The tuple doesn't match the expected tuple to delete");
        }
        break;
      }
    }
    if (idx >= key_cnt_) {
      if (deleted) *deleted = false;
      return false; // not found
    }
    if (deleted) *deleted = true;

    bool is_tombstone = reinterpret_cast<ITuple *>(children_[idx + 1].Get())->IsTombstone();
    bool is_gapbit_set = reinterpret_cast<ITuple *>(children_[idx + 1].Get())->IsGapBitSet();
    bool need_update_prior_gapbit = is_gapbit_set || !is_tombstone;

    children_[idx + 1].Free();
    if (need_update_prior_gapbit) {
      if (idx != 0 ) {
        reinterpret_cast<ITuple *>(children_[idx].Get())->next_key_in_storage_ = true;
      } else {
        left_bound_in_storage_ = true;
      }
    }

    auto shift_num = key_cnt_ - idx - 1;
    if (shift_num > 0) {
      memmove(&keys_[idx], &keys_[idx + 1],
              sizeof(SearchKey) * shift_num);
      memmove(&children_[idx + 1], &children_[idx + 2],
              sizeof(SharedPtr) * shift_num);
    }
    key_cnt_--;
    empty = (key_cnt_ == 0);
  } else {
    for (idx = 0; idx < key_cnt_; idx++) {
      if (key < keys_[idx]) break;
    }
    children_[idx].Free();
    auto shift_num = key_cnt_ > idx ? key_cnt_ - idx - 1 : 0;
    if (shift_num > 0) {
      memmove(&keys_[idx], &keys_[idx + 1],
              sizeof(SearchKey) * shift_num);
    }
    shift_num = key_cnt_ - idx;
    if (shift_num > 0) {
      memmove(&children_[idx], &children_[idx + 1],
              sizeof(SharedPtr) * shift_num);
    }
    if (key_cnt_ == 0) {
      empty = true;
    } else {
      key_cnt_--;
    }
  }
  return empty;
}

bool BTreeObjNode::SafeDelete(SearchKey &key, ITuple *tuple) {
  bool ignored_deleted = false;
  return SafeDelete(key, tuple, &ignored_deleted);
}



bool IndexBTreeObject::EvictRandomLeafNode() {
  BTreeObjNode *parent;
  BTreeObjNode *child;
  //TODO: check when usage bit adds one
  RandomPickLeafNode(AccessType::DELETE, parent, child);
  // find the number of eviction candidates 
  std::vector<SearchKey> keys;
  std::vector<ITuple *> tuples;
  FindEvictionCandidates(child, keys, tuples);
  size_t evict_tuple_num = keys.size();
  if (evict_tuple_num == 0) {
    // LOG_DEBUG("The chosen leaf node has no eviction candidates");
    child->latch_.Unlock();
    parent->latch_.Unlock();
    return false;
  } else {
    // LOG_INFO("Index Tree Before a eviction:");
    // PrintTree();
    // std::cout.flush();
    if (!DeleteSafe(child, evict_tuple_num) && !DeleteSafe(parent)) {
      child->latch_.Unlock();
      parent->latch_.Unlock();
      // avoid choosing a underflow leaf node as the eviction candidate node.
      M_ASSERT(false, "Eviction may incur splitting more than 2 levels, start pessimistic search!");
    } else {
      // LOG_DEBUG("Will Evict %ld tuples from the Tree", evict_tuple_num);
      auto stack = NEW(StackData);
      stack->parent_ = nullptr;
      stack->node_ = parent;

      // writeback dirty data to storage
      std::unordered_map<uint64_t, std::multimap<std::string, std::string>> flush_set;
      size_t dirty_tuple_num = 0;
      for (size_t i = 0; i < evict_tuple_num; i++) {
        auto tuple = tuples[i];
        auto key = keys[i];
        if (ObjectBufferManager::CheckFlag(tuple->buf_state_, BM_DIRTY)) {
          dirty_tuple_num++;
          std::string flush_data(tuple->GetData(), table_->GetTupleDataSize());
          flush_set[key.ToUInt64() / g_partition_sz].emplace(key.ToString(), flush_data);
        }
      }
      int64_t num_parts = flush_set.size();
      if (num_parts > 0) {
        // LOG_INFO("[IndexBTreeObject] start a batch eviction for %d tuples with %d write-back tuples", evict_tuple_num, dirty_tuple_num);
        // std::cout.flush();
        std::string tbl_name = table_->GetStorageId();
        CountDownLatch latch(num_parts);
        for (auto &pair : flush_set) {
          g_data_store->WriteBatchAsyncCallback(tbl_name, std::to_string(pair.first),  pair.second, [&latch]() {
            latch.CountDown();
          });
        }
        latch.Wait();
        // LOG_INFO("[IndexBTreeObject] finish a batch eviction for %d tuples with %d write-back tuples", evict_tuple_num, dirty_tuple_num);
        // std::cout.flush();
      }
      RecursiveDeleteMultiple(stack, keys, child, tuples, stack);
    }
    child->latch_.Unlock();
    return true;
  }
}



void IndexBTreeObject::RecursiveDelete(IndexBTreeObject::Stack stack,
                                       SearchKey key, BTreeObjNode *child,
                                       ITuple * tuple,
                                       IndexBTreeObject::Stack full_stack,
                                       bool* deleted) {
  auto parent = stack ? stack->node_ : nullptr;
  if (DeleteSafe(child)) {
    child->SafeDelete(key, tuple, deleted);
    FreeStack(full_stack);
  } else {
    bool empty = child->SafeDelete(key, tuple, deleted);
    if (empty) {
      if (child->IsLeaf()) {
        auto prev = const_cast<BTreeObjNode *>(child->prev_);
        if (prev) {
          while (true) {
            child->latch_.Unlock(); // avoid deadlock
            prev->latch_.Lock(true);
            child->latch_.Lock(true);
            if (child->prev_ == prev) {
              prev->next_ = child->next_;
              prev->latch_.Unlock();
              break;
            } else {
              prev->latch_.Unlock();
              prev = const_cast<BTreeObjNode *>(child->prev_);
              if (!prev) break; // someone may delete prev node.
            }
          }
        }
        auto next = const_cast<BTreeObjNode *>(child->next_);
        if (next) {
          next->latch_.Lock(true);
          child->next_->prev_ = prev;
          next->left_bound_ = child->left_bound_;
          next->latch_.Unlock();
        }
      }
      ((ObjectBufferManager *) ((g_buf_type == HYBRBUF)? g_top_buf_mgr:g_buf_mgr))->DeallocSpace(minimized_node_sz_);
      if (stack) {
        stack = stack->parent_;
        RecursiveDelete(stack, key, parent, nullptr, full_stack, deleted);
      }
    } else {
      FreeStack(full_stack);
    }
  }
}


void IndexBTreeObject::RecursiveDeleteMultiple(IndexBTreeObject::Stack stack,
                                               std::vector<SearchKey> keys, BTreeObjNode *child,
                                               std::vector<ITuple *> tuples,
                                               IndexBTreeObject::Stack full_stack) {
  auto parent = stack ? stack->node_ : nullptr;
  size_t tuple_num = keys.size();
  if (DeleteSafe(child, tuple_num)) {
    size_t tuple_num = keys.size();
    for (size_t i = 0; i < tuple_num; ++i) {
      child->SafeDelete(keys[i], tuples[i]);
    }
    ((ObjectBufferManager *) ((g_buf_type == HYBRBUF)? g_top_buf_mgr:g_buf_mgr))->DeallocTuples(tuple_num);
    FreeStack(full_stack);
  } else {
    // M_ASSERT(false, "RecursiveDeleteMultiple should go to this branch");
    bool empty = false;
    size_t tuple_num = keys.size();
    for (size_t i = 0; i < tuple_num; ++i) {
      empty = child->SafeDelete(keys[i], tuples[i]);
      if (empty) {
        M_ASSERT(i == tuple_num - 1, "The last tuple should make the node empty");
        break;
      }
    }
    ((ObjectBufferManager *) ((g_buf_type == HYBRBUF)? g_top_buf_mgr:g_buf_mgr))->DeallocTuples(tuple_num);
    if (empty) {
      if (child->IsLeaf()) {
        auto prev = const_cast<BTreeObjNode *>(child->prev_);
        if (prev) {
          while (true) {
            child->latch_.Unlock(); // avoid deadlock
            prev->latch_.Lock(true);
            child->latch_.Lock(true);
            if (child->prev_ == prev) {
              prev->next_ = child->next_;
              prev->latch_.Unlock();
              break;
            } else {
              prev->latch_.Unlock();
              prev = const_cast<BTreeObjNode *>(child->prev_);
              if (!prev) break; // someone may delete prev node.
            }
          }
        }
        auto next = const_cast<BTreeObjNode *>(child->next_);
        if (next) {
          next->latch_.Lock(true);
          child->next_->prev_ = prev;
          next->left_bound_ = child->left_bound_;
          next->latch_.Unlock();
        }
      } else {
        M_ASSERT(false, "Eviction Delete shouldn't make intermediate node empty");
      }
      ((ObjectBufferManager *) ((g_buf_type == HYBRBUF)? g_top_buf_mgr:g_buf_mgr))->DeallocSpace(minimized_node_sz_);
      if (stack) {
        stack = stack->parent_;
        RecursiveDelete(stack, keys[0], parent, nullptr, full_stack, nullptr);
      }
    } else {
      FreeStack(full_stack);
    }
  }
}

void IndexBTreeObject::PrintNode(BTreeObjNode * node) {
  std::cout << "node-" << node->id_ << " (level=" << node->level_
            << ", #keys=" << node->key_cnt_ << ")" << std::endl;
  if (!node->IsLeaf() && node->key_cnt_ == 0) {
    auto child = AccessNode(node->children_[0]);
    std::cout << "\t[ptr=node-" << child->id_ << "]" << std::endl;
    return;
  }
  for (auto i = 0; i < node->key_cnt_; i++) {
    if (node->IsLeaf()) {
      std::cout << "\t(key=" << node->keys_[i].ToString() << ", row=" <<
                node->children_[i+1].Get() << "), ";
      continue;
    } else {
      if (i == 0) {
        auto child = AccessNode(node->children_[0]);
        std::cout << "\t[ptr=node-" << child->id_;
      }
      auto child = AccessNode(node->children_[i + 1]);
      std::cout << ", key=" << node->keys_[i].ToString() << ", ptr=node-"
                << child->id_;
      if (i == node->key_cnt_ - 1)
        std::cout << "]";
    }
  }
  std::cout << std::endl;
}

bool IndexBTreeObject::PrintTree(ITuple * p_tuple) {
  bool found = false;
  LOG_DEBUG("==== Print Tree ====");
  std::queue<BTreeObjNode *> queue;
  auto node = AccessNode(root_);
  queue.push(node);
  while (!queue.empty()) {
    node = queue.front();
    queue.pop();
    if (!node)
      continue;
    std::cout << "node-" << node->id_ << " (level=" << node->level_
              << ", #keys=" << node->key_cnt_ << ", left bound="<< node->left_bound_.ToUInt64() <<", right bound="<< node->right_bound_.ToUInt64() <<" )" << std::endl;
    if (!node->IsLeaf() && node->key_cnt_ == 0) {
      auto child = AccessNode(node->children_[0]);
      std::cout << "\t[ptr=node-" << child->id_ << "]" << std::endl;
      queue.push(child);
      continue;
    }
    for (auto i = 0; i < node->key_cnt_; i++) {
      if (node->IsLeaf()) {
        bool gapbit_set = (reinterpret_cast<ITuple *>(node->children_[i+1].Get()))->IsGapBitSet();
        bool is_tombstone = (reinterpret_cast<ITuple *>(node->children_[i+1].Get()))->IsTombstone();
        std::cout << "\t(key=" << node->keys_[i].ToString() << ", row=" <<
                  node->children_[i+1].Get() <<", gapbit =" << (gapbit_set? 1: 0) << ", phantom=" << (is_tombstone? 1:0)<< "), ";
        if (p_tuple && node->children_[i+1].Get() == p_tuple)
          found = true;
        continue;
      } else {
        if (i == 0) {
          auto child = AccessNode(node->children_[0]);
          std::cout << "\t[ptr=node-" << child->id_;
          queue.push(child);
        }
        auto child = AccessNode(node->children_[i + 1]);
        std::cout << ", key=" << node->keys_[i].ToString() << ", ptr=node-"
                  << child->id_;
        if (i == node->key_cnt_ - 1)
          std::cout << "]";
        queue.push(child);
      }
    }
    std::cout << std::endl;
  }
  LOG_DEBUG("==== End Print ====");
  return found;
}

SharedPtr *IndexBTreeObject::ScanLeafNode(BTreeObjNode *node, SearchKey key) {
  for (size_t i = 0; i < node->key_cnt_; i++) {
    if (key < node->keys_[i]) {
      break;
    } else if (key == node->keys_[i]) {
      return &node->children_[i + 1];
    }
  }
  return nullptr;
}

void IndexBTreeObject::FreeStack(IndexBTreeObject::Stack full_stack) {
  auto stack = full_stack;
  while (stack) {
    if (stack->node_) stack->node_->latch_.Unlock();
    auto used_stack = stack;
    stack = stack->parent_;
    DEALLOC(used_stack);
  }
}

void IndexBTreeObject::MarkRightBoundAvailInStorage() {
  BTreeObjNode * parent;
  BTreeObjNode * child;
  SearchKey key(UINT64_MAX - 1);
  Search(key, AccessType::READ, parent, child);
  auto tuple = reinterpret_cast<ITuple *>(child->children_[child->key_cnt_].Get());
  tuple->next_key_in_storage_ = true;
  child->latch_.Unlock();
}

} // arboretum