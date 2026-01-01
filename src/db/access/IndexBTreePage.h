#ifndef ARBORETUM_SRC_DB_ACCESS_INDEXBTREEPAGE_H_
#define ARBORETUM_SRC_DB_ACCESS_INDEXBTREEPAGE_H_

#include "IIndex.h"
#include "local/ITuple.h"
#include "local/IPage.h"

namespace arboretum {

class PageBufferManager;
class ITxn;

struct BTreePgNode {
  BasePage *pg_{nullptr};
  PageTag pg_tag_{};
  size_t level_{0};
  size_t key_cnt_{0};
  SearchKey *keys_{};
  PageTag *children_{}; // leaf node: reserve children_[0] for neighbor ptr
  std::atomic<uint32_t> next_key_lock_{0};
  inline bool IsLeaf() const { return level_ == 0; };
  bool HasNext() const { return children_[0].first_ != 0 || children_[0].pg_id_ != 0; };
  PageTag& GetNext() const { return children_[0]; };
  void SetNext(PageTag &tag) { children_[0] = tag; };
  void SafeInsert(SearchKey &key, PageTag &tag);
};

struct BTreePgMetaData {
  size_t fanout_{6};
  size_t height_{0};
  size_t split_bar_{0};
  std::atomic<uint32_t> num_nodes_{0};
  std::atomic<OID> root_pg_id_{};
  std::atomic<OID> root_buf_id_{}; // for fast access

  std::string ToString() const{
    return "fanout: " + std::to_string(fanout_) + ", height: " + std::to_string(height_) + ", split_bar_: " + std::to_string(split_bar_) + ", num_nodes_: " + std::to_string(num_nodes_.load()) + ", root_pg_id_: " + std::to_string(root_pg_id_.load()) + ", root_buf_id_: " + std::to_string(root_buf_id_.load());
  }
};

class IndexBTreePage : public IIndex {

  struct StackData {
    BTreePgNode * node_{nullptr};
    struct StackData * parent_{nullptr};
    bool dirty_{false};
  };
  typedef StackData *Stack;

 public:
  IndexBTreePage(OID tbl_id, ITable * tbl, PageBufferManager * buffer_manager);
  void Insert(SearchKey key, OID pg_id, OID offset);
  SharedPtr* RangeSearch(SearchKey start, SearchKey end, ITxn *txn, RC &rc, size_t &cnt);
  SharedPtr* Search(SearchKey key, int limit=1);
  bool PrintTree();

  // Note: assume a table cannot have more than 2^4 indexes
  // reserve the first 4 bit as index identifier
  OID GetIndexTableId() { 
    if (g_index_type == TWOTREE) {
      return tbl_id_ + (1 << 28);
    } else {
      return tbl_id_ + (GetIndexId() << 28);
    }
  };
  void FlushMetaData();
  BTreePgMetaData & GetMetaData() { return meta_data_; };

 private:
  void Search(SearchKey key, AccessType ac, BTreePgNode * &parent,
              BTreePgNode * &child);
  Stack PessimisticSearch(SearchKey key, BTreePgNode *&parent,
                          BTreePgNode *&child, AccessType ac);
  void RecursiveInsert(Stack stack, SearchKey key, PageTag &val,
                       BTreePgNode *child, Stack full_stack);
  BTreePgNode * AllocNewNode();
  BTreePgNode *AccessNode(OID pg_id, bool exclusive);
  void LockNode(BTreePgNode *node, bool exclusive);
  void UnLockNode(BTreePgNode *node);
  void FinishAccessNode(BTreePgNode *node, bool dirty);
  static OID ScanBranchNode(BTreePgNode *node, SearchKey key);
  static PageTag * ScanLeafNode(BTreePgNode *node, SearchKey key);
  void FreeStack(Stack full_stack);
 

  // Note: assume a table cannot have more than 2^4 indexes

  inline bool SplitSafe(BTreePgNode *node) const {
    return !node || node->key_cnt_ + 1 <= meta_data_.split_bar_;
  }
  inline bool DeleteSafe(BTreePgNode *node) const {
    return !node || node->key_cnt_ > 1;
  }
  inline bool IsRoot(BTreePgNode *node) {
    return node->pg_->header_.buf_id == meta_data_.root_buf_id_; };




 private:
  BTreePgMetaData meta_data_;
  PageBufferManager *buf_mgr_;
  std::string storage_id_;

 public:
  // for cache warm up
  void Load(size_t pg_cnt) override;
  static void ExecuteLoad(IndexBTreePage * idx, int thd_id, int num_thds, OID tbl_id, size_t cnt) {
    idx->ParallelLoad(thd_id, num_thds, tbl_id, cnt);
  };
  void ParallelLoad(int thd_id, int num_thds, OID tbl_id, size_t cnt);
};
}

#endif //ARBORETUM_SRC_DB_ACCESS_INDEXBTREEPAGE_H_
