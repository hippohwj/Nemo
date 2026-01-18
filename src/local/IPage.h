#ifndef ARBORETUM_SRC_LOCAL_IPAGE_H_
#define ARBORETUM_SRC_LOCAL_IPAGE_H_

#include "common/Common.h"

namespace arboretum {

#define AR_PAGE_INIT_SPACE AR_PAGE_SIZE - sizeof(PageHeader) - sizeof(PageFoot)

// Page Layout
// || header | item pointers | item data | foot ||
// || PageHeader | ItemPtr * | void * | PageFoot ||
// header: store information on page id, stats of the page, and ptrs to the first item pointer
struct BasePage {

  struct ItemPtr {
    bool empty{true};
    size_t data{0};
  };

  struct PageHeader {
    PageTag tag;
    OID buf_id{};
    uint64_t ptr_tail_offset{0};
    uint64_t data_head_offset{0};
    size_t free_space{};
    int64_t num_items{0};
    int64_t num_holes{0}; // number of ptrs with deleted data
    char * address{nullptr};
  };

  struct PageFoot {
    PageTag previous{};
    PageTag next{};
  };

  PageHeader header_{};
  char data_[AR_PAGE_INIT_SPACE]{};
  PageFoot foot_{};

  BasePage() {
    auto begin = (char *) this;
    auto item_ptr_begin = (char *) &data_;
    auto data_begin = (char *) &foot_;
    header_ = {PageTag(), 0, static_cast<uint64_t>(item_ptr_begin - begin),
               static_cast<uint64_t>(data_begin - begin),
               AR_PAGE_INIT_SPACE, 0, 0, begin};
  }

  void SetFromLoaded(const char * data, size_t sz) {
    memcpy(this, data, sz);
    header_.address = (char *) this;
  }

  void ResetPage() {
    auto begin = (char *) this;
    auto item_ptr_begin = (char *) &data_;
    auto data_begin = (char *) &foot_;
    // reserve buffer id
    header_.ptr_tail_offset = item_ptr_begin - begin;
    header_.data_head_offset = data_begin - begin;
    header_.free_space = AR_PAGE_INIT_SPACE;
    header_.num_items = 0;
    header_.num_holes = 0;
  }
  inline size_t MaxSizeOfNextItem() const {
    if (header_.free_space <= sizeof(ItemPtr)) return 0;
    return header_.free_space - sizeof(ItemPtr); };
  inline OID GetMaxOffsetNumber() const { return header_.num_items ? OID(header_.num_items) - 1 : 0; };
  inline size_t GetFreeSpace() const { return header_.free_space; };
  static inline int64_t GetInitSpace() { return AR_PAGE_INIT_SPACE; };
  inline char * GetDataTail() { return reinterpret_cast<char *>(&foot_); };
  inline char * GetDataHead() { return reinterpret_cast<char *>(header_.address + header_.data_head_offset); };
  inline ItemPtr * GetItemPtrHead() { return reinterpret_cast<ItemPtr *>(data_); };
  inline ItemPtr * GetItemPtrTail() { return reinterpret_cast<ItemPtr *>(header_.address + header_.ptr_tail_offset); };
  inline ItemPtr * GetItemPtr(size_t idx) { return GetItemPtrHead() + idx; }
  inline OID GetPageID() const { return header_.tag.pg_id_; };
  inline OID GetTableID() const { return header_.tag.first_; };
  inline size_t GetMaxNumItems(size_t sz) const { return header_.free_space / (sz + sizeof(ItemPtr)); };
  void *GetData(size_t index);
  size_t GetDataSize(size_t index);
  void *InsertData(void *data, size_t size);
  void *InsertData(void *data, size_t size, OID loc);
  void *AllocDataSpace(size_t size);
  void *AllocDataSpaceAt(size_t size, OID loc);
  void BatchDelete(int64_t begin, int64_t end = -1); // inclusive [begin, end]
  void ShiftIfNeeded(int64_t begin, int64_t end, int64_t dest);
  // debugging method
  void PrintPage();
};
}

#endif //ARBORETUM_SRC_LOCAL_IPAGE_H_
