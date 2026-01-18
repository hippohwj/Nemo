#include "IPage.h"


namespace arboretum {

//////////////////////////////////////
//// Buffer Page Methods         ////
/////////////////////////////////////

void *BasePage::InsertData(void *new_data, size_t size) {
  auto begin = AllocDataSpace(size);
  memcpy(begin, new_data, size);
  return begin;
}

void *BasePage::AllocDataSpace(size_t size) {
  // TODO: assume having the latches.
  // TODO: currently ignoring holes. in the future, we may delete record and has holes.
  //  once vacuum implemented, it needs to update free_space to reflect the real info and remove holes.
  // insert tuple into buffer page, return null if no space available
  if (size + sizeof(ItemPtr) > header_.free_space)
    return nullptr;
  // assign a item ptr
  auto ptr = new(GetItemPtrTail()) ItemPtr;
  // allocate data backwards
  auto begin = GetDataHead() - size;
  ptr->data = begin - header_.address;
  ptr->empty = false;
  // update page stats
  header_.data_head_offset = ptr->data;
  header_.free_space -= (size + sizeof(ItemPtr));
  header_.num_items += 1;
  header_.ptr_tail_offset = (char *) (GetItemPtrTail() + 1) - header_.address;
  return header_.address + ptr->data;
}

void *BasePage::GetData(size_t index) {
  if (index >= header_.num_items) {
    LOG_ERROR("Accessing data item at %zu exceeding total number of items %zu",
              index, header_.num_items);
  }
  M_ASSERT(index < header_.num_items,
           "Accessing data item at %zu exceeding total number of items %zu",
           index, header_.num_items);
  auto ptr = GetItemPtr(index);
  M_ASSERT(!ptr->empty, "Accessing empty data at index %zu", index);
  return header_.address + ptr->data;
}

size_t BasePage::GetDataSize(size_t index) {
  auto data_begin = GetItemPtr(index)->data;
  auto data_end = (index == 0) ? (((char *) &foot_) - (char *) this) : GetItemPtr(index - 1)->data;
  return data_end - data_begin;
}

void BasePage::ShiftIfNeeded(int64_t begin, int64_t end, int64_t dest) {
  if (begin == dest || begin > end || end >= header_.num_items)
    return;
  // LOG_DEBUG("ShiftIfNeeded: chunk [%lu, %lu], to dest %lu", begin, end, dest);
  // Note: here we assume data are of fixed sizes except for the first one (index node)!!!
  M_ASSERT(end - begin + 1 < header_.num_items && begin != 0 && dest != 0, "not impl corner case yet");
  auto ptr_head = GetItemPtrHead();
  // 1. shift data item
  //
  // | data n | data n-1 | ... | data 1 | data 0 |
  // |<- [itemptr n]      [itemptr 0 ->]|        | <- [GetDataTail()]
  auto src = header_.address + (ptr_head + end)->data;
  auto chunk_sz = header_.address + (ptr_head + begin - 1)->data - src;
  auto data_dest = header_.address + (ptr_head + dest - 1)->data - chunk_sz;
  // update each ptr position
  memmove(data_dest, src, chunk_sz);
  // LOG_DEBUG("memmove(dest=%p, src=%p, sz=%lu)", data_dest, src, chunk_sz);
  // 2. no need to shift item ptr
  // item ptrs is allocated forward
  // | ptr 0 | ptr 1 | ... | ptr n-1 | ptr n |
  // |<- [ptr_head]        |<- [ptr n - 1]   |<- ptr_tail
  if (end == header_.num_items - 1) {
    auto item_src = (char *) (ptr_head + begin);
    auto item_dest = (char *) (ptr_head + dest);
    M_ASSERT(false, "case not tested yet");
    // update ptr tail info
    // LOG_DEBUG("Updating ptr tail from %ld to %ld", header_.ptr_tail - ptr_head, dest - begin + 1);
    // header_.ptr_tail -= (dest - begin + 1);
    // change the data_head info
    // header_.data_head = header_.address + (header_.ptr_tail - 1)->data;
    // update item size and free space
    // LOG_DEBUG("Updating free space from %ld to %ld", header_.free_space,
    //           header_.free_space + (data_dest - src) + (item_src - item_dest));
    header_.free_space += (data_dest - src) + (item_src - item_dest);
    assert(header_.free_space < GetInitSpace());
    header_.num_items -= (dest - begin + 1);
  }
}

void BasePage::BatchDelete(int64_t del_begin, int64_t del_end) {
  if (del_end == -1) {
    // delete till the end
    del_end = (int) header_.num_items - 1;
    auto prev_ptr_tail = GetItemPtrTail();
    auto data_start = (char *) (del_begin == 0 ? GetDataTail() :
                                header_.address + ((GetItemPtrHead()) + del_begin - 1)->data);
    header_.ptr_tail_offset -= (char *) (prev_ptr_tail - (del_end - del_begin + 1)) - header_.address;
    header_.free_space += (data_start - GetDataHead()) +
        ((char *) prev_ptr_tail - (char *) GetItemPtrTail());
    header_.data_head_offset = (GetItemPtrTail() - 1)->data;
    header_.num_items -= (del_end - del_begin + 1);
  } else {
    ShiftIfNeeded(del_end + 1, header_.num_items - 1, del_begin);
  }
}

void *BasePage::InsertData(void *data, size_t size, OID loc) {
  M_ASSERT(loc != 0, "Do not use InsertData for inserting at head");
  auto dest = AllocDataSpaceAt(size, loc);
  memcpy(dest, data, size);
  return dest;
}
void *BasePage::AllocDataSpaceAt(size_t size, OID loc) {
  M_ASSERT(loc != 0, "Do not use InsertData for inserting at head");
  if (loc >= header_.num_items) {
    return AllocDataSpace(size);
  }
  AllocDataSpace(size);
  ShiftIfNeeded(loc, header_.num_items - 2, loc + 1);
  // update ptr of newly allocated one
  auto data_start = (GetItemPtrHead() + loc - 1)->data;
  (GetItemPtrHead() + loc)->data = data_start - size;
  return header_.address + (GetItemPtrHead() + loc)->data;
}

void BasePage::PrintPage() {
  LOG_DEBUG("========= Print Page =========");
  LOG_DEBUG("---------  [header]  ---------");
  LOG_DEBUG("tag = {tbl_id=%u, pg_id=%u}", GetTableID(), GetPageID());
  LOG_DEBUG("buf_id = %u", header_.buf_id);
  LOG_DEBUG("num_items = %ld", header_.num_items);
  LOG_DEBUG("free_space = %ld", header_.free_space);
  assert(header_.free_space < GetInitSpace());
  LOG_DEBUG("ptr_tail = %lu", header_.ptr_tail_offset);
  LOG_DEBUG("data_head = %lu", header_.data_head_offset);
//  LOG_DEBUG("---------  [ItemPtr] ---------");
  auto ptr_head = GetItemPtrHead();
//  for (int i = 0; i < header_.num_items; i++) {
//    LOG_DEBUG("[Item %d] start at: %p", i, ptr_head + i);
//  }
  LOG_DEBUG("---------  [Data] ---------");
  for (int64_t i = header_.num_items - 1; i >= 0; i--) {
    auto end = (i == 0 ? GetDataTail() : header_.address + (ptr_head + i - 1)->data);
    size_t sz = end - (header_.address + (ptr_head + i)->data);
    LOG_DEBUG("[Data %ld] relative address in pg: %ld, size: %zu",
              i, header_.address + (ptr_head + i)->data - (char *) this, sz);
  }
  assert((ItemPtr *) (header_.ptr_tail_offset + header_.address) == ptr_head + header_.num_items);
  assert(header_.data_head_offset == (ptr_head + header_.num_items - 1)->data);
  LOG_DEBUG("========= End Page =========");
}

}

