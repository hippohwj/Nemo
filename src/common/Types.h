#ifndef ARBORETUM_DISTRIBUTED_SRC_COMMON_TYPES_H_
#define ARBORETUM_DISTRIBUTED_SRC_COMMON_TYPES_H_


#include <atomic>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <cstdint>
#include <cstring>
#include <cassert>
#include <filesystem>
#include <fstream>
#include <map>
#include <numeric>
#include <vector>
#include <queue>
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>
#include <condition_variable>
#include <mutex>
#include "Message.h"
#include "MemoryAllocator.h"
#include "RWLock.h"
#include "CircularLinkedList.h"

namespace arboretum {

// type definitions
typedef uint32_t OID; // Object ID


// enumerations
enum RC { OK, ERROR, ABORT, PENDING, NOT_FOUND};
enum IndexType { REMOTE, BTREE, TWOTREE};
// NO_OP is for record cache, GAPBIT_ONLY is for positive records with gap bit opt, GAPBIT_PHANTOM is for gap bit + phantom bounds
enum NegativeSearchOpType {NO_OP, GAPBIT_ONLY, GAPBIT_PHANTOM};
enum PhantomProtectionType {NO_PRO, PHANTOM_NEXT, NEXT_KEY, PCL};
enum TxnType { RW_TXN, INSERT_TXN, SCAN_TXN};
enum AccessType { READ, UPDATE, INSERT, DELETE, SCAN };
// B+tree insert types, USER_INSERT and DELETE_INSERT can be merged 
// CACHE_INSERT is for cache-triggered insertion, PHANTOM_INSERT is for phantom protection insertion,
// USER_INSERT is for user-triggered insertion, DELETE_INSERT is for user-triggered deletion
enum InsertType { CACHE_INSERT, PHANTOM_INSERT, USER_INSERT, DELETE_INSERT};
enum BufferType { NOBUF, PGBUF, OBJBUF, HYBRBUF, NA};
enum DataType { CHAR, VARCHAR, INTEGER, BIGINT, FLOAT8 };
// current only support three intention lock types on partition lock
enum ILockType { S, IX, IS};
DataType StringToDataType(std::string &s);
BufferType StringToBufferType(std::string &s);
IndexType StringToIndexType(std::string &s);
NegativeSearchOpType StringToNegativeOpType(std::string &s);
PhantomProtectionType StringToPhantomProtectionType(std::string &s); 

bool NeedNextKeyLock(PhantomProtectionType tpe);
bool NeedPhantom(PhantomProtectionType tpe);


std::string PhantomProtectionTypeToString(PhantomProtectionType tpe);
std::string NegativeOpTypeToString(NegativeSearchOpType tpe);
std::string BufferTypeToString(BufferType tpe);
std::string IndexTypeToString(IndexType tpe);
std::string BoolToString(bool tpe);
std::string RCToString(RC rc);

// macros
#define REMOTE_STORAGE_REDIS 1
#define REMOTE_STORAGE_TIKV 2
#define REMOTE_STORAGE_AZURE_TABLE 3
#define REMOTE_STORAGE_TYPE REMOTE_STORAGE_AZURE_TABLE
#define NEW(tpe) new (MemoryAllocator::Alloc(sizeof(tpe))) tpe
#define NEW_SZ(tpe, sz) new (MemoryAllocator::Alloc(sizeof(tpe) * (sz))) tpe[sz]
#define DEALLOC(ptr) MemoryAllocator::Dealloc(ptr)
#define DELETE(ptr, tpe) (ptr)->~tpe()
#define NANO_TO_US(t) t / 1000.0
#define NANO_TO_MS(t) t / 1000000.0
#define NANO_TO_S(t)  t / 1000000000.0

// structures
struct SearchKey {
  #define ARBORETUM_KEY_SIZE 8
  char data_[ARBORETUM_KEY_SIZE];
  uint64_t numeric_{0};
  DataType type_{DataType::BIGINT};
  explicit SearchKey(uint64_t key = 0, DataType type = DataType::BIGINT) : type_(type) {
    numeric_ = key;
    type_ = type;
    data_[ARBORETUM_KEY_SIZE - 1] = '0';
  };
  explicit SearchKey(const std::string& s) : type_(DataType::CHAR) {
    if (s.size() > ARBORETUM_KEY_SIZE) LOG_ERROR("Key size exceed limits! ");
    strcpy(data_, s.c_str());
    numeric_ = strtol(data_, nullptr, 64);
  };
  explicit SearchKey(char * s, size_t sz) : type_(DataType::CHAR) {
    if (sz > ARBORETUM_KEY_SIZE) LOG_ERROR("Key size exceed limits! ");
    memcpy(data_, s, sz);
    numeric_ = strtol(data_, nullptr, 64);
  };
  void SetValue(uint64_t key, char * data) const;
  void SetValue(uint64_t key, DataType type = DataType::BIGINT);
  std::string ToString() const;
  uint64_t ToUInt64() const { return numeric_; };
  bool operator==(const SearchKey &tag) const {
    if (type_ != DataType::VARCHAR)
      return ToUInt64() == tag.ToUInt64();
    else
      return data_ == tag.data_;
  };
  bool operator!=(const SearchKey &tag) const {
    if (type_ != DataType::VARCHAR)
      return ToUInt64() != tag.ToUInt64();
    else
      return data_ != tag.data_;
  };
  bool operator<(const SearchKey &tag) const {
    if (type_ != DataType::VARCHAR)
      return ToUInt64() < tag.ToUInt64();
    else
      return data_ < tag.data_;
  }
  bool operator>(const SearchKey &tag) const {
    if (type_ != DataType::VARCHAR)
      return ToUInt64() > tag.ToUInt64();
    else
      return data_ > tag.data_;
  }

  bool operator>=(const SearchKey &tag) const {
    if (type_ != DataType::VARCHAR)
      return ToUInt64() >= tag.ToUInt64();
    else
      return data_ >= tag.data_;
  }

  bool operator<=(const SearchKey &tag) const {
    if (type_ != DataType::VARCHAR)
      return ToUInt64() <= tag.ToUInt64();
    else
      return data_ <= tag.data_;
  }
  struct SearchKeyHash {
    size_t operator()(const SearchKey& k) const
    {
      if (k.type_ != DataType::VARCHAR) {
        return std::hash<uint64_t>()(k.ToUInt64());
      } else {
        return std::hash<std::string>()(k.ToString());
      }
    }
  };
};

// -------------------------
// | Item Identifier (TID) |
// -------------------------

struct PageTag {
  OID first_{0};
  OID pg_id_{0};
  PageTag() = default;;
  explicit PageTag(uint64_t key) {
    first_ = static_cast<uint32_t> (key >> 32);
    pg_id_ = static_cast<uint32_t> (key & 0xffffffff);
  }
  PageTag(OID tbl_id, OID pg_id) : first_(tbl_id), pg_id_(pg_id) {};
  bool IsNull() const { return first_ == 0 && pg_id_ == 0; };
  uint64_t ToUInt64() const { return ((uint64_t)first_ << 32) + pg_id_; };
  std::string ToString() const { return std::to_string(ToUInt64()); };
  SearchKey GetStorageKey() const { return SearchKey(ToUInt64()); };
  bool operator==(const PageTag &tag) const {
    return first_ == tag.first_ && pg_id_ == tag.pg_id_;
  };
  bool operator!=(const PageTag &tag) const {
    return first_ != tag.first_ || pg_id_ != tag.pg_id_;
  };
};

} // arboretum

#endif //ARBORETUM_DISTRIBUTED_SRC_COMMON_TYPES_H_
