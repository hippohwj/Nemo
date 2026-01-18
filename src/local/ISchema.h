#ifndef ARBORETUM_SRC_LOCAL_ISCHEMA_H_
#define ARBORETUM_SRC_LOCAL_ISCHEMA_H_

#include "common/Common.h"

namespace arboretum {

class IColumn {
 public:
  IColumn(uint64_t id, uint64_t sz, DataType type, size_t offset,
          const std::string& name) : id_(id), sz_(sz), type_(type),
          offset_(offset) { memcpy(name_, name.c_str(), name.size()); };
  inline size_t GetOffset() const { return offset_; };
  inline DataType GetType() const { return type_; };
  inline size_t GetSize() const { return sz_; };

 private:
  uint64_t id_{0};
  uint32_t sz_{0};
  DataType type_{DataType::BIGINT};
  size_t offset_{0};
  char name_[80]{};
};

class ISchema {
 public:
  ISchema(const std::string &name, int col_cnt) {
    memcpy(tbl_name_, name.c_str(), name.size());
    columns_ = (IColumn *) MemoryAllocator::Alloc(sizeof(IColumn) * col_cnt);
  };
  void AddCol(const std::string &col_name, uint32_t size, DataType type,
              bool is_pkey = false);
  uint32_t GetColumnCnt() const { return col_cnt_; };
  size_t GetFieldOffset(OID col_id) const { return columns_[col_id].GetOffset(); };
  DataType GetFieldType(OID col_id) const { return columns_[col_id].GetType(); };
  size_t GetFieldSize(OID col_id) const { return columns_[col_id].GetSize(); };
  uint32_t GetTupleSz() const { return tuple_sz_; };
  std::vector<uint32_t>& GetPKeyColIds() { return pkey_ids_; };
  template <typename T> void SetNumericField(OID col, char * data, T value) {
    *((T *) &data[GetFieldOffset(col)]) = value;
  };
  template <typename T> void SetCharField(OID col, char * data, T value, size_t sz) {
    memcpy(&data[GetFieldOffset(col)], (char *) value, sz);
  };
  template <typename T> void SetPrimaryKey(char * data, T value) {
    auto pkey_id = GetPKeyColIds()[0]; // assume not composite key
    auto key_type = GetFieldType(pkey_id);
    if (key_type == DataType::BIGINT || key_type == INTEGER || key_type == FLOAT8)
      SetNumericField(pkey_id, data, value);
    else {
      SetCharField(pkey_id, data, value, GetFieldSize(pkey_id));
    }
  };

  uint32_t col_cnt_{0};
  char tbl_name_[80]{};
  IColumn *columns_{nullptr};
  uint32_t tuple_sz_{0};
  std::vector<uint32_t> pkey_ids_{};
};
}

#endif //ARBORETUM_SRC_LOCAL_ISCHEMA_H_
