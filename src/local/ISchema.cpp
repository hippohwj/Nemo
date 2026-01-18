#include "ISchema.h"

namespace arboretum {

void ISchema::AddCol(const std::string &col_name, uint32_t size, DataType type, bool is_pkey) {
  new (&columns_[col_cnt_]) IColumn(col_cnt_, size, type, tuple_sz_, col_name );
  tuple_sz_ += size;
  if (is_pkey) {
    pkey_ids_.push_back(col_cnt_);
  }
  col_cnt_++;
}

}