#include "Types.h"
#include "GlobalData.h"

namespace arboretum {

std::string SearchKey::ToString() const {
  //TODO(hippo): support two-tree;
  // M_ASSERT(false, "to support two-tree")
  if (g_buf_type == PGBUF || g_buf_type == HYBRBUF)
    return std::to_string(numeric_);
  if (data_[ARBORETUM_KEY_SIZE - 1] == '0') {
    SetValue(numeric_, (char *) &data_[0]);
  }
  return "0" + std::string(data_, ARBORETUM_KEY_SIZE);
}

void SearchKey::SetValue(uint64_t key, DataType type) {
  numeric_ = key;
  type_ = type;
  SetValue(numeric_, &data_[0]);
}

void SearchKey::SetValue(uint64_t key, char* data) const {
  auto s = std::to_string(key);
  M_ASSERT(s.size() <= ARBORETUM_KEY_SIZE, "search key exceed bound: %s", s.c_str());
  int cnt = 0;
  for (int i = ARBORETUM_KEY_SIZE - 1; i >= 0; i--) {
    cnt++;
    if (cnt <= s.size()) {
      data[i] = s[s.size() - cnt];
    } else {
      data[i] = '0';
    }
  }
}

DataType StringToDataType(std::string &s) {
  if (s == "string") {
    return DataType::CHAR;
  } else if (s == "int64_t") {
    return DataType::BIGINT;
  } else if (s == "double") {
    return DataType::FLOAT8;
  }
  LOG_ERROR("type %s not supported", s.c_str());
}

BufferType StringToBufferType(std::string &s) {
  if (s == "NOBUF") {
    return BufferType::NOBUF;
  } else if (s == "OBJBUF") {
    return BufferType::OBJBUF;
  } else if (s == "PGBUF") {
    return BufferType::PGBUF;
  } else if (s == "HYBRBUF") {
    return BufferType::HYBRBUF;
  }
  LOG_ERROR("type %s not supported", s.c_str());
}

IndexType StringToIndexType(std::string &s) {
  if (s == "REMOTE") {
    return IndexType::REMOTE;
  } else if (s == "BTREE") {
    return IndexType::BTREE;
  } else if (s== "TWOTREE") {
    return IndexType::TWOTREE;
  }
  LOG_ERROR("type %s not supported", s.c_str());
}

NegativeSearchOpType StringToNegativeOpType(std::string &s) {
  if (s == "NOOP") {
    //Record cache
    return NegativeSearchOpType::NO_OP;
  } else if (s == "GAPBIT") {
    return NegativeSearchOpType::GAPBIT_ONLY;
  } else if (s == "GAPPHANTOM") {
    return NegativeSearchOpType::GAPBIT_PHANTOM;
  } 
  LOG_ERROR("type %s not supported", s.c_str());
}


// enum PhantomProtectionType {NO_PRO, PHANTOM_NEXT, NEXT_KEY, PCL};

PhantomProtectionType StringToPhantomProtectionType(std::string &s) {
  if (s == "NO_PRO") {
    //Record cache
    return PhantomProtectionType::NO_PRO;
  } else if (s == "PHANTOM_NEXT") {
    return PhantomProtectionType::PHANTOM_NEXT;
  } else if (s == "NEXT_KEY") {
    return PhantomProtectionType::NEXT_KEY;
  } else if (s == "PCL"){
    return PhantomProtectionType::PCL;
  }

  LOG_ERROR("type %s not supported", s.c_str());
}



bool NeedNextKeyLock(PhantomProtectionType tpe) {
  return tpe == PhantomProtectionType::NEXT_KEY || tpe == PhantomProtectionType::PHANTOM_NEXT;
}

bool NeedPhantom(PhantomProtectionType tpe) {
  return tpe == PhantomProtectionType::PHANTOM_NEXT;
}

std::string PhantomProtectionTypeToString(PhantomProtectionType tpe) {
  switch (tpe) {
    case PhantomProtectionType::NO_PRO:
      return "NO_PRO";
    case PhantomProtectionType::PHANTOM_NEXT:
      return "PHANTOM_NEXT";
    case PhantomProtectionType::NEXT_KEY:
      return "NEXT_KEY";
    case PhantomProtectionType::PCL:
      return "PCL";
    default: LOG_ERROR("type not supported");
  }
}




std::string NegativeOpTypeToString(NegativeSearchOpType tpe) {
  switch (tpe) {
    case NegativeSearchOpType::NO_OP:
      return "NOOP";
    case NegativeSearchOpType::GAPBIT_ONLY:
      return "GAPBIT";
    case NegativeSearchOpType::GAPBIT_PHANTOM:
      return "GAPPHANTOM";
    default: LOG_ERROR("type not supported");
  }
}


std::string BufferTypeToString(BufferType tpe) {
  switch (tpe) {
    case BufferType::OBJBUF:
      return "OBJBUF";
    case NOBUF:
      return "NOBUF";
    case PGBUF:
      return "PGBUF";
    case HYBRBUF:
      return "HYBRBUF";
    default: LOG_ERROR("type not supported");
  }
}

std::string IndexTypeToString(IndexType tpe) {
  switch (tpe) {
    case IndexType::BTREE:
      return "BTREE";
    case IndexType::REMOTE:
      return "REMOTE";
    case IndexType::TWOTREE:
      return "TWOTREE";
    default: LOG_ERROR("type not supported");
  }
}

std::string BoolToString(bool tpe) {
  if (tpe) {
    return "True";
  }
  return "False";
}

std::string RCToString(RC rc) {
  switch (rc) {
    case RC::ABORT:
      return "ABORT";
    case RC::ERROR:
      return "ERROR";
    case RC::OK:
      return "OK";
    case RC::PENDING:
      return "PENDING";
    case RC::NOT_FOUND:
      return "NOT_FOUND";
    default:
      return "UNKNOWN";
  }
}

}