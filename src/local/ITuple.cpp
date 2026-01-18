#include "ITuple.h"

namespace arboretum {

void ITuple::SetData(char *src, size_t sz) {
  memcpy(GetData(), src, sz);
}

void ITuple::SetData(char * src, size_t sz, BufferType btype) {
  memcpy(GetData(btype), src, sz);
}

void ITuple::GetField(ISchema * schema, OID col, SearchKey &key) {
  auto val = &(GetData()[schema->GetFieldOffset(col)]);
  switch (schema->GetFieldType(col)) {
    case DataType::BIGINT:
      key.SetValue(*((int64_t *) val), DataType::BIGINT);
      break;
    case DataType::INTEGER:
      key.SetValue(*((int *) val), DataType::INTEGER);
      break;
    default:
      LOG_ERROR("Not support FLOAT8 as search key yet");
  }
}

void ITuple::SetField(ISchema * schema, OID col, int64_t value) {
  schema->SetNumericField(col, GetData(), value);
}

void ITuple::SetField(ISchema * schema, OID col, std::string value) {
  schema->SetCharField(col, GetData(), value.c_str(), value.size());
}

void ITuple::GetPrimaryKey(ISchema* schema, SearchKey& key) {
  if (IsTombstone() || IsPhantom()) {
    key = SearchKey(GetTID());
  } else {
    // TODO: for now assume non-composite pkeys
    auto col = schema->GetPKeyColIds()[0];
    GetField(schema, col, key);
  }
}

// Bit masks
// --- Reference Bit Helpers ---
// Set REF_BIT (returns true if it was 0 -> 1)
bool ITuple::SetRefBit() {
  return SetMeta(REF_BIT);
}

// Clear REF_BIT (returns true if it was 1 -> 0)
bool ITuple::UnsetRefBit() {
  return UnsetMeta(REF_BIT);
}

// --- Dirty Bit Helpers ---

// Set DIRTY_BIT (returns true if it was 0 -> 1)
bool ITuple::SetDirty() {
  return SetMeta(DIRTY_BIT);
}

// Clear DIRTY_BIT (returns true if it was 1 -> 0)
bool ITuple::UnsetDirty() {
    return UnsetMeta(DIRTY_BIT);
}


bool ITuple::SetGapBit() {
  return SetMeta(GAP_BIT);
}

bool ITuple::UnsetGapBit() {
  return UnsetMeta(GAP_BIT);
}

bool ITuple::SetTombstone() {
  return SetMeta(TOMBSTONE_BIT);
}

bool ITuple::UnsetTombstone() {
    return UnsetMeta(TOMBSTONE_BIT);
}


bool ITuple::SetPhantom() {
  return SetMeta(PHANTOM_BIT);
}

bool ITuple::UnsetPhantom() {
    return UnsetMeta(PHANTOM_BIT);
}



bool ITuple::IsRefBitSet() {
   return IsMetaSet(REF_BIT); 
}

bool ITuple::IsDirty() {
  return IsMetaSet(DIRTY_BIT);
}

bool ITuple::IsTombstone() {
  return IsMetaSet(TOMBSTONE_BIT);
}

bool ITuple::IsPhantom() {
  return IsMetaSet(PHANTOM_BIT);
}

bool ITuple::IsGapBitSet() {
  return IsMetaSet(GAP_BIT);
}

bool ITuple::IsNegativeGap() {
  return !IsMetaSet(GAP_BIT);
}

bool ITuple::IsMetaSet(uint8_t bitmask) {
  uint8_t val = tuple_meta_.load(std::memory_order_acquire);
  return (val & bitmask) != 0;
}

bool ITuple::SetMeta(uint8_t bitmask) {
  uint8_t old_val = tuple_meta_.fetch_or(bitmask, std::memory_order_acq_rel);
  return (old_val & bitmask) == 0;
}

bool ITuple::UnsetMeta(uint8_t bitmask) {
  uint8_t old_val = tuple_meta_.fetch_and(~bitmask, std::memory_order_acq_rel);
  return (old_val & bitmask) != 0;
}

}