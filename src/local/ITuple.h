#ifndef ARBORETUM_SRC_LOCAL_ITUPLE_H_
#define ARBORETUM_SRC_LOCAL_ITUPLE_H_

#include "common/Common.h"
#include "ISchema.h"

namespace arboretum {

// metadata manipulation (ref bit/dirty bit/gap bit/tombstone bit/phantom bit)
// Bit masks for tuple metadata
static constexpr uint8_t REF_BIT   = 0b00000001; // Bit 0: Reference Bit
static constexpr uint8_t DIRTY_BIT = 0b00000010; // Bit 1: Dirty Bit
static constexpr uint8_t GAP_BIT = 0b00000100; // Bit 2: GAP Bit
static constexpr uint8_t TOMBSTONE_BIT = 0b00001000; // Bit 3: tombstone Bit
static constexpr uint8_t PHANTOM_BIT = 0b00010000; // Bit 4: Phantom Bit


class ITuple {

 public:
  ITuple() = default;;
  explicit ITuple(OID tbl_id, OID tid) : tbl_id_(tbl_id), tid_(tid) {};
  void SetFromLoaded(char * loaded_data, size_t sz) { SetData(loaded_data, sz); };
  void SetData(char * src, size_t sz);
  void SetData(char * src, size_t sz, BufferType btype);
  char * GetData() {
    if (g_buf_type == PGBUF) {
      // XXX: pg data in azure does not have the two newly added fields
      return (char *) this + sizeof(ITuple) - sizeof(std::atomic<uint32_t>) - sizeof(bool);
    }
    return (char *) this + sizeof(ITuple);
  };

  char * GetData(BufferType btype) {
    if (btype == PGBUF) {
      // XXX: pg data in azure does not have the two newly added fields
      return (char *) this + sizeof(ITuple) - sizeof(std::atomic<uint32_t>) - sizeof(bool);
    }
    return (char *) this + sizeof(ITuple);
  };

  void SetTID(OID tid) { tid_ = tid; };
  OID GetTID() const { return tid_; };
  OID GetTableId() const { return tbl_id_; };
  void GetField(ISchema * schema, OID col, SearchKey &key);
  void SetField(ISchema * schema, OID col, std::string value);
  void SetField(ISchema * schema, OID col, int64_t value);
  void GetPrimaryKey(ISchema * schema, SearchKey &key);
  SearchKey GetPrimaryKey(ISchema * schema) {
    SearchKey key;
    GetPrimaryKey(schema, key);
    return key;
  }


  // metadata manipulation (ref bit/dirty bit/phantom bits)
  bool SetRefBit();
  bool UnsetRefBit();
  bool SetDirty();
  bool UnsetDirty();
  bool SetGapBit();
  bool UnsetGapBit();
  bool SetTombstone();
  bool UnsetTombstone(); 
  bool SetPhantom();
  bool UnsetPhantom();
  bool SetMeta(uint8_t bitmask);
  bool UnsetMeta(uint8_t bitmask);


  bool IsRefBitSet();
  bool IsDirty(); 
  bool IsTombstone();
  bool IsPhantom();
  bool IsGapBitSet(); 
  bool IsNegativeGap();
  bool IsMetaSet(uint8_t bitmask);

  // buffer helper
  std::atomic<uint32_t> buf_state_{0};

  // metadata manipulation (ref bit/dirty bit/gap bit/tombstone bit/phantom bit)
  std::atomic<uint8_t> tuple_meta_{GAP_BIT};

  // should not be counted in the the size of the tuple, it is only used for a evcition baseline comparison
  ITuple * clock_next_{nullptr};
  std::mutex buf_latch_{};

 private:
  OID tbl_id_{0};
 public:
  OID tid_{0}; // identifier used in the lock table

  // a hack for the next key lock, not counted in the size of the tuple
  // should use a separate lock table instead of embedding the lock in the tuple
  std::atomic<uint32_t> next_key_lock_{0};

  // remove this field, it is already encoded in tuple_meta_
  bool next_key_in_storage_{false};
};

}

#endif //ARBORETUM_SRC_LOCAL_ITUPLE_H_
