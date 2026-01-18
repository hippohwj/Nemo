#include "YCSBWorkload.h"
#include "common/BenchWorker.h"
#include "ITxn.h"
#include "db/buffer/PageBufferManager.h"
#include "db/buffer/ObjectBufferManager.h"

namespace arboretum {

RC YCSBWorkload::RWTxn(YCSBQuery * query, ITxn *txn) {
  RC rc = RC::OK;
  txn->txn_tpe = TxnType::RW_TXN;
  txn->SetReadOnly(query->read_only);
  for (size_t i = 0; i < query->req_cnt; i++) {
    YCSBRequest & req = query->requests[i];
    char * data;
    size_t size;
    rc = db_->GetTuple(tables_["MAIN_TABLE"], indexes_[0],
                       SearchKey(req.key),data, size, req.ac_type, txn);
    if (rc == ABORT)
      break;
    if (req.ac_type == UPDATE) {
      auto schema = schemas_[0];
      for (size_t cid = 1; cid < schema->GetColumnCnt(); cid++) {
        schema->SetNumericField(cid, data, txn->GetTxnId());
      }
    }
  }
  return rc;
}

// RC YCSBWorkload::InsertTxn(YCSBQuery * query, ITxn *txn) {
//   txn->txn_tpe = TxnType::INSERT_TXN;
//   txn->SetReadOnly(false);
//   auto &req = query->requests[0];
//   char data[schemas_[0]->GetTupleSz()];
//   strcpy(&data[schemas_[0]->GetFieldOffset(1)], "insert");
//   schemas_[0]->SetPrimaryKey((char *) data, req.key);
//   SearchKey pkey(req.key);
//   return db_->InsertTuple(pkey, tables_["MAIN_TABLE"], data,
//                           schemas_[0]->GetTupleSz(), txn);
// }


RC YCSBWorkload::InsertTxn(YCSBQuery* query, ITxn* txn) {
  // use update to similate insert, only for phantom_protection tests
  RC rc = RC::OK;
  txn->txn_tpe = TxnType::RW_TXN;
  YCSBRequest& req = query->requests[0];
  M_ASSERT((g_enable_phantom_protection || g_enable_partition_covering_lock) && (req.ac_type == UPDATE), "insert request ac type must be update");
  char* data;
  size_t size;
  rc = db_->GetTuple(tables_["MAIN_TABLE"], indexes_[0], SearchKey(req.key), data, size, req.ac_type, txn);
  if (rc == ABORT) {
     return rc;
  }
  auto schema = schemas_[0];
  for (size_t cid = 1; cid < schema->GetColumnCnt(); cid++) {
    schema->SetNumericField(cid, data, txn->GetTxnId());
  }
  return rc;
}

RC YCSBWorkload::ScanTxn(YCSBQuery * query, ITxn *txn) {
  txn->txn_tpe = TxnType::SCAN_TXN;
  txn->SetReadOnly(true);
  YCSBRequest & low_req = query->requests[0];
  YCSBRequest & high_req = query->requests[1];
  return db_->GetTuples(tables_["MAIN_TABLE"], indexes_[0],
                        SearchKey(low_req.key),
                        SearchKey(high_req.key),
                        txn);
}

}