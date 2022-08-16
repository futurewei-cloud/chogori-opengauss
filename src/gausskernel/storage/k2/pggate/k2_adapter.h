// Copyright(c) 2020 Futurewei Cloud
//
// Permission is hereby granted,
//        free of charge, to any person obtaining a copy of this software and associated documentation files(the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and / or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :
//
// The above copyright notice and this permission notice shall be included in all copies
// or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS",
// WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE
//        AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
//        DAMAGES OR OTHER LIABILITY,
// WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
//

#pragma once
#include <boost/function.hpp>

#include "../common/status.h"
#include "../entities/schema.h"
// #include "../entities/expr.h"
#include "k2_config.h"
#include "k2_gate.h"
#include "k2_includes.h"
#include "k2_log.h"
#include "k2_thread_pool.h"
#include "k2_txn.h"
#include "pg_env.h"
// #include "pg_op_api.h"
#include "../common/result.h"
#include "../session.h"

namespace k2pg {
namespace gate {

using k2pg::Status;
// using k2pg::sql::PgExpr;
// using k2pg::sql::PgConstant;
// using k2pg::sql::PgOperator;

using k2fdw::TXMgr;
namespace sh=skv::http;

// An adapter between SQL/Connector layer operations and K2 SKV storage, designed to be the ONLY interface in between.
// It contains 5 sub-groups of APIs
//  1) SKV Schema APIs (CRUD of Collection, Schema, similar to DDL)
//  2) K2-3SI transaction APIs (Begin, Commit, Abort)
//  3) Transactional SKV Record/data API (CRUD, QueryScan, etc, similar to DML)
//  4) (static) Utility functions, e.g. K2 type conversion to PG types
//  5) K2Adapter selcf-managment APIs, e.g. ctor, dtor, Init() etc.
class K2Adapter {
public:
  // 1/5 SKV Schema and collection APIs (CPO opera// tions)
    sh::Response<> CreateSchema(const std::string& collectionName, std::shared_ptr<sh::dto::Schema> schema)
    { return TXMgr.CreateSchema(collectionName, *schema.get()); };

    sh::Response<std::shared_ptr<sh::dto::Schema>> GetSchema(const std::string& collectionName, const std::string& schemaName, uint64_t schemaVersion=sh::dto::ANY_SCHEMA_VERSION)
    { return TXMgr.GetSchema(collectionName, schemaName, schemaVersion); }

    sh::Response<> CreateCollection(const std::string& collection_name, const std::string& DBName);
    sh::Response<> DropCollection(const std::string& collection_name);
//     {
// // return k23si_->dropCollection(collection_name);
        
//     }

  // 2/5 K2-3SI transaction APIs
  K23SITxn BeginTransaction();
  // Async EndTransaction shouldCommit - true to commit the transaction, false to abort the transaction
    sh::Response<> EndTransaction(std::shared_ptr<K23SITxn> k23SITxn, bool shouldCommit) { return k23SITxn->endTxn(shouldCommit);}

  // 3/5 SKV data APIs
  // Read a record based on recordKey(which will be consumed/moved).
  sh::Response<sh::dto::SKVRecord> ReadRecord(std::shared_ptr<K23SITxn> k23SITxn, sh::dto::SKVRecord& recordKey)
    { return k23SITxn->read(std::move(recordKey)); }
  // param record will be consumed/moved
    sh::Response<> UpsertRecord(std::shared_ptr<K23SITxn> k23SITxn, sh::dto::SKVRecord& record)
      { return WriteRecord(k23SITxn, record, false/*isDelete*/); }
  // param record will be consumed/moved
    sh::Response<> DeleteRecord(std::shared_ptr<K23SITxn> k23SITxn, sh::dto::SKVRecord& record)
    { return WriteRecord(k23SITxn, record, true/*isDelete*/); }

    skv::http::Response<skv::http::dto::QueryRequest> CreateScanRead(std::shared_ptr<K23SITxn> k23SITxn, const sh::String& collectionName, const sh::String& schemaName, sh::dto::SKVRecord& startKey, sh::dto::SKVRecord& endKey, sh::dto::expression::Expression&& filter=sh::dto::expression::Expression{},
    std::vector<sh::String>&& projection=std::vector<sh::String>{}, int32_t recordLimit=-1, bool reverseDirection=false, bool includeVersionMismatch=false);
    
    sh::Response<sh::dto::QueryResponse> ScanRead(std::shared_ptr<K23SITxn> k23SITxn, sh::dto::QueryRequest query)
    { return k23SITxn->scanRead(query); }

 static std::string GetRowIdFromReadRecord(sh::dto::SKVRecord& record);

  // static void SerializeValueToSKVRecord(const SqlValue& value, sh::dto::SKVRecordBuilder& builder);
  static Status K2StatusToK2PgStatus(const sh::Status& status);
  // static SqlOpResponse::RequestStatus K2StatusToPGStatus(const sh::Status& status);

  // We have two implicit fields (tableID and indexID) in the SKV, so this is the offset to get a user field
  static constexpr uint32_t SKV_FIELD_OFFSET = 2;

  // 5/5 Self managment APIs
  // TODO make thead pool size configurable and investigate best number of threads
  K2Adapter():threadPool_(conf_.get("thread_pool_size", 2)) {
    k23si_ = std::make_shared<K23SIGate>();
  };

  CHECKED_STATUS Init();
  CHECKED_STATUS Shutdown();


  private:

    std::shared_ptr<K23SIGate> k23si_;
    Config conf_;

    ThreadPool threadPool_;

  // will consume/move record param
    sh::Response<> WriteRecord(std::shared_ptr<K23SITxn> k23SITxn, sh::dto::SKVRecord& record, bool isDelete)
    { return k23SITxn->write(std::move(record), isDelete); }

  // CBFuture<Status> handleReadOp(std::shared_ptr<K23SITxn> k23SITxn, std::shared_ptr<PgReadOpTemplate> op);
  // CBFuture<Status> handleWriteOp(std::shared_ptr<K23SITxn> k23SITxn, std::shared_ptr<PgWriteOpTemplate> op);

    // Status HandleRangeConditions(PgExpr *range_conds, std::vector<PgExpr *>& leftover_exprs, std::shared_ptr<sh::dto::Schema> schema, sh::dto::SKVRecordBuilder& start, sh::dto::SKVRecordBuilder& end);

  // // Helper funcxtion for handleReadOp when k2pgctid is set in the request
  // void handleReadByRowIds(std::shared_ptr<K23SITxn> k23SITxn,
  //                          std::shared_ptr<PgReadOpTemplate> op,
  //                          std::shared_ptr<std::promise<Status>> prom);
     std::tuple<Status, std::shared_ptr<sh::dto::Schema>> getSchema(const sh::String& collectionName, const sh::String& schemaName, uint64_t schemaVersion);

  template <class T> // Works with SqlOpWriteRequest and SqlOpReadRequest types
  std::pair<sh::dto::SKVRecordBuilder, Status> MakeSKVRecordWithKeysSerialized(T& request, std::shared_ptr<sh::dto::Schema> schema, bool existYbctids, bool ignoreK2PGTID=false);

  // Sorts values by field index, serializes values into SKVRecord, and returns skv indexes of written fields
  // std::vector<uint32_t> SerializeSKVValueFields(sh::dto::SKVRecordBuilder& record, std::shared_ptr<sh::dto::Schema> schema,
  //                                               std::vector<std::shared_ptr<BindVariable>>& values);

  // static std::string K2PGTIDToString(std::shared_ptr<BindVariable> k2pgctid_column_value);
  static std::string SerializeSKVRecordToString(sh::dto::SKVRecord& record);
  // static sh::dto::SKVRecord K2PGTIDToRecord(const std::string& collection, std::shared_ptr<sh::dto::Schema> schema, std::shared_ptr<BindVariable> k2pgctid_column_value);

  // Column ID of the virtual column which is not stored in k2 data
  static constexpr int32_t VIRTUAL_COLUMN = -8;
};

}  // namespace gate
}  // namespace k2pg
