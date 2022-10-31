/*
MIT License

Copyright(c) 2022 Futurewei Cloud

    Permission is hereby granted,
    free of charge, to any person obtaining a copy of this software and associated documentation files(the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and / or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :

    The above copyright notice and this permission notice shall be included in all copies
    or
    substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS",
    WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
    DAMAGES OR OTHER
    LIABILITY,
    WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
*/

#pragma once

// When we mix certain C++ standard lib code and pg code there seems to be a macro conflict that
// will cause compiler errors in libintl.h. Including as the first thing fixes this.
#include <libintl.h>
#include "postgres.h"
#include "access/k2/pg_gate_api.h"
#include "access/k2/k2pg_aux.h"
#include "catalog/pg_type.h"
#include "fmgr/fmgr_comp.h"

#include <skvhttp/dto/SKVRecord.h>
#include <skvhttp/dto/K23SI.h>
#include <skvhttp/common/Status.h>

struct K2PgScanHandle {
    std::string collectionName;
    std::shared_ptr<skv::http::dto::Schema> primarySchema;
    std::shared_ptr<skv::http::dto::Schema> secondarySchema;
    std::shared_ptr<k2pg::PgTableDesc> primaryTable;
    std::shared_ptr<k2pg::PgTableDesc> secondaryTable;
    boost::future<skv::http::Response<skv::http::dto::QueryResponse>> queryReq;
    std::shared_ptr<skv::http::dto::QueryRequest> query;
    std::deque<skv::http::dto::SKVRecord> queryRecords;
    std::deque<boost::future<skv::http::Response<skv::http::dto::SKVRecord>>> readReqs;
    K2PgSelectIndexParams indexParams;
    uint32_t maxParallelReads = 5;
    bool queryInFlight = false;
};

namespace k2pg {
namespace gate {
    constexpr int K2_FIELD_OFFSET = 2;

    K2PgStatus populateDatumsFromSKVRecord(skv::http::dto::SKVRecord& record, std::shared_ptr<k2pg::PgTableDesc> pg_table,
                                           int nattrs, Datum* values, bool* isnulls, K2PgSysColumns* syscols);

    skv::http::dto::SKVRecord makePrimaryKeyFromSecondary(skv::http::dto::SKVRecord& secondary, std::shared_ptr<k2pg::PgTableDesc> secondaryTable,
                                                          std::shared_ptr<skv::http::dto::Schema> primarySchema);

    void BuildRangeRecords(skv::http::dto::expression::Expression& range_conds, std::vector<skv::http::dto::expression::Expression>& leftover_exprs,
                           skv::http::dto::SKVRecordBuilder& start, skv::http::dto::SKVRecordBuilder& end, bool& isRangeScan);

    skv::http::dto::expression::Value serializePGConstToValue(const K2PgConstant& constant);

    skv::http::dto::expression::Expression buildScanExpr(K2PgScanHandle* scan, const K2PgConstraintDef& constraint, const std::unordered_map<int, uint32_t>& attr_to_offset);

    void serializePGConstToK2SKV(skv::http::dto::SKVRecordBuilder& builder, K2PgConstant constant);

    K2PgStatus getSKVBuilder(K2PgOid database_oid, K2PgOid table_oid, std::unique_ptr<skv::http::dto::SKVRecordBuilder>& builder);

    // Helper function to serialize all attrs into the passed in SKV builder
    K2PgStatus serializePgAttributesToSKV(skv::http::dto::SKVRecordBuilder& builder,  uint32_t table_id, uint32_t index_id,
                                          const std::vector<K2PgAttributeDef>& attrs, const std::unordered_map<int, uint32_t>& attr_num_to_index);

    // Serialize a full SKVRecord from the pg columns, the passed in SKVRecord record is an out parameter
    K2PgStatus makeSKVRecordFromK2PgAttributes(K2PgOid database_oid, K2PgOid table_oid,
                                               const std::vector<K2PgAttributeDef>& columns,
                                               skv::http::dto::SKVRecord& record, std::shared_ptr<k2pg::PgTableDesc> pg_table);

    // builder is an out parameter that is serialized with just the key fields for the record. columns may or may not contain a virtual tupleID attribute,
    // and columns may or may not contain attributes that are not part of the key fields
    K2PgStatus makeSKVBuilderWithKeysSerialized(K2PgOid database_oid, K2PgOid table_oid,
                                               const std::vector<K2PgAttributeDef>& columns,
                                               std::unique_ptr<skv::http::dto::SKVRecordBuilder>& builder);

    // Helper function to deserialize a tupleID datum back into a SKVRecord
    skv::http::dto::SKVRecord tupleIDDatumToSKVRecord(Datum tuple_id, std::string collection, std::shared_ptr<skv::http::dto::Schema> schema);

    // Helper function to take a source SKVRecord and serialize its key fields into a SKVRecordBuilder.
    // Meant for use with SKVRecords materialized from tupleID datums
    K2PgStatus serializeKeysFromSKVRecord(skv::http::dto::SKVRecord& source, skv::http::dto::SKVRecordBuilder& builder);
} // k2pg ns
} // gate ns
