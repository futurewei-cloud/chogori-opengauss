// Copyright(c) 2021 Futurewei Cloud
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

#include "k2_adapter.h"

#include <cstddef>
#include <unordered_map>

// #include <seastar/core/memory.hh>
// #include <seastar/core/resource.hh>

// #include "pg_gate_defaults.h"
// #include "pg_op_api.h"
// #include "../pggate/pg_gate_typedefs.h"

namespace k2pg {
namespace gate {

using sh::dto::TxnOptions;
// using sql::PgConstant;

Status K2Adapter::Init() {
    K2LOG_I(log::k2Adapter, "Initialize adapter");

    return Status::OK();
}

Status K2Adapter::Shutdown() {
    // TODO: add implementation
    K2LOG_I(log::k2Adapter, "Shutdown adapter");
    return Status::OK();
}

std::string K2Adapter::GetRowIdFromReadRecord(sh::dto::SKVRecord& record) {
    sh::dto::SKVRecord key_record = record.getSKVKeyRecord();
    return SerializeSKVRecordToString(key_record);
}


std::tuple<Status, std::shared_ptr<sh::dto::Schema>>  K2Adapter::getSchema(const sh::String& collectionName, const sh::String& schemaName, uint64_t schemaVersion) {
    auto&& [status, schema] =  k23si_->getSchema(collectionName, schemaName, schemaVersion);
    return std::make_tuple(K2StatusToK2PgStatus(status), schema);
}


sh::Response<> K2Adapter::CreateCollection(const std::string& collection_name, const std::string& DBName)
{
    auto start = sh::Clock::now();
    K2LOG_I(log::k2Adapter, "Create collection: name={} for Database: {}", collection_name, DBName);

    // Working around json conversion to/from sh::String which uses b64
    std::vector<std::string> stdRangeEnds = conf_()["create_collections"][DBName]["range_ends"];
    std::vector<sh::String> rangeEnds;
    for (const std::string& end : stdRangeEnds) {
        rangeEnds.emplace_back(end);
    }

    sh::dto::HashScheme scheme = rangeEnds.size() ? sh::dto::HashScheme::Range : sh::dto::HashScheme::HashCRC32C;

    auto createCollectionReq = sh::dto::CollectionCreateRequest{
        .metadata{
            .name = collection_name,
            .hashScheme = scheme,
            .storageDriver = sh::dto::StorageDriver::K23SI,
            .capacity{
                // TODO: get capacity from config or pass in from param
                //.dataCapacityMegaBytes = 1000,
                //.readIOPs = 100000,
                //.writeIOPs = 100000
            },
            .retentionPeriod = sh::Duration(1h) * 90 * 24  //TODO: get this from config or from param in
        },
        .rangeEnds = std::move(rangeEnds)
    };

//    auto result = k23si_->createCollection(std::move(createCollectionReq));
    // TODO(ahsank): Fix
    auto result = k23si_->createCollection(createCollectionReq.metadata, createCollectionReq.rangeEnds);
    K2LOG_D(log::k2Adapter, "CreateCollection took {}", sh::Clock::now() - start);
    return result;
}

sh::Response<sh::dto::QueryRequest> K2Adapter::CreateScanRead(std::shared_ptr<K23SITxn> k23SITxn, const sh::String& collectionName, const sh::String& schemaName, sh::dto::SKVRecord& startKey, sh::dto::SKVRecord& endKey, sh::dto::expression::Expression&& filter,
    std::vector<sh::String>&& projection, int32_t recordLimit, bool reverseDirection, bool includeVersionMismatch) {
    auto start = sh::Clock::now();
    auto&& result = k23SITxn->createScanRead(collectionName, schemaName, startKey, endKey, std::move(filter), std::move(projection), recordLimit, reverseDirection, includeVersionMismatch);
    K2LOG_V(log::k2Adapter, "CreateScanRead took {}", sh::Clock::now() - start);
    return result;
}


K23SITxn K2Adapter::BeginTransaction() {
    sh::dto::TxnOptions options{};
    // use default values for now
    // TODO: read from configuration/env files
    // Actual partition request deadline is min of this and command line option
    options.timeout = sh::Duration(60000s);
    //options.priority = sh::dto::TxnPriority::Medium;
    auto [status, txn] = k23si_->beginTxn(options);
    if (!status.is2xxOK()) {
        throw std::runtime_error("Failed to begin txn");
    }
     return txn;
}


Status K2Adapter::K2StatusToK2PgStatus(const sh::Status& status) {
    // TODO verify this translation with how the upper layers use the Status,
    // especially the Aborted status
    switch (status.code) {
        case 200: // OK Codes
        case 201:
        case 202:
            return Status();
        case 400: // Bad request
            return STATUS_PG(InvalidCommand, status.message.c_str());
        case 403: // Forbidden, used to indicate AbortRequestTooOld in K23SI
            return STATUS_PG(Aborted, status.message.c_str());
        case 404: // Not found
            return STATUS_PG(NotFound, status.message.c_str());
        case 405: // Not allowed, indicates a bug in K2 usage or operation
        case 406: // Not acceptable, used to indicate BadFilterExpression
            return STATUS_PG(InvalidArgument, status.message.c_str());
        case 408: // Timeout
            return STATUS_PG(TimedOut, status.message.c_str());
        case 409: // Conflict, used to indicate K23SI transaction conflicts
            return STATUS_PG(Aborted, status.message.c_str());
        case 410: // Gone, indicates a partition map error
            return STATUS_PG(ServiceUnavailable, status.message.c_str());
        case 412: // Precondition failed, indicates a failed K2 insert operation
            return STATUS_PG(AlreadyPresent, status.message.c_str());
        case 422: // Unprocessable entity, BadParameter in K23SI, indicates a bug in usage or operation
            return STATUS_PG(InvalidArgument, status.message.c_str());
        case 500: // Internal error, indicates a bug in K2 code
            return STATUS_PG(Corruption, status.message.c_str());
        case 503: // Service unavailable, indicates a partition is not assigned
            return STATUS_PG(ServiceUnavailable, status.message.c_str());
        default:
            return STATUS_PG(Corruption, "Unknown K2 status code");
    }
}

}  // namespace gate
}  // namespace k2pg
