/*
MIT License

Copyright(c) 2020 Futurewei Cloud

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
#include "k2_gate.h"

#include "k2_config.h"
// #include "k2_queue_defs.h"
#include "k2_session_metrics.h"
#include "../session.h"

namespace k2pg {
namespace gate {
namespace sh=skv::http;
using namespace k2;
using k2fdw::TXMgr;


K23SIGate::K23SIGate() {
    Config conf;
    _syncFinalize = conf()["force_sync_finalize"];
}

sh::Response<K23SITxn> K23SIGate::beginTxn(const sh::dto::TxnOptions& txnOpts) {
    sh::dto::TxnOptions opts;
    opts.syncFinalize = _syncFinalize;
    auto [status, txn] = TXMgr.BeginTxn(opts);
    auto start = k2::Clock::now();
    if (!status.is2xxOK())
        return sh::Response<K23SITxn>(status, K23SITxn(nullptr, start));
    else
        return sh::Response<K23SITxn>(status, K23SITxn(txn, start)); 
}

sh::Response<std::shared_ptr<sh::dto::Schema>> K23SIGate::getSchema(const sh::String& collectionName, const sh::String& schemaName, uint64_t schemaVersion) {
    return TXMgr.GetSchema(collectionName, schemaName, schemaVersion);
}

sh::Response<> K23SIGate::createSchema(const sh::String& collectionName, sh::dto::Schema& schema) {
    K2LOG_D(log::k2Client, "create schema: collname={}, schema={}, raw={}", collectionName, schema.name, schema);
    return TXMgr.CreateSchema(collectionName, schema);
}

sh::Response<> K23SIGate::createCollection(const sh::dto::CollectionMetadata& metadata, const std::vector<sh::String>& rangeEnds)
{
    K2LOG_D(log::k2Client, "create collection: cname={}", metadata.name);
    return TXMgr.CreateCollection(metadata, rangeEnds);
}

sh::Response<> K23SIGate::dropCollection(const sh::String& collectionName)
{   K2LOG_D(log::k2Client, "drop collection: cname={}", collectionName);
    // TODO: Implement in http proxy
    return sh::Response<>(sh::Statuses::S200_OK);
}


} // ns gate
} // ns k2pg
