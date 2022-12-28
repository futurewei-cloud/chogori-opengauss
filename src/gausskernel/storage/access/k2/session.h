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
#include <skvhttp/client/SKVClient.h>
#include "config.h"
#include "access/k2/pg_session.h"
namespace k2pg {
namespace sh=skv::http;

// The TxnManager class enforces single-txn per thread model. It manages the lifetime of the transaction by
// observing callbacks from PG
class TxnManager {
public:
    // For exceptional cases, you can force the end of a txn if needed here.
    // Note that any further operations (read/write) issued in this thread will open a new txn
    boost::future<sh::Response<>>
        endTxn(sh::dto::EndAction endAction);

    // this method creates a new txn in the session if one does not exist already
    boost::future<sh::Response<>>
        beginTxn();

    // Any of the following operations will open a new txn if one does not exist
    boost::future<sh::Response<sh::dto::SKVRecord>>
        read(sh::dto::SKVRecord record);
    boost::future<sh::Response<>>
        write(sh::dto::SKVRecord record, bool erase=false,
              sh::dto::ExistencePrecondition precondition=sh::dto::ExistencePrecondition::None);
    boost::future<sh::Response<>>
        partialUpdate(sh::dto::SKVRecord record, std::vector<uint32_t> fieldsForPartialUpdate);
    boost::future<sh::Response<sh::dto::QueryResponse>>
        query(std::shared_ptr<sh::dto::QueryRequest> query);
    boost::future<sh::Response<std::shared_ptr<sh::dto::QueryRequest>>>
        createQuery(sh::dto::SKVRecord startKey, sh::dto::SKVRecord endKey,
                    sh::dto::expression::Expression&& filter=sh::dto::expression::Expression{},
                    std::vector<std::string>&& projection=std::vector<std::string>{}, int32_t recordLimit=-1,
                    bool reverseDirection=false, bool includeVersionMismatch=false);
    // Queries are automatically destroyed on txn end, so this is only needed for long running txns
    boost::future<sh::Response<>>
        destroyQuery(std::shared_ptr<sh::dto::QueryRequest> query);
    boost::future<sh::Response<>>
        createSchema(const std::string& collectionName, const sh::dto::Schema& schema);
    boost::future<sh::Response<std::shared_ptr<sh::dto::Schema>>>
        getSchema(const std::string& collectionName, const std::string& schemaName,
                  int64_t schemaVersion=sh::dto::ANY_SCHEMA_VERSION);
    boost::future<sh::Response<>>
        createCollection(sh::dto::CollectionMetadata metadata, std::vector<std::string> rangeEnds);
    boost::future<sh::Response<>>
        createCollection(const std::string& collection_name, const std::string& DBName);

    // use to set the txn options for all new txns in the thread/session
    void setSessionTxnOpts(sh::dto::TxnOptions opts);

    Config& getConfig() { return _config; }

private:
    // Helper used to initialize the skv client and register txn callbacks
    void _init();

    // this txn is managed by this manager.
    std::unique_ptr<sh::TxnHandle> _txn;
    Metric _txnMt;

    std::shared_ptr<sh::Client> _client;

    Config _config;
    bool _initialized{false};
    sh::dto::TxnOptions _txnOpts;
};

// the thread-local TxnManager. It allows access to k2 from any thread in opengauss,
// in particular, non-fdw threads of execution.
// The general execution model is that we can have at most one active transaction per thread.
inline thread_local TxnManager TXMgr;

// thread local session
inline thread_local std::shared_ptr<PgSession> pg_session;
} // ns
