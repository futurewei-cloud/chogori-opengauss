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

// This is the include pattern needed for mixing our C++ code with pg code. libintl.h first,
// postgres.h second, then other pg headers, then our headers

#include <libintl.h>
#include "postgres.h"
#include "access/xact.h"
#include "access/k2/status.h"
#include "access/k2/k2pg_aux.h"

#include "session.h"
using namespace std::chrono_literals;  // so that we can type "1ms"

namespace k2pg {

static void reportXactError(std::string&& msg, sh::Status& status) {
    K2PgStatus pg_status{};
    pg_status.pg_code = ERRCODE_FDW_ERROR;
    pg_status.k2_code = status.code;
    pg_status.msg = msg;
    pg_status.detail = std::move(status.message);
    HandleK2PgStatus(pg_status);
}

static void K2XactCallback(XactEvent event, void* arg) {
    if (event == XACT_EVENT_START) {
        elog(DEBUG2, "XACT_EVENT_START");
        if (auto [status] = TXMgr.endTxn(sh::dto::EndAction::Abort).get(); !status.is2xxOK()) {
            reportXactError("TXMgr abort failed", status);
        }

        TXMgr.setSessionTxnOpts(sh::dto::TxnOptions{
                .timeout= Config().getDurationMillis("k2.txn_op_timeout_ms", 1s),
                .priority= static_cast<sh::dto::TxnPriority>(Config().get<uint8_t>("k2.txn_priority", 128)), // 0 is highest, 255 is lowest.
                .syncFinalize = Config().get<bool>("k2.sync_finalize_txn", false)
            });

        if (auto [status] = TXMgr.beginTxn().get(); !status.is2xxOK()) {
            reportXactError("TXMgr begin failed", status);
        }
    } else if (event == XACT_EVENT_COMMIT) {
        elog(DEBUG2, "XACT_EVENT_COMMIT");
        if (auto [status] = TXMgr.endTxn(sh::dto::EndAction::Commit).get(); !status.is2xxOK()) {
            reportXactError("TXMgr commit failed", status);
        }
    } else if (event == XACT_EVENT_ABORT) {
        elog(DEBUG2, "XACT_EVENT_ABORT");
        if (auto [status] = TXMgr.endTxn(sh::dto::EndAction::Abort).get(); !status.is2xxOK()) {
            reportXactError("TXMgr abort failed", status);
        }
    }
}

void TxnManager::_init() {
    if (!_initialized) {
        RegisterXactCallback(K2XactCallback, NULL);
        // TODO
        // We don't really handle nested transactions separately - all ops are just bundled in the parent
        // if we did, register this callback to handle nested txns:
        // RegisterSubXactCallback(K2SubxactCallback, NULL);
        _initialized = true;

        auto clientConfig = _config.sub("client");
        std::string host = clientConfig.get<std::string>("host", "localhost");
        int port = clientConfig.get<int>("port", 30000);
        K2LOG_I(log::k2pg, "Initializing SKVClient with url {}:{}", host, port);
        _client = std::make_shared<sh::Client>(host, port);
    }
}

void TxnManager::setSessionTxnOpts(sh::dto::TxnOptions opts) {
    _init();
    _txnOpts = std::move(opts);
}

boost::future<sh::Response<>> TxnManager::beginTxn() {
    _init();
    auto status = sh::Statuses::S200_OK;
    if (!_txn) {
        K2LOG_D(log::k2pg, "Starting new transaction");
        return _client->beginTxn(_txnOpts)
        .then([this] (auto&& respFut) mutable {
            auto&& [status, handle] = respFut.get();
            if (status.is2xxOK()) {
                K2LOG_D(log::k2pg, "Started new txn: {}", handle);
                _txn = std::make_unique<sh::TxnHandle>(std::move(handle));
            }
            K2LOG_E(log::k2pg, "Unable to begin txn due to: {}", status);
            return sh::Response<>(std::move(status));
        });
    }
    K2LOG_D(log::k2pg, "Found existing txn");
    return sh::MakeResponse<>(std::move(status));
}

boost::future<sh::Response<>> TxnManager::endTxn(sh::dto::EndAction endAction) {
    _init();
    if (_txn) {
        K2LOG_D(log::k2pg, "end txn, with action: {}", endAction)
        return _txn->endTxn(endAction)
            .then([this](auto&& respFut) mutable {
                auto&& [status] = respFut.get();
                _txn.reset();
                return sh::Response<>(std::move(status));
            });
    }
    else {
        K2LOG_W(log::k2pg, "no txn found in endTxn");
    }
    return sh::MakeResponse<>(sh::Statuses::S410_Gone("transaction not found in end"));
}

boost::future<sh::Response<std::shared_ptr<sh::dto::Schema>>>
TxnManager::getSchema(const sh::String& collectionName, const sh::String& schemaName, int64_t schemaVersion) {
    _init();
    K2LOG_D(log::k2pg, "cname: {}, sname: {}, version: {}", collectionName, schemaName, schemaVersion);
    return _client->getSchema(collectionName, schemaName, schemaVersion);
}

boost::future<sh::Response<>>
TxnManager::createSchema(const sh::String& collectionName, const sh::dto::Schema& schema) {
    _init();
    K2LOG_D(log::k2pg, "cname: {}, schema: {}", collectionName, schema);
    return _client->createSchema(collectionName, schema);
}

boost::future<sh::Response<>>
TxnManager::createCollection(sh::dto::CollectionMetadata metadata, std::vector<sh::String> rangeEnds) {
    _init();
    K2LOG_D(log::k2pg, "createCollection: {}, rends: {}", metadata, rangeEnds);
    return _client->createCollection(metadata, rangeEnds);
}

boost::future<sh::Response<>>
TxnManager::createCollection(const std::string& collection_name, const std::string& DBName) {
    _init();
    K2LOG_D(log::k2pg, "Create collection: name={} for database: {}", collection_name, DBName);

    auto cconf = _config.sub("create_collections").sub(DBName);
    std::vector<std::string> rangeEnds = cconf.get<std::vector<std::string>>("range_ends");

    sh::dto::HashScheme scheme = rangeEnds.size() ? sh::dto::HashScheme::Range : sh::dto::HashScheme::HashCRC32C;
    sh::dto::CollectionMetadata metadata{
        .name = collection_name,
        .hashScheme = scheme,
        .storageDriver = sh::dto::StorageDriver::K23SI,
        .capacity{
            .dataCapacityMegaBytes = 0,
            .readIOPs = 0,
            .writeIOPs = 0,
            .minNodes = 1   // K2 Http proxy hangs if minNodes = 0
        },
        .retentionPeriod = cconf.getDurationMillis("retention_period", 1h*24*90),
        .heartbeatDeadline = sh::Duration(0),
        .deleted = false
    };

    return createCollection(metadata, rangeEnds);
}

boost::future<sh::Response<sh::dto::SKVRecord>>
TxnManager::read(sh::dto::SKVRecord& record) {
    K2LOG_D(log::k2pg, "read: {}", record);
    return beginTxn()
        .then([this, &record](auto&& beginFut) mutable {
            auto&& [beginStatus] = beginFut.get();
            if (!beginStatus.is2xxOK()) {
                return sh::MakeResponse<sh::dto::SKVRecord>(std::move(beginStatus), sh::dto::SKVRecord{});
            }
            return _txn->read(record);
        })
        .unwrap();
}

boost::future<sh::Response<>>
TxnManager::write(sh::dto::SKVRecord& record, bool erase,
                  sh::dto::ExistencePrecondition precondition) {
    K2LOG_D(log::k2pg, "write: {}, erase: {}, precond: {}", record, erase, precondition);
    return beginTxn()
        .then([this, &record, erase = erase, precondition = precondition](auto&& beginFut) mutable {
            auto&& [beginStatus] = beginFut.get();
            if (!beginStatus.is2xxOK()) {
                return sh::MakeResponse<>(std::move(beginStatus));
            }
            return _txn->write(record, erase, precondition);
        })
        .unwrap();
}

boost::future<sh::Response<>>
TxnManager::partialUpdate(sh::dto::SKVRecord& record, std::vector<uint32_t> fieldsForPartialUpdate) {
    K2LOG_D(log::k2pg, "partialUpdate: {}, fields: {}", record, fieldsForPartialUpdate);
    return beginTxn()
        .then([this, &record, fields = std::move(fieldsForPartialUpdate)](auto&& beginFut) mutable {
            auto&& [beginStatus] = beginFut.get();
            if (!beginStatus.is2xxOK()) {
                return sh::MakeResponse<>(std::move(beginStatus));
            }
            return _txn->partialUpdate(record, std::move(fields));
        })
        .unwrap();
}

boost::future<sh::Response<sh::dto::QueryResponse>>
TxnManager::query(std::shared_ptr<sh::dto::QueryRequest> query) {
    if (query) {
        K2LOG_D(log::k2pg, "query: {}", *query);
    }
    else {
        K2LOG_E(log::k2pg, "null query");
    }
    return beginTxn()
        .then([this, query = std::move(query)](auto&& beginFut) mutable {
            auto&& [beginStatus] = beginFut.get();
            if (!beginStatus.is2xxOK()) {
                return sh::MakeResponse<sh::dto::QueryResponse>(std::move(beginStatus), sh::dto::QueryResponse{});
            }
            return _txn->query(std::move(query));
        })
        .unwrap();
}

boost::future<sh::Response<std::shared_ptr<sh::dto::QueryRequest>>>
TxnManager::createQuery(sh::dto::SKVRecord& startKey, sh::dto::SKVRecord& endKey,
                        sh::dto::expression::Expression&& filter,
                        std::vector<std::string>&& projection, int32_t recordLimit,
                        bool reverseDirection, bool includeVersionMismatch) {
    K2LOG_D(log::k2pg, "startKey={}, endKey={}, filter={}, projection={}, recordLimit={}, reverseDirection={}, includeVersionMismatch={}",
            startKey, endKey, filter, projection, recordLimit, reverseDirection, includeVersionMismatch);
    return beginTxn()
        .then([this, &startKey, &endKey, filter = std::move(filter),
               projection = std::move(projection), recordLimit, reverseDirection,
               includeVersionMismatch](auto&& beginFut) mutable {
            auto&& [beginStatus] = beginFut.get();
            if (!beginStatus.is2xxOK()) {
                return sh::MakeResponse<std::shared_ptr<sh::dto::QueryRequest>>(std::move(beginStatus), nullptr);
            }
            return _txn->createQuery(startKey, endKey, std::move(filter),
                                     std::move(projection), recordLimit, reverseDirection,
                                     includeVersionMismatch);
        })
        .unwrap();
}

boost::future<sh::Response<>>
TxnManager::destroyQuery(std::shared_ptr<sh::dto::QueryRequest> query) {
    if (query) {
        K2LOG_D(log::k2pg, "query: {}", *query);
    } else {
        K2LOG_E(log::k2pg, "null query");
    }
    return beginTxn()
        .then([this, query] (auto&& beginFut) mutable {
            auto&& [beginStatus] = beginFut.get();
            if (!beginStatus.is2xxOK()) {
                return sh::MakeResponse<>(std::move(beginStatus));
            }
            return _txn->destroyQuery(query);
        })
        .unwrap();
}

} // ns
