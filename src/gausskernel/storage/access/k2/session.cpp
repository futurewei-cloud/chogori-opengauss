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

namespace k2pg {

#define TXNFMT(txn) (txn ? "null" : fmt::format("{}", *txn).c_str())

static void reportXactError(std::string&& msg, sh::Status& status) {
    K2PgStatus pg_status{};
    pg_status.pg_code = ERRCODE_FDW_ERROR;
    pg_status.k2_code = status.code;
    pg_status.msg = msg;
    pg_status.detail = std::move(status.message);
    HandleK2PgStatus(pg_status);
}
    
static void K2XactCallback(XactEvent event, void* arg)
{
    auto currentTxn = TXMgr.GetTxn();
   
    elog(DEBUG2, "xact_callback event %u, txn %s", event, TXNFMT(currentTxn));

    if (event == XACT_EVENT_START) {
        elog(DEBUG2, "XACT_EVENT_START, txn %s", TXNFMT(currentTxn));
        if (currentTxn) {
            auto [status] = TXMgr.EndTxn(sh::dto::EndAction::Abort);
            if (!status.is2xxOK()) {
                reportXactError("TXMgr abort failed", status);
            }
        }
        auto [status, txh] = TXMgr.BeginTxn(sh::dto::TxnOptions{
                .timeout= Config().getDurationMillis("k2.txn_op_timeout_ms", 1s),
                .priority= static_cast<sh::dto::TxnPriority>(Config().get<uint8_t>("k2.txn_priority", 128)), // 0 is highest, 255 is lowest.
                .syncFinalize = Config().get<bool>("k2.sync_finalize_txn", false)
            });
        if (!status.is2xxOK()) {
            reportXactError("TXMgr begin failed", status);
        }
    } else if (event == XACT_EVENT_COMMIT) {
        elog(DEBUG2, "XACT_EVENT_COMMIT, txn %s", TXNFMT(currentTxn));
        auto [status] = TXMgr.EndTxn(sh::dto::EndAction::Commit);
        if (!status.is2xxOK()) {
            reportXactError("TXMgr commit failed", status);
        }
    } else if (event == XACT_EVENT_ABORT) {
        elog(DEBUG2, "XACT_EVENT_ABORT, txn %s", TXNFMT(currentTxn));

        auto [status] = TXMgr.EndTxn(sh::dto::EndAction::Abort);
        if (!status.is2xxOK()) {
            reportXactError("TXMgr abort failed", status);
        }
    }
}

void TxnManager::Init() {
    if (!_initialized) {
        RegisterXactCallback(K2XactCallback, NULL);
        // We don't really handle nested transactions separately - all ops are just bundled in the parent
        // if we did, register this callback to handle nested txns:
        // RegisterSubXactCallback(K2SubxactCallback, NULL);
        _initialized = true;
    }
    
    if (!_client) {
        // TODO add client config here (e.g. proxy url/port, etc)
        _client = std::make_shared<sh::Client>();
        K2LOG_I(log::k2pg, "Initializing SKVClient");
    }
}

std::shared_ptr<sh::TxnHandle> TxnManager::GetTxn() {
    return _txn;
}

sh::Response<std::shared_ptr<sh::TxnHandle>> TxnManager::BeginTxn(sh::dto::TxnOptions opts) {
    Init();
    auto status = sh::Statuses::S200_OK;
    if (!_txn) {
        K2LOG_D(log::k2pg, "Starting new transaction");
        auto resp = _client->beginTxn(std::move(opts)).get();
        auto& [status, handle] = resp;
        if (status.is2xxOK()) {
            K2LOG_D(log::k2pg, "Started new txn: {}", handle);
            _txn = std::make_shared<sh::TxnHandle>(std::move(handle));
        }
        else {
            K2LOG_E(log::k2pg, "Unable to begin txn due to: {}", status);
        }
    }
    return sh::Response<std::shared_ptr<sh::TxnHandle>>(std::move(status), _txn);
}

sh::Response<> TxnManager::EndTxn(sh::dto::EndAction endAction) {
    auto status = sh::Statuses::S410_Gone("transaction not found in end");
    if (_txn) {
        status = std::get<0>(_txn->endTxn(endAction).get());
    }

    _txn.reset();
    return sh::Response<>(std::move(status));
}

} // ns
