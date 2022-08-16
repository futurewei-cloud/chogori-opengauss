/*
MIT License

Copyright(c) 2022 Futurewei Cloud

    Permission is hereby granted,stx
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

#include "session.h"

namespace k2fdw {

void TxnManager::_Init() {
    if (!_client) {
        // TODO add client config here (e.g. proxy url/port, etc)
        _client = std::make_shared<sh::Client>();
        K2LOG_I(log::k2fdw, "Initializing SKVClient");
    }
}

std::shared_ptr<sh::TxnHandle> TxnManager::GetTxn() {
    return _txn;
}

sh::Response<std::shared_ptr<sh::TxnHandle>> TxnManager::BeginTxn(sh::dto::TxnOptions opts) {
    _Init();
    auto status = sh::Statuses::S200_OK;
    if (!_txn) {
        K2LOG_D(log::k2fdw, "Starting new transaction");
        auto resp = _client->beginTxn(std::move(opts)).get();
        auto& [status, handle] = resp;
        if (status.is2xxOK()) {
            K2LOG_D(log::k2fdw, "Started new txn: {}", handle);
            _txn = std::make_shared<sh::TxnHandle>(std::move(handle));
        }
        else {
            K2LOG_E(log::k2fdw, "Unable to begin txn due to: {}", status);
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

sh::Response<std::shared_ptr<sh::dto::Schema>> TxnManager::GetSchema(const sh::String& collectionName, const sh::String& schemaName, int64_t schemaVersion) {
    return _client->getSchema(collectionName, schemaName, schemaVersion).get();
}

sh::Response<> TxnManager::CreateCollection(sh::dto::CollectionMetadata metadata, std::vector<sh::String> rangeEnds) {
    return _client->createCollection(metadata, rangeEnds).get();
}

sh::Response<> TxnManager::CreateSchema(const sh::String& collectionName, const sh::dto::Schema& schema) {
    return _client->createSchema(collectionName, schema).get();
}

} // ns
