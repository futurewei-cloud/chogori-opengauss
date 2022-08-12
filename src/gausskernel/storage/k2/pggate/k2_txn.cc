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
#include "k2_txn.h"

// #include "k2_queue_defs.h"
#include "k2_session_metrics.h"

namespace k2pg {
namespace gate {
using namespace k2;

K23SITxn::K23SITxn(std::shared_ptr<sh::TxnHandle> txn, k2::TimePoint startTime):_txn(txn), _startTime(startTime){
    if (!_txn) return;
    K2LOG_D(log::k2Client, "starting txn {} at time: {}", _txn, _startTime);
    _inFlightTxns++;
    session::in_flight_txns->observe(_inFlightTxns);
}

K23SITxn::~K23SITxn() {
    if (!_txn) return;
    K2LOG_D(log::k2Client, "dtor for txn {} started at {}", _txn, _startTime);
}

 skv::http::Response<> K23SITxn::endTxn(bool shouldCommit) {
    _inFlightOps ++;
    session::in_flight_ops->observe(_inFlightOps);
    auto endRequestTime=Clock::now();
    K2LOG_D(log::k2Client, "endtxn: {}", _txn);
    auto result = _txn->endTxn(shouldCommit? sh::dto::EndAction::Commit : sh::dto::EndAction::Abort).get();
    auto now = Clock::now();
    K2LOG_D(log::k2Client, "ended txn {} started at {}", _txn, _startTime);
    _inFlightOps --;
    _inFlightTxns--;

    session::txn_latency->observe(now - _startTime);
    session::txn_end_latency->observe(now - endRequestTime);
    _reportEndMetrics(now);
    return result;
}


 sh::Response<sh::dto::QueryResponse> K23SITxn::scanRead(std::shared_ptr<sh::dto::QueryRequest> query) {
    _scanOps++;
    _inFlightOps++;
    session::in_flight_ops->observe(_inFlightOps);
    K2LOG_D(log::k2Client, "scanread: mtr={}, query={}", _txn, (*query));
    auto result = _txn->query(*query).get();
    _inFlightOps--;
    session::scan_op_latency->observe(Clock::now() - Clock::now());
    return result;
}

sh::Response<sh::dto::SKVRecord> K23SITxn::read(sh::dto::SKVRecord&& record) {
    _readOps++;
    _inFlightOps++;
    K2LOG_D(log::k2Client, "read: txn={}, coll={}, schema-name={}, schema-version={}, key-pk={}, key-rk={}",
            _txn,
            record.collectionName,
            record.schema->name,
            record.schema->version,
            record.getPartitionKey(),
            record.getRangeKey());

    session::in_flight_ops->observe(_inFlightOps);
    auto st = Clock::now();
    auto result = _txn->read(record).get();
    _inFlightOps--;
    session::read_op_latency->observe(Clock::now() - st);
    return result;
}

// TODO(akhan): Uncomment or remove
// sh::Response<sh::dto::SKVRecord> K23SITxn::read(k2::dto::Key key, std::string collectionName) {
//     _readOps++;
//     _inFlightOps++;
//     K2LOG_D(log::k2Client, "read: txn={}, coll={}, key-pk={}, key-rk={}",
//       _txn,
//       collectionName,
//       key.partitionKey,
//       key.rangeKey());
//     // There is no api to read from key directly, build the skv record
//     session::in_flight_ops->observe(_inFlightOps);
//     auto st = Clock::now();
//     result = 
//     _inFlightOps--;
//     session::read_op_latency->observe(Clock::now() - st);

//     return result;
// }

sh::Response<> K23SITxn::write(sh::dto::SKVRecord&& record, bool erase, sh::dto::ExistencePrecondition precondition) {
    _writeOps++;
    _inFlightOps++;
    session::in_flight_ops->observe(_inFlightOps);
    K2LOG_D(log::k2Client,
        "write: mtr={}, erase={}, precondition={}, coll={}, schema-name={}, schema-version={}, key-pk={}, key-rk={}",
        _txn,
        erase,
        precondition,
        record.collectionName,
        record.schema->name,
        record.schema->version,
        record.getPartitionKey(),
        record.getRangeKey());
    
    auto st = Clock::now();
    auto result =  _txn->write(record, erase, precondition).get();
    _inFlightOps--;
    session::write_op_latency->observe(Clock::now() - st);
    return result;
}

sh::Response<> K23SITxn::partialUpdate(sh::dto::SKVRecord&& record,
                                                         std::vector<uint32_t> fieldsForUpdate,
                                                         std::string partitionKey) {
    _writeOps++;
    _inFlightOps++;
    session::in_flight_ops->observe(_inFlightOps);
    K2LOG_D(log::k2Client,
        "partial write: mtr={}, record={}, kkey-pk={}, fieldsForUpdate={}",
        _txn,
        record,
        partitionKey,
        fieldsForUpdate);
    auto st = Clock::now();
    _inFlightOps--;
    session::write_op_latency->observe(Clock::now() - st);
    // TODO(akhan): Implement partial udpate
    // auto result = _txn->partialUpdate(record, fieldsForUpdate, partitionKey).get();
    // return result;

    return sh::Response<>(sh::Statuses::S200_OK);
}

// const k2::dto::K23SI_MTR& K23SITxn::mtr() const {
//     return _mtr;
// }

void K23SITxn::_reportEndMetrics(sh::TimePoint now) {
    session::txn_latency->observe(now - _startTime);
    session::txn_ops->observe(_readOps + _writeOps + _scanOps);
    session::txn_read_ops->observe(_readOps);
    session::txn_write_ops->observe(_writeOps);
    session::txn_scan_ops->observe(_scanOps);
};

} // ns gate
} // ns k2pg
