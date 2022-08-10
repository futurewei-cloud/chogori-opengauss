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

#include "pggate/pg_txn_handler.h"

namespace k2pg {
namespace gate {

using k2pg::Status;
using k2fdw::TxMgr;

Status K2StatusToK2PgStatus(const skv::http::Status& status) {
    // TODO verify this translation with how the upper layers use the Status,
    // especially the Aborted status
    switch (status.code) {
        case 200: // OK Codes
        case 201:
        case 202:
            return Status();
        case 400: // Bad request
            return STATUS(InvalidCommand, status.message.c_str());
        case 403: // Forbidden, used to indicate AbortRequestTooOld in K23SI
            return STATUS(Aborted, status.message.c_str());
        case 404: // Not found
            return STATUS(NotFound, status.message.c_str());
        case 405: // Not allowed, indicates a bug in K2 usage or operation
        case 406: // Not acceptable, used to indicate BadFilterExpression
            return STATUS(InvalidArgument, status.message.c_str());
        case 408: // Timeout
            return STATUS(TimedOut, status.message.c_str());
        case 409: // Conflict, used to indicate K23SI transaction conflicts
            return STATUS(Aborted, status.message.c_str());
        case 410: // Gone, indicates a partition map error
            return STATUS(ServiceUnavailable, status.message.c_str());
        case 412: // Precondition failed, indicates a failed K2 insert operation
            return STATUS(AlreadyPresent, status.message.c_str());
        case 422: // Unprocessable entity, BadParameter in K23SI, indicates a bug in usage or operation
            return STATUS(InvalidArgument, status.message.c_str());
        case 500: // Internal error, indicates a bug in K2 code
            return STATUS(Corruption, status.message.c_str());
        case 503: // Service unavailable, indicates a partition is not assigned
            return STATUS(ServiceUnavailable, status.message.c_str());
        default:
            return STATUS(Corruption, "Unknown K2 status code");
    }
}

RequestStatus K2Adapter::K2StatusToPGStatus(const skv::http::Status& status) {
    switch (status.code) {
        case 200: // OK Codes
        case 201:
        case 202:
            return SqlOpResponse::RequestStatus::PGSQL_STATUS_OK;
        case 400: // Bad request
            return SqlOpResponse::RequestStatus::PGSQL_STATUS_USAGE_ERROR;
        case 403: // Forbidden, used to indicate AbortRequestTooOld in K23SI
            return SqlOpResponse::RequestStatus::PGSQL_STATUS_RESTART_REQUIRED_ERROR;
        case 404: // Not found
            return SqlOpResponse::RequestStatus::PGSQL_STATUS_OK; // TODO: correct?
        case 405: // Not allowed, indicates a bug in K2 usage or operation
        case 406: // Not acceptable, used to indicate BadFilterExpression
            return SqlOpResponse::RequestStatus::PGSQL_STATUS_USAGE_ERROR;
        case 408: // Timeout
            return SqlOpResponse::RequestStatus::PGSQL_STATUS_RUNTIME_ERROR;
        case 409: // Conflict, used to indicate K23SI transaction conflicts
            return SqlOpResponse::RequestStatus::PGSQL_STATUS_RESTART_REQUIRED_ERROR;
        case 410: // Gone, indicates a partition map error
            return SqlOpResponse::RequestStatus::PGSQL_STATUS_RUNTIME_ERROR;
        case 412: // Precondition failed, indicates a failed K2 insert operation
            return SqlOpResponse::RequestStatus::PGSQL_STATUS_DUPLICATE_KEY_ERROR;
        case 422: // Unprocessable entity, BadParameter in K23SI, indicates a bug in usage or operation
            return SqlOpResponse::RequestStatus::PGSQL_STATUS_USAGE_ERROR;
        case 500: // Internal error, indicates a bug in K2 code
            return SqlOpResponse::RequestStatus::PGSQL_STATUS_RUNTIME_ERROR;
        case 503: // Service unavailable, indicates a partition is not assigned
            return SqlOpResponse::RequestStatus::PGSQL_STATUS_RUNTIME_ERROR;
        default:
            return SqlOpResponse::RequestStatus::PGSQL_STATUS_RUNTIME_ERROR;
    }
}

PgTxnHandler::PgTxnHandler(): adapter_(adapter) {
}

PgTxnHandler::~PgTxnHandler() {
  // Abort the transaction before the transaction handler gets destroyed.
  if ( TXMgr.HasTxn()) {
    auto status = AbortTransaction();
    if (!status.ok()) {
      K2LOG_E(log::pg, "Transaction abortion failed during destructor due to: {}", status.code());
    }
  }
}

Status PgTxnHandler::BeginTransaction() {
  K2LOG_D(log::pg, "BeginTransaction: txn_in_progress_={}", txn_in_progress_);
  if (txn_in_progress_) {
    return STATUS(IllegalState, "Transaction is already in progress");
  }
  ResetTransaction();
  auto&& result = k2fdw::TXMgr.BeginTxn({});
  auto&& status = result.get<0>();
  if (!status.is2xxOK()) {
      return K2StatusToK2PgStatus(status);
  }
  txn_in_progress_ = true;
  return Status::OK();
}

Status PgTxnHandler::CommitTransaction() {
  if (!txn_in_progress_) {
    K2LOG_D(log::pg, "No transaction in progress, nothing to commit.");
    return Status::OK();
  }

  // if ( k2fdw::TXMgr.GetTxn()!= nullptr && read_only_) {
  //   K2LOG_D(log::pg, "This was a read-only transaction, nothing to commit.");
  //   // currently for K2-3SI transaction, we actually just abort the transaction if it is read only
  //   return AbortTransaction();
  // }

  K2LOG_D(log::pg, "Committing transaction.");
  // Use synchronous call for now until PG supports additional state check after this call
  auto result = TXMgr.EndTxn(skv::http::dto::EndAction.Commit);
  if (!result.status.is2xxOK()) {
    K2LOG_E(log::pg, "Transaction commit failed due to: {}", result.status);
    // status: Not allowed - transaction is also aborted (no need for abort)
    if (result.status == skv::http::Statuses::S410_Gone) {
      txn_already_aborted_ = true;
  } else {
     ResetTransaction();
     K2LOG_D(log::pg, "Transaction commit succeeded");
  }

  return K2StatusToK2PgStatus(result.status);
}

Status PgTxnHandler::AbortTransaction() {
  if (!txn_in_progress_) {
    return Status::OK();
  }

  if (txn_already_aborted_ ) {
    // This was a already commited or read-only transaction, nothing to commit.
    ResetTransaction();
    return Status::OK();
  }

  auto&& [status] = TxMgr.EndTransaction(skv::http::dto::EndAction.Abort);
  // always abandon current transaction and reset regardless abort success or not.
  ResetTransaction();
  if (!status.is2xxOK()) {
    K2LOG_E(log::pg, "Transaction abort failed due to: {}", rstatus);
  }

  return K2Adapter::K2StatusToK2PgStatus(status);
}

Status PgTxnHandler::RestartTransaction() {
  // TODO: how do we decide whether a transaction is restart required?
  if (TxMgr.HasTxn()) {
    auto status = AbortTransaction();
    if (!status.ok()) {
      return status;
    }
  }

  return BeginTransaction();
}

Status PgTxnHandler::SetIsolationLevel(int level) {
  isolation_level_ = static_cast<PgIsolationLevel>(level);
  return Status::OK();
}

Status PgTxnHandler::SetReadOnly(bool read_only) {
  read_only_ = read_only;
  return Status::OK();
}

Status PgTxnHandler::SetDeferrable(bool deferrable) {
  deferrable_ = deferrable;
  return Status::OK();
}

Status PgTxnHandler::EnterSeparateDdlTxnMode() {
  // TODO: do we support this mode and how ?
  return Status::OK();
}

Status PgTxnHandler::ExitSeparateDdlTxnMode(bool success) {
  // TODO: do we support this mode and how ?
  return Status::OK();
}

std::shared_ptr<K23SITxn> PgTxnHandler::GetTxn() {
  // start transaction if not yet started.
  if (!TXMgr.HasTxn()) {
    auto status = BeginTransaction();
    if (!status.ok())
    {
        throw std::runtime_error("Cannot start new transaction.");
    }
  }

  DCHECK(txn_in_progress_);
  return TXMgr.GetTxn();
}

void PgTxnHandler::ResetTransaction() {
  read_only_ = false;
  txn_in_progress_ = false;
  txn_already_aborted_ = false;
  can_restart_.store(true, std::memory_order_release);
}

}  // namespace gate
}  // namespace k2pg
