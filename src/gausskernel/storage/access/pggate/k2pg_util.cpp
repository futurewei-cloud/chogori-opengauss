// Copyright (c) YugaByte, Inc.
// Portions Copyright (c) 2021 Futurewei Cloud
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied.  See the License for the specific language governing permissions and limitations
// under the License.

#include "access/k2/k2pg_util.h"

#include <stdarg.h>
#include <fstream>
#include <string>
#include <type_traits>
#include <utility>

#if defined(__APPLE__)
#include <mach-o/dyld.h>
#include <sys/sysctl.h>
#else
#include <linux/falloc.h>
#include <sys/sysinfo.h>
#endif  // defined(__APPLE__)

#include "k2pg-internal.h"
#include "access/k2/k2pg_errcodes.h"
#include "status.h"

using k2pg::Status;
using k2pg::K2PgErrorCode;

K2PgStatus K2PgStatusOK() {
  return nullptr;
}

extern "C" {

K2PgStatus K2PgStatus_OK = nullptr;

// Wraps Status object created by K2PgStatus.
class StatusWrapper {
 public:
  explicit StatusWrapper(K2PgStatus s) : status_(s, false) {}

  ~StatusWrapper() {
    status_.DetachStruct();
  }

  Status* operator->() {
    return &status_;
  }

  Status& operator*() {
    return status_;
  }

 private:
  Status status_;
};

bool K2PgStatusIsOK(K2PgStatus s) {
  return StatusWrapper(s)->IsOk();
}

bool K2PgStatusIsNotFound(K2PgStatus s) {
  return StatusWrapper(s)->IsNotFound();
}

bool K2PgStatusIsDuplicateKey(K2PgStatus s) {
  return StatusWrapper(s)->IsAlreadyPresent();
}

uint32_t K2PgStatusPgsqlError(K2PgStatus s) {
/*   StatusWrapper wrapper(s);
  const uint8_t* pg_err_ptr = wrapper->ErrorData(PgsqlErrorTag::kCategory);
  // If we have PgsqlError explicitly set, we decode it
  K2PgErrorCode result = pg_err_ptr != nullptr ? PgsqlErrorTag::Decode(pg_err_ptr)
                                               : K2PgErrorCode::K2PG_INTERNAL_ERROR;
  // If the error is the default generic K2PG_INTERNAL_ERROR (as we also set in AsyncRpc::Failed)
  // then we try to deduce it from a transaction error.
  if (result == K2PgErrorCode::K2PG_INTERNAL_ERROR) {
    const uint8_t* txn_err_ptr = wrapper->ErrorData(TransactionErrorTag::kCategory);
    if (txn_err_ptr != nullptr) {
      switch (TransactionErrorTag::Decode(txn_err_ptr)) {
        case TransactionErrorCode::kAborted: [[fallthrough]];
        case TransactionErrorCode::kReadRestartRequired: [[fallthrough]];
        case TransactionErrorCode::kConflict:
          result = K2PgErrorCode::K2PG_T_R_SERIALIZATION_FAILURE;
          break;
        case TransactionErrorCode::kSnapshotTooOld:
          result = K2PgErrorCode::K2PG_SNAPSHOT_TOO_OLD;
          break;
        case TransactionErrorCode::kNone: [[fallthrough]];
        default:
          result = K2PgErrorCode::K2PG_INTERNAL_ERROR;
      }
    }
  }
*/

// TODO: FIXME
K2PgErrorCode result = K2PgErrorCode::K2PG_INTERNAL_ERROR;
  return static_cast<uint32_t>(result);
}

uint16_t K2PgStatusTransactionError(K2PgStatus s) {
//  const TransactionError txn_err(*StatusWrapper(s));
//  return static_cast<uint16_t>(txn_err.value());
// TODO: FIXME
  return k2pg::to_underlying(k2pg::TransactionErrorCode::kConflict);
}

void K2PgFreeStatus(K2PgStatus s) {
  k2pg::FreeK2PgStatus(s);
}

size_t K2PgStatusMessageLen(K2PgStatus s) {
  return StatusWrapper(s)->message().size();
}

const char* K2PgStatusMessageBegin(K2PgStatus s) {
  return StatusWrapper(s)->message().cdata();
}

const char* K2PgStatusCodeAsCString(K2PgStatus s) {
  return StatusWrapper(s)->CodeAsCString();
}

char* DupK2PgStatusMessage(K2PgStatus status, bool message_only) {
  const char* const code_as_cstring = K2PgStatusCodeAsCString(status);
  const size_t code_strlen = strlen(code_as_cstring);
  const size_t status_len = K2PgStatusMessageLen(status);
  size_t sz = code_strlen + status_len + 3;
  if (message_only) {
    sz -= 2 + code_strlen;
  }
  char* const msg_buf = reinterpret_cast<char*>(k2pg::K2PgPAlloc(sz));
  char* pos = msg_buf;
  if (!message_only) {
    memcpy(msg_buf, code_as_cstring, code_strlen);
    pos += code_strlen;
    *pos++ = ':';
    *pos++ = ' ';
  }
  memcpy(pos, K2PgStatusMessageBegin(status), status_len);
  pos[status_len] = 0;
  return msg_buf;
}

bool K2PgIsRestartReadError(uint16_t txn_errcode) {
  return txn_errcode == static_cast<uint16_t>(k2pg::TransactionErrorCode::kReadRestartRequired);
}

K2PgStatus K2PgInit(const char* argv0,
                  K2PgPAllocFn palloc_fn,
                  K2PgCStringToTextWithLenFn cstring_to_text_with_len_fn) {
  k2pg::K2PgSetPAllocFn(palloc_fn);
  if (cstring_to_text_with_len_fn) {
    k2pg::K2PgSetCStringToTextWithLenFn(cstring_to_text_with_len_fn);
  }
  // TODO: add more logic here
  return K2PgStatusOK();
}

} // extern "C"
