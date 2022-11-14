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

#include <string>
#include <unordered_map>

#include "utils/errcodes.h"

#include "access/k2/pg_memctx.h"

namespace k2pg {

PgMemctx::PgMemctx() {
}

PgMemctx::~PgMemctx() {
    _Clear();
}

namespace {
  // Table of memory contexts.
  // - Although defined in K2Sql, this table is owned and managed by Postgres process.
  //   Other processes cannot control "PgMemctx" to avoid memory violations.
  // - Table "postgres_process_memctxs" is to help releasing the references to PgStatement when
  //   Postgres Process (a C program) is exiting.
  // - Transaction layer and Postgres BOTH hold references to PgStatement objects, and those
  //   PgGate objects wouldn't be destroyed unless both layers release their references or both
  //   layers are terminated.
  std::unordered_map<PgMemctx *, PgMemctx::SharedPtr> postgres_process_memctxs;
} // namespace

PgMemctx *PgMemctx::Create() {
  auto memctx = std::make_shared<PgMemctx>();
  postgres_process_memctxs[memctx.get()] = memctx;
  return memctx.get();
}

Status PgMemctx::Destroy(PgMemctx *handle) {
  if (handle) {
    if(postgres_process_memctxs.find(handle) == postgres_process_memctxs.end()) {
        Status status {
            .pg_code = ERRCODE_INTERNAL_ERROR,
            .k2_code = 500,
            .msg = "Invalid memory context handle",
            .detail = ""
        };
        return status;
    }

    postgres_process_memctxs.erase(handle);
  }

  return Status::OK;
}

Status PgMemctx::Reset(PgMemctx *handle) {
  if (handle) {
    if (postgres_process_memctxs.find(handle) == postgres_process_memctxs.end()) {
        Status status {
            .pg_code = ERRCODE_INTERNAL_ERROR,
            .k2_code = 500,
            .msg = "Invalid memory context handle",
            .detail = ""
        };
        return status;
    }
    handle->_Clear();
  }
  return Status::OK;
}

void PgMemctx::_Clear() {
  // The safest option is to retain all K2SQL statement objects.
  // - Clear the table descriptors from cache. We can just reload them when requested.
  //
  // PgGate and its contexts are between Postgres and K2SQL lower layers, and because these
  // layers might be still operating on the raw pointer or reference after Postgres's
  // cancellation, there's a chance we might have an unexpected issue.
  tabledesc_map_.clear();
  // invoke all deleters
  for (auto& func: _deleters) {
    func();
  }
  _deleters.clear();
}

void PgMemctx::Cache(size_t hash_id, const std::shared_ptr<PgTableDesc> &table_desc) {
  // Add table descriptor to table.
  tabledesc_map_[hash_id] = table_desc;
}

void PgMemctx::GetCache(size_t hash_id, PgTableDesc **handle) {
  // Read table descriptor to table.
  const auto iter = tabledesc_map_.find(hash_id);
  if (iter == tabledesc_map_.end()) {
    *handle = nullptr;
  } else {
    *handle = iter->second.get();
  }
}

}  // namespace k2pg
