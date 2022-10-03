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

#include <memory>
#include <string>

#include "access/k2/pg_ids.h"

namespace k2pg {
// PostgreSQL can represent text strings up to 1 GB minus a four-byte header.
static const int64_t kMaxPostgresTextSizeBytes = 1024ll * 1024 * 1024 - 4;

// Structure to hold the values of hidden columns when passing tuple from K2 SQL to PG.
struct PgSysColumns {
    // Postgres system columns.
    uint32_t oid;
    uint32_t tableoid;
    uint32_t xmin;
    uint32_t cmin;
    uint32_t xmax;
    uint32_t cmax;
    uint64_t ctid;

    // K2 Sql system columns.
    uint8_t *k2pgctid;
    uint8_t *k2pgbasectid;
};

// In addition to regular columns, K2PG support for postgres also have virtual columns.
// Virtual columns are just expression that is evaluated by K2 PG.
enum class PgSystemAttrNum : int {
    // Postgres system columns.
    kSelfItemPointer      = -1, // ctid.
    kObjectId             = -2, // oid.
    kMinTransactionId     = -3, // xmin
    kMinCommandId         = -4, // cmin
    kMaxTransactionId     = -5, // xmax
    kMaxCommandId         = -6, // cmax
    kTableOid             = -7, // tableoid

    // K2PG system columns.
    kPgTupleId            = -8, // k2pgctid: virtual column representing K2 record key.
                                // K2 PG analogue of Postgres's SelfItemPointer/ctid column.

    // The following attribute numbers are stored persistently in the table schema. For this reason,
    // they are chosen to avoid potential conflict with Postgres' own sys attributes now and future.
    kPgRowId              = -100, // k2pgrowid: auto-generated key-column for tables without pkey.
    kPgIdxBaseTupleId     = -101, // k2pgidxbasectid: for indexes, the k2pgctid of base table row.
    kPgUniqueIdxKeySuffix = -102, // k2pguniqueidxkeysuffix: extra key column for unique indexes, used
                                  // to ensure SQL semantics for null (null != null) in K2 PG
                                  // (where null == null). For each index row will be set to:
                                  //  - the base table ctid when one or more indexed cols are null
                                  //  - to null otherwise (all indexed cols are non-null).
};

struct PgTableProperties {
  uint32_t num_hash_key_columns;
};

}  // namespace k2pg
