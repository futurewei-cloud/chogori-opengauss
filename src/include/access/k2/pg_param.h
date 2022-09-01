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

#include "access/k2/pg_type.h"
#include "access/k2/pg_ids.h"

namespace k2pg {
// PostgreSQL can represent text strings up to 1 GB minus a four-byte header.
static const int64_t kMaxPostgresTextSizeBytes = 1024ll * 1024 * 1024 - 4;

// type oids defined in pg_type_d.h, which was generated by the build script
static const int32_t BOOL_TYPE_OID = 16;
static const int32_t STRING_TYPE_OID = 19;
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

// Datatype representation:
// Definition of a datatype is divided into two different sections.
// - K2PgTypeEntity is used to keep static information of a datatype.
// - K2PgTypeAttrs is used to keep customizable information of a datatype.
//
struct PgTypeAttrs {
  // Currently, we only need typmod, but we might need more datatype information in the future.
  // For example, array dimensions might be needed.
  int32_t typmod;
};

// Datatype conversion functions.
typedef void (*K2PgDatumToData)(uint64_t datum, void *ybdata, int64_t *bytes);
typedef uint64_t (*K2PgDatumFromData)(const void *ybdata, int64_t bytes,
                                       const PgTypeAttrs *type_attrs);

struct PgTypeEntity {
  // Postgres type OID.
  int type_oid;

  // K2 SQL type.
  K2SqlDataType k2pg_type;

  // Allow to be used for primary key.
  bool allow_for_primary_key;

  // Datum in-memory fixed size.
  // - Size of in-memory representation for a type. Usually it's sizeof(a_struct).
  //   Example: BIGINT in-memory size === sizeof(int64)
  //            POINT in-memory size === sizeof(struct Point)
  // - Set to (-1) for types of variable in-memory size - VARSIZE_ANY should be used.
  int64_t datum_fixed_size;

  // Converting Postgres datum to K2 SQL expression.
  K2PgDatumToData datum_to_k2pg;

  // Converting K2 SQL values to Postgres in-memory-formatted datum.
  K2PgDatumFromData k2pg_to_datum;
};

// Structure to hold parameters for preparing query plan.
//
// Index-related parameters are used to describe different types of scan.
//   - Sequential scan: Index parameter is not used.
//     { index_oid, index_only_scan, use_secondary_index } = { kInvalidOid, false, false }
//   - IndexScan:
//     { index_oid, index_only_scan, use_secondary_index } = { IndexOid, false, true }
//   - IndexOnlyScan:
//     { index_oid, index_only_scan, use_secondary_index } = { IndexOid, true, true }
//   - PrimaryIndexScan: This is a special case as K2 SQL doesn't have a separated
//     primary-index database object from table object.
//       index_oid = TableOid
//       index_only_scan = true if ROWID is wanted. Otherwise, regular rowset is wanted.
//       use_secondary_index = false
//
struct PgPrepareParameters {
  PgOid index_oid;
  bool index_only_scan;
  bool use_secondary_index;
};

// Structure to hold the execution-control parameters.
struct PgExecParameters {
  // TODO(neil) Move forward_scan flag here.
  // Scan parameters.
  // bool is_forward_scan;

  // LIMIT parameters for executing DML read.
  // - limit_count is the value of SELECT ... LIMIT
  // - limit_offset is value of SELECT ... OFFSET
  // - limit_use_default: Although count and offset are pushed down to K2 platform from Postgres,
  //   they are not always being used to identify the number of rows to be read from K2 platform.
  //   Full-scan is needed when further operations on the rows are not done by K2 platform.
  //
  //   Examples:
  //   o All rows must be sent to Postgres code layer
  //     for filtering before LIMIT is applied.
  //   o ORDER BY clause is not processed by K2 platform. Similarly all rows must be fetched and sent
  //     to Postgres code layer.
  int64_t limit_count;
  uint64_t limit_offset;
  bool limit_use_default;
  // For now we only support one rowmark.
  int rowmark = -1;
  uint64_t read_time = 0;
  char *partition_key = NULL;
};

struct PgTableProperties {
  uint32_t num_hash_key_columns;
};

}  // namespace k2pg
