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

#include <stddef.h>
#include "access/k2/data_type.h"
#include "access/k2/status.h"
#include "access/k2/pg_ids.h"
#include "access/k2/pg_session.h"
#include "access/k2/pg_expr.h"
#include "access/k2/pg_tabledesc.h"
#include "access/k2/pg_statement.h"
#include "access/k2/pg_memctx.h"

// K2 SQL data type in PG C code
typedef enum   K2SqlDataType K2PgDataType;
typedef class  k2pg::PgSession  * K2PgSession;
typedef class  k2pg::PgStatement * K2PgStatement;
typedef class  k2pg::PgExpr * K2PgExpr;
typedef class  k2pg::PgTableDesc * K2PgTableDesc;
typedef class  k2pg::PgMemctx * K2PgMemctx;
typedef struct k2pg::Status K2PgStatus;
typedef struct k2pg::PgSysColumns K2PgSysColumns;

// Postgres object identifier (OID) defined in Postgres' postgres_ext.h
typedef unsigned int K2PgOid;
#define kInvalidOid ((K2PgOid) 0)

typedef struct k2pg::PgTypeAttrs K2PgTypeAttrs;

typedef struct k2pg::PgTypeEntity K2PgTypeEntity;

// API to read type information.
const K2PgTypeEntity *K2PgFindTypeEntity(int type_oid);
K2PgDataType K2PgGetType(const K2PgTypeEntity *type_entity);
bool K2PgAllowForPrimaryKey(const K2PgTypeEntity *type_entity);

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
typedef struct PgPrepareParameters {
  K2PgOid index_oid;
  bool index_only_scan;
  bool use_secondary_index;
} K2PgPrepareParameters;

// Structure to hold the execution-control parameters.
typedef struct PgExecParameters {
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
} K2PgExecParameters;

typedef struct PgCallbacks {
  void (*FetchUniqueConstraintName)(K2PgOid, char*, size_t);
  K2PgMemctx (*GetCurrentK2Memctx)();
} K2PgCallbacks;

typedef struct PgTableProperties {
  uint32_t num_hash_key_columns;
} K2PgTableProperties;
