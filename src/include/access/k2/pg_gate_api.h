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

#include "postgres.h"

#include <cstdint>
#include <vector>

#include "access/k2/k2pg_util.h"
#include "access/k2/pg_gate_typedefs.h"
#include "access/k2/status.h"

// This must be called exactly once to initialize the YPostgreSQL/SKV gateway API before any other
// functions in this API are called.
void PgGate_InitPgGate();
void PgGate_DestroyPgGate();

// Initialize a session to process statements that come from the same client connection.
K2PgStatus PgGate_InitSession(const char *database_name);

// Initialize K2PgMemCtx.
// - Postgres uses memory context to hold all of its allocated space. Once all associated operations
//   are done, the context is destroyed.
// - There K2Pg objects are bound to Postgres operations. All of these objects' allocated
//   memory will be held by K2PgMemCtx, whose handle belongs to Postgres MemoryContext. Once all
//   Postgres operations are done, associated K2PG memory context (K2PgMemCtx) will be
//   destroyed toghether with Postgres memory context.
K2PgMemctx PgGate_CreateMemctx();
K2PgStatus PgGate_DestroyMemctx(K2PgMemctx memctx);
K2PgStatus PgGate_ResetMemctx(K2PgMemctx memctx);

// Invalidate the sessions table cache.
K2PgStatus PgGate_InvalidateCache();

// Check if initdb has been already run.
K2PgStatus PgGate_IsInitDbDone(bool* initdb_done);

// Sets catalog_version to the local tserver's catalog version stored in shared
// memory, or an error if the shared memory has not been initialized (e.g. in initdb).
K2PgStatus PgGate_GetSharedCatalogVersion(uint64_t* catalog_version);

//--------------------------------------------------------------------------------------------------
// DDL Statements
//--------------------------------------------------------------------------------------------------

// DATABASE ----------------------------------------------------------------------------------------
// Connect database. Switch the connected database to the given "database_name".
K2PgStatus PgGate_ConnectDatabase(const char *database_name);

// K2 InitPrimaryCluster
K2PgStatus PgGate_InitPrimaryCluster();

K2PgStatus PgGate_FinishInitDB();

// Create database.
K2PgStatus PgGate_ExecCreateDatabase(const char *database_name,
                                 K2PgOid database_oid,
                                 K2PgOid source_database_oid,
                                 K2PgOid next_oid);

// Drop database.
K2PgStatus PgGate_ExecDropDatabase(const char *database_name,
                                   K2PgOid database_oid);

// Alter database.
K2PgStatus PgGate_NewAlterDatabase(const char *database_name,
                               K2PgOid database_oid,
                               K2PgStatement *handle);
K2PgStatus PgGate_AlterDatabaseRenameDatabase(K2PgStatement handle, const char *new_name);
K2PgStatus PgGate_ExecAlterDatabase(K2PgStatement handle);

// Reserve oids.
K2PgStatus PgGate_ReserveOids(K2PgOid database_oid,
                           K2PgOid next_oid,
                           uint32_t count,
                           K2PgOid *begin_oid,
                           K2PgOid *end_oid);

K2PgStatus PgGate_GetCatalogMasterVersion(uint64_t *version);

void PgGate_InvalidateTableCache(
    const K2PgOid database_oid,
    const K2PgOid table_oid);

K2PgStatus PgGate_InvalidateTableCacheByTableId(const char *table_uuid);

// TABLE -------------------------------------------------------------------------------------------
struct K2PGColumnDef {
    const char* attr_name;
    int attr_num;
    Oid type_oid;
    bool is_key;
    bool is_desc;
    bool is_nulls_first;
};

// Create and drop table "database_name.schema_name.table_name()".
// - When "schema_name" is NULL, the table "database_name.table_name" is created.
// - When "database_name" is NULL, the table "connected_database_name.table_name" is created.
K2PgStatus PgGate_ExecCreateTable(const char *database_name,
                              const char *schema_name,
                              const char *table_name,
                              K2PgOid database_oid,
                              K2PgOid table_oid,
                              bool if_not_exist,
                              bool add_primary_key,
                              const std::vector<K2PGColumnDef>& columns);

K2PgStatus PgGate_NewAlterTable(K2PgOid database_oid,
                             K2PgOid table_oid,
                             K2PgStatement *handle);

K2PgStatus PgGate_AlterTableAddColumn(K2PgStatement handle, const char *name, int order,
                                   int type_oid, bool is_not_null);

K2PgStatus PgGate_AlterTableRenameColumn(K2PgStatement handle, const char *oldname,
                                      const char *newname);

K2PgStatus PgGate_AlterTableDropColumn(K2PgStatement handle, const char *name);

K2PgStatus PgGate_AlterTableRenameTable(K2PgStatement handle, const char *db_name,
                                     const char *newname);

K2PgStatus PgGate_ExecAlterTable(K2PgStatement handle);

K2PgStatus PgGate_NewDropTable(K2PgOid database_oid,
                            K2PgOid table_oid,
                            bool if_exist,
                            K2PgStatement *handle);

K2PgStatus PgGate_ExecDropTable(K2PgStatement handle);

K2PgStatus PgGate_GetTableDesc(K2PgOid database_oid,
                            K2PgOid table_oid,
                            K2PgTableDesc *handle);

K2PgStatus PgGate_GetColumnInfo(K2PgTableDesc table_desc,
                             int16_t attr_number,
                             bool *is_primary);

K2PgStatus PgGate_SetIsSysCatalogVersionChange(K2PgStatement handle);

K2PgStatus PgGate_SetCatalogCacheVersion(K2PgStatement handle, uint64_t catalog_cache_version);

// INDEX -------------------------------------------------------------------------------------------
// Create and drop index "database_name.schema_name.index_name()".
// - When "schema_name" is NULL, the index "database_name.index_name" is created.
// - When "database_name" is NULL, the index "connected_database_name.index_name" is created.
K2PgStatus PgGate_ExecCreateIndex(const char *database_name,
                              const char *schema_name,
                              const char *index_name,
                              K2PgOid database_oid,
                              K2PgOid index_oid,
                              K2PgOid table_oid,
                              bool is_unique_index,
                              const bool skip_index_backfill,
                              bool if_not_exist,
                              const std::vector<K2PGColumnDef>& columns);

K2PgStatus PgGate_NewDropIndex(K2PgOid database_oid,
                            K2PgOid index_oid,
                            bool if_exist,
                            K2PgStatement *handle);

K2PgStatus PgGate_ExecDropIndex(K2PgStatement handle);

//--------------------------------------------------------------------------------------------------
// DML statements (select, insert, update, delete, truncate)
//--------------------------------------------------------------------------------------------------
struct K2PgConstant {
    Oid type_id;
    Datum datum;
    bool is_null;
};

enum K2PgConstraintType {
    K2PG_CONSTRAINT_UNKNOWN,
    K2PG_CONSTRAINT_EQ,
    K2PG_CONSTRAINT_BETWEEN,
    K2PG_CONSTRAINT_IN
    // TODO Add constraints needed by user scan
};

struct K2PgConstraintDef {
    int attr_num;
    K2PgConstraintType constraint;
    std::vector<K2PgConstant> constants; // Only 1 element for EQ etc, 2 for BETWEEN, many for IN
};

struct K2PgAttributeDef {
    int attr_num;
    K2PgConstant value;
};

class K2PgScanHandle;

// This function is to fetch the targets in the vector from the Exec call, from the rows that were defined
// by the constraints vector in the Exec call
K2PgStatus PgGate_DmlFetch(K2PgScanHandle* handle, int32_t natts, uint64_t *values, bool *isnulls,
                        K2PgSysColumns *syscols, bool *has_data);

// This function returns the tuple id (k2pgctid) of a Postgres tuple.
K2PgStatus PgGate_DmlBuildPgTupleId(Oid db_oid, Oid table_oid, const std::vector<K2PgAttributeDef>& attrs,
                                    uint64_t *k2pgctid);

// DB Operations: WHERE(partially supported by K2-SKV)
// TODO: ORDER_BY, GROUP_BY, etc.

// INSERT ------------------------------------------------------------------------------------------
K2PgStatus PgGate_ExecInsert(K2PgOid database_oid,
                             K2PgOid table_oid,
                             bool upsert,
                             bool increment_catalog,
                             std::vector<K2PgAttributeDef>& columns);

// UPDATE ------------------------------------------------------------------------------------------
K2PgStatus PgGate_ExecUpdate(K2PgOid database_oid,
                             K2PgOid table_oid,
                             bool increment_catalog,
                             int* rows_affected,
                             const std::vector<K2PgAttributeDef>& columns);

// DELETE ------------------------------------------------------------------------------------------
K2PgStatus PgGate_ExecDelete(K2PgOid database_oid,
                             K2PgOid table_oid,
                             bool increment_catalog,
                             int* rows_affected,
                             const std::vector<K2PgAttributeDef>& columns);

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
struct K2PgSelectIndexParams {
  K2PgOid index_oid;
  bool index_only_scan;
  bool use_secondary_index;
};

// SELECT ------------------------------------------------------------------------------------------
K2PgStatus PgGate_NewSelect(K2PgOid database_oid,
                         K2PgOid table_oid,
                         const K2PgSelectIndexParams& index_params,
                         K2PgScanHandle **handle);

struct K2PgSelectLimitParams {
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
};

// NOTE ON KEY CONSTRAINTS
// Scan type is speficied as part of index_params in NewSelect
// - For Sequential Scan, the target columns of the bind are those in the main table.
// - For Primary Scan, the target columns of the bind are those in the main table.
// - For Index Scan, the target columns of the bind are those in the index table.
//   The index-scan will use the bind to find base-k2pgctid which is then use to read data from
//   the main-table, and therefore the bind-arguments are not associated with columns in main table.
K2PgStatus PgGate_ExecSelect(K2PgScanHandle *handle, const std::vector<K2PgConstraintDef>& constraints, const std::vector<int>& targets_attrnum,
                             bool whole_table_scan, bool forward_scan, const K2PgSelectLimitParams& limit_params);

// Transaction control -----------------------------------------------------------------------------
K2PgStatus PgGate_BeginTransaction();
K2PgStatus PgGate_RestartTransaction();
K2PgStatus PgGate_CommitTransaction();
K2PgStatus PgGate_AbortTransaction();
K2PgStatus PgGate_SetTransactionIsolationLevel(int isolation);
K2PgStatus PgGate_SetTransactionReadOnly(bool read_only);
K2PgStatus PgGate_SetTransactionDeferrable(bool deferrable);
K2PgStatus PgGate_EnterSeparateDdlTxnMode();
K2PgStatus PgGate_ExitSeparateDdlTxnMode(bool success);

//--------------------------------------------------------------------------------------------------

// Referential Integrity Check Caching.
// Check if foreign key reference exists in cache.
bool PgGate_ForeignKeyReferenceExists(K2PgOid table_oid, const char* k2pgctid, int64_t k2pgctid_size);

// Add an entry to foreign key reference cache.
K2PgStatus PgGate_CacheForeignKeyReference(K2PgOid table_oid, const char* k2pgctid, int64_t k2pgctid_size);

// Delete an entry from foreign key reference cache.
K2PgStatus PgGate_DeleteFromForeignKeyReferenceCache(K2PgOid table_oid, uint64_t k2pgctid);

void PgGate_ClearForeignKeyReferenceCache();

bool PgGate_IsK2PgEnabled();

// Sets the specified timeout in the rpc service.
void PgGate_SetTimeout(int timeout_ms, void* extra);

//--------------------------------------------------------------------------------------------------
// Thread-Local variables.

void* PgGate_GetThreadLocalCurrentMemoryContext();

void* PgGate_SetThreadLocalCurrentMemoryContext(void *memctx);

void PgGate_ResetCurrentMemCtxThreadLocalVars();

void* PgGate_GetThreadLocalStrTokPtr();

void PgGate_SetThreadLocalStrTokPtr(char *new_pg_strtok_ptr);

void* PgGate_SetThreadLocalJumpBuffer(void* new_buffer);

void* PgGate_GetThreadLocalJumpBuffer();

void PgGate_SetThreadLocalErrMsg(const void* new_msg);

const void* PgGate_GetThreadLocalErrMsg();

// APIs called by pg_dump.c only
void PgGate_ShutdownPgGateBackend();

K2PgStatus PgGate_InitPgGateBackend();
