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

#ifdef __cplusplus
extern "C" {
#endif

// This must be called exactly once to initialize the YPostgreSQL/SKV gateway API before any other
// functions in this API are called.
void PgGate_InitPgGate(const K2PgTypeEntity *k2PgDataTypeTable, int count, K2PgCallbacks pg_callbacks);
void PgGate_DestroyPgGate();

// Initialize ENV within which PGSQL calls will be executed.
K2PgStatus PgGate_CreateEnv(K2PgEnv *pg_env);
K2PgStatus PgGate_DestroyEnv(K2PgEnv pg_env);

// Initialize a session to process statements that come from the same client connection.
K2PgStatus PgGate_InitSession(const K2PgEnv pg_env, const char *database_name);

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

K2PgStatus PgGate_InsertSequenceTuple(int64_t db_oid,
                                 int64_t seq_oid,
                                 uint64_t psql_catalog_version,
                                 int64_t last_val,
                                 bool is_called);

K2PgStatus PgGate_UpdateSequenceTupleConditionally(int64_t db_oid,
                                              int64_t seq_oid,
                                              uint64_t psql_catalog_version,
                                              int64_t last_val,
                                              bool is_called,
                                              int64_t expected_last_val,
                                              bool expected_is_called,
                                              bool *skipped);

K2PgStatus PgGate_UpdateSequenceTuple(int64_t db_oid,
                                 int64_t seq_oid,
                                 uint64_t psql_catalog_version,
                                 int64_t last_val,
                                 bool is_called,
                                 bool* skipped);

K2PgStatus PgGate_ReadSequenceTuple(int64_t db_oid,
                               int64_t seq_oid,
                               uint64_t psql_catalog_version,
                               int64_t *last_val,
                               bool *is_called);

K2PgStatus PgGate_DeleteSequenceTuple(int64_t db_oid, int64_t seq_oid);

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
    const K2PgTypeEntity *attr_type;
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
                                   const K2PgTypeEntity *attr_type, bool is_not_null);

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

K2PgStatus PgGate_NewTruncateTable(K2PgOid database_oid,
                                K2PgOid table_oid,
                                K2PgStatement *handle);

K2PgStatus PgGate_ExecTruncateTable(K2PgStatement handle);

K2PgStatus PgGate_GetTableDesc(K2PgOid database_oid,
                            K2PgOid table_oid,
                            K2PgTableDesc *handle);

K2PgStatus PgGate_GetColumnInfo(K2PgTableDesc table_desc,
                             int16_t attr_number,
                             bool *is_primary,
                             bool *is_hash);

K2PgStatus PgGate_GetTableProperties(K2PgTableDesc table_desc,
                                  K2PgTableProperties *properties);

K2PgStatus PgGate_SetIsSysCatalogVersionChange(K2PgStatement handle);

K2PgStatus PgGate_SetCatalogCacheVersion(K2PgStatement handle, uint64_t catalog_cache_version);

K2PgStatus PgGate_IsTableColocated(const K2PgOid database_oid,
                                const K2PgOid table_oid,
                                bool *colocated);

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

K2PgStatus PgGate_WaitUntilIndexPermissionsAtLeast(
    const K2PgOid database_oid,
    const K2PgOid table_oid,
    const K2PgOid index_oid,
    const uint32_t target_index_permissions,
    uint32_t *actual_index_permissions);

K2PgStatus PgGate_AsyncUpdateIndexPermissions(
    const K2PgOid database_oid,
    const K2PgOid indexed_table_oid);

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

class K2PgScanHandle;

// bind range condition so as to derive key prefix
// TODO maybe remove if not needed after user table scan is hooked in
K2PgStatus PgGate_DmlBindRangeConds(K2PgStatement handle, K2PgExpr where_conds);

// bind where clause for a DML operation
// TODO maybe remove if not needed after user table scan is hooked in
K2PgStatus PgGate_DmlBindWhereConds(K2PgStatement handle, K2PgExpr where_conds);

// Binding Tables: Bind the whole table in a statement.  Do not use with BindColumn.
K2PgStatus PgGate_DmlBindTable(K2PgStatement handle);

// This function is to fetch the targets in the vector from the Exec call, from the rows that were defined
// by the constraints vector in the Exec call
K2PgStatus PgGate_DmlFetch(K2PgScanHandle* handle, int32_t natts, uint64_t *values, bool *isnulls,
                        K2PgSysColumns *syscols, bool *has_data);

// Utility method that checks stmt type and calls either exec insert, update, or delete internally.
// TODO now only used by DDL commands that we do not support right now (e.g. Drop table). It will need
// to be updated when we add this support.
K2PgStatus PgGate_DmlExecWriteOp(K2PgStatement handle, int32_t *rows_affected_count);

// This function returns the tuple id (k2pgctid) of a Postgres tuple.
    K2PgStatus PgGate_DmlBuildPgTupleId(K2PgStatement handle, const K2PgAttrValueDescriptor *attrs,
                                 int32_t nattrs, uint64_t *k2pgctid);

// DB Operations: WHERE(partially supported by K2-SKV)
// TODO: ORDER_BY, GROUP_BY, etc.

// INSERT ------------------------------------------------------------------------------------------
struct K2PgWriteColumnDef {
    int attr_num;
    Oid type_id;
    Datum datum;
    bool is_null;
};

K2PgStatus PgGate_ExecInsert(K2PgOid database_oid,
                             K2PgOid table_oid,
                             bool upsert,
                             bool increment_catalog,
                             const std::vector<K2PgWriteColumnDef>& columns);

// UPDATE ------------------------------------------------------------------------------------------
K2PgStatus PgGate_ExecUpdate(K2PgOid database_oid,
                             K2PgOid table_oid,
                             bool increment_catalog,
                             int* rows_affected,
                             const std::vector<K2PgWriteColumnDef>& columns);

// DELETE ------------------------------------------------------------------------------------------
K2PgStatus PgGate_ExecDelete(K2PgOid database_oid,
                             K2PgOid table_oid,
                             bool increment_catalog,
                             int* rows_affected,
                             const std::vector<K2PgWriteColumnDef>& columns);

// Colocated TRUNCATE ------------------------------------------------------------------------------
K2PgStatus PgGate_NewTruncateColocated(K2PgOid database_oid,
                                    K2PgOid table_oid,
                                    bool is_single_row_txn,
                                    K2PgStatement *handle);

K2PgStatus PgGate_ExecTruncateColocated(K2PgStatement handle);

// SELECT ------------------------------------------------------------------------------------------
K2PgStatus PgGate_NewSelect(K2PgOid database_oid,
                         K2PgOid table_oid,
                         const K2PgPrepareParameters *prepare_params,
                         K2PgScanHandle **handle);

// NOTE ON KEY CONSTRAINTS
// Scan type is speficied as part of prepare_params in NewSelect
// - For Sequential Scan, the target columns of the bind are those in the main table.
// - For Primary Scan, the target columns of the bind are those in the main table.
// - For Index Scan, the target columns of the bind are those in the index table.
//   The index-scan will use the bind to find base-k2pgctid which is then use to read data from
//   the main-table, and therefore the bind-arguments are not associated with columns in main table.
K2PgStatus PgGate_ExecSelect(K2PgScanHandle *handle, std::vector<K2PgConstraintDef> constraints, std::vector<int> targets_attrnum,
                             bool whole_table_scan, bool forward_scan, const K2PgExecParameters *exec_params);

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
// Expressions.

// Column references.
K2PgStatus PgGate_NewColumnRef(K2PgStatement stmt, int attr_num, const K2PgTypeEntity *type_entity,
                            const K2PgTypeAttrs *type_attrs, K2PgExpr *expr_handle);

// Constant expressions.
K2PgStatus PgGate_NewConstant(K2PgStatement stmt, const K2PgTypeEntity *type_entity,
                           uint64_t datum, bool is_null, K2PgExpr *expr_handle);
K2PgStatus PgGate_NewConstantOp(K2PgStatement stmt, const K2PgTypeEntity *type_entity,
                           uint64_t datum, bool is_null, K2PgExpr *expr_handle, bool is_gt);

// The following update functions only work for constants.
// Overwriting the constant expression with new value.
K2PgStatus PgGate_UpdateConstInt2(K2PgExpr expr, int16_t value, bool is_null);
K2PgStatus PgGate_UpdateConstInt4(K2PgExpr expr, int32_t value, bool is_null);
K2PgStatus PgGate_UpdateConstInt8(K2PgExpr expr, int64_t value, bool is_null);
K2PgStatus PgGate_UpdateConstFloat4(K2PgExpr expr, float value, bool is_null);
K2PgStatus PgGate_UpdateConstFloat8(K2PgExpr expr, double value, bool is_null);
K2PgStatus PgGate_UpdateConstText(K2PgExpr expr, const char *value, bool is_null);
K2PgStatus PgGate_UpdateConstChar(K2PgExpr expr, const char *value, int64_t bytes, bool is_null);

// Expressions with operators "=", "+", "between", "in", ...
K2PgStatus PgGate_NewOperator(K2PgStatement stmt, const char *opname,
                           const K2PgTypeEntity *type_entity,
                           K2PgExpr *op_handle);
K2PgStatus PgGate_OperatorAppendArg(K2PgExpr op_handle, K2PgExpr arg);

// Referential Integrity Check Caching.
// Check if foreign key reference exists in cache.
bool PgGate_ForeignKeyReferenceExists(K2PgOid table_oid, const char* k2pgctid, int64_t k2pgctid_size);

// Add an entry to foreign key reference cache.
K2PgStatus PgGate_CacheForeignKeyReference(K2PgOid table_oid, const char* k2pgctid, int64_t k2pgctid_size);

// Delete an entry from foreign key reference cache.
K2PgStatus PgGate_DeleteFromForeignKeyReferenceCache(K2PgOid table_oid, uint64_t k2pgctid);

void PgGate_ClearForeignKeyReferenceCache();

bool PgGate_IsInitDbModeEnvVarSet();

// This is called by initdb. Used to customize some behavior.
void PgGate_InitFlags();

// Retrieves value of psql_max_read_restart_attempts gflag
int32_t PgGate_GetMaxReadRestartAttempts();

// Retrieves value of psql_output_buffer_size gflag
int32_t PgGate_GetOutputBufferSize();

// Retrieve value of psql_disable_index_backfill gflag.
bool PgGate_GetDisableIndexBackfill();

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

#ifdef __cplusplus
}  // extern "C"
#endif
