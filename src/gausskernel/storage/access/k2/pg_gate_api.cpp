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

#include "access/k2/pg_gate_api.h"

#include "k2pg-internal.h"
#include "access/k2/k2pg_util.h"

#include "utils/elog.h"
#include "pg_gate_defaults.h"
#include "pg_gate_thread_local.h"

#include <atomic>

namespace k2pg {
namespace gate {

namespace {
// Using a raw pointer here to fully control object initialization and destruction.
// k2pg::gate::PgGateApiImpl* api_impl;
std::atomic<bool> api_impl_shutdown_done;

} // anonymous namespace

extern "C" {

void PgGate_InitPgGate(const K2PgTypeEntity *k2PgDataTypeTable, int count, PgCallbacks pg_callbacks) {
    elog(INFO, "K2 PgGate open");
 //   K2ASSERT(log::pg, api_impl == nullptr, "can only be called once");
    api_impl_shutdown_done.exchange(false);
}

void PgGate_DestroyPgGate() {
    if (api_impl_shutdown_done.exchange(true)) {
      elog(ERROR, "should only be called once");
    } else {
      elog(INFO, "K2 PgGate destroyed");
    }
}

K2PgStatus PgGate_CreateEnv(K2PgEnv *pg_env) {
  elog(DEBUG5, "PgGateAPI: PgGate_CreateEnv");
  
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

K2PgStatus PgGate_DestroyEnv(K2PgEnv pg_env) {
  elog(DEBUG5, "PgGateAPI: PgGate_DestroyEnv");
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

// Initialize a session to process statements that come from the same client connection.
K2PgStatus PgGate_InitSession(const K2PgEnv pg_env, const char *database_name) {
  elog(LOG, "PgGateAPI: PgGate_InitSession %s", database_name);
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

// Initialize K2PgMemCtx.
// - Postgres uses memory context to hold all of its allocated space. Once all associated operations
//   are done, the context is destroyed.
// - There K2PG objects are bound to Postgres operations. All of these objects' allocated
//   memory will be held by K2PgMemCtx, whose handle belongs to Postgres MemoryContext. Once all
//   Postgres operations are done, associated K2PG memory context (K2PgMemCtx) will be
//   destroyed toghether with Postgres memory context.
K2PgMemctx PgGate_CreateMemctx() {
  elog(DEBUG5, "PgGateAPI: PgGate_CreateMemctx");
  return nullptr;
}

K2PgStatus PgGate_DestroyMemctx(K2PgMemctx memctx) {
  elog(DEBUG5, "PgGateAPI: PgGate_DestroyMemctx");
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

K2PgStatus PgGate_ResetMemctx(K2PgMemctx memctx) {
  elog(DEBUG5, "PgGateAPI: PgGate_ResetMemctx");
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

// Invalidate the sessions table cache.
K2PgStatus PgGate_InvalidateCache() {
  elog(DEBUG5, "PgGateAPI: PgGate_InvalidateCache");
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

// Check if initdb has been already run.
K2PgStatus PgGate_IsInitDbDone(bool* initdb_done) {
  elog(DEBUG5, "PgGateAPI: PgGate_IsInitDbDone");
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

// Sets catalog_version to the local tserver's catalog version stored in shared
// memory, or an error if the shared memory has not been initialized (e.g. in initdb).
K2PgStatus PgGate_GetSharedCatalogVersion(uint64_t* catalog_version) {
  elog(DEBUG5, "PgGateAPI: PgGate_GetSharedCatalogVersion");
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

//--------------------------------------------------------------------------------------------------
// DDL Statements
//--------------------------------------------------------------------------------------------------

// K2 InitPrimaryCluster
K2PgStatus PgGate_InitPrimaryCluster()
{
  elog(DEBUG5, "PgGateAPI: PgGate_InitPrimaryCluster");
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

K2PgStatus PgGate_FinishInitDB()
{
  elog(DEBUG5, "PgGateAPI: PgGate_FinishInitDB()");
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

// DATABASE ----------------------------------------------------------------------------------------
// Connect database. Switch the connected database to the given "database_name".
K2PgStatus PgGate_ConnectDatabase(const char *database_name) {
  elog(DEBUG5, "PgGateAPI: PgGate_ConnectDatabase %s", database_name);
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

// Create database.
K2PgStatus PgGate_ExecCreateDatabase(const char *database_name,
                                 K2PgOid database_oid,
                                 K2PgOid source_database_oid,
                                 K2PgOid next_oid) {
  elog(LOG, "PgGateAPI: PgGate_ExecCreateDatabase %s, %d, %d, %d",
         database_name, database_oid, source_database_oid, next_oid);
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

// Drop database.
K2PgStatus PgGate_ExecDropDatabase(const char *database_name,
                                   K2PgOid database_oid) {
  elog(DEBUG5, "PgGateAPI: PgGate_ExecDropDatabase %s, %d", database_name, database_oid);
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

// Alter database.
K2PgStatus PgGate_NewAlterDatabase(const char *database_name,
                               K2PgOid database_oid,
                               K2PgStatement *handle) {
  elog(DEBUG5, "PgGateAPI: PgGate_NewAlterDatabase %s, %d", database_name, database_oid);
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

K2PgStatus PgGate_AlterDatabaseRenameDatabase(K2PgStatement handle, const char *new_name) {
  elog(DEBUG5, "PgGateAPI: PgGate_AlterDatabaseRenameDatabase %s", new_name);
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

K2PgStatus PgGate_ExecAlterDatabase(K2PgStatement handle) {
  elog(DEBUG5, "PgGateAPI: PgGate_ExecAlterDatabase");
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

// Reserve oids.
K2PgStatus PgGate_ReserveOids(K2PgOid database_oid,
                           K2PgOid next_oid,
                           uint32_t count,
                           K2PgOid *begin_oid,
                           K2PgOid *end_oid) {
  elog(DEBUG5, "PgGateAPI: PgGate_ReserveOids %d, %d, %d", database_oid, next_oid, count);
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

K2PgStatus PgGate_GetCatalogMasterVersion(uint64_t *version) {
  elog(DEBUG5, "PgGateAPI: PgGate_GetCatalogMasterVersion");
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

void PgGate_InvalidateTableCache(
    const K2PgOid database_oid,
    const K2PgOid table_oid) {
  elog(DEBUG5, "PgGateAPI: PgGate_InvalidateTableCache %d, %d", database_oid, table_oid);
}

K2PgStatus PgGate_InvalidateTableCacheByTableId(const char *table_uuid) {
  elog(DEBUG5, "PgGateAPI: PgGate_InvalidateTableCacheByTableId %s", table_uuid);
  if (table_uuid == NULL) {
    K2PgStatus status {
        .pg_code = ERRCODE_FDW_ERROR,
        .k2_code = 400,
        .msg = "Invalid argument",
        .detail = "table_uuid is null"
    };

    return status;
  }

  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

// Sequence Operations -----------------------------------------------------------------------------

K2PgStatus PgGate_InsertSequenceTuple(int64_t db_oid,
                                 int64_t seq_oid,
                                 uint64_t psql_catalog_version,
                                 int64_t last_val,
                                 bool is_called) {
  elog(DEBUG5, "PgGateAPI: PgGate_InsertSequenceTuple %ld, %ld, %ld", db_oid, seq_oid, psql_catalog_version);
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

K2PgStatus PgGate_UpdateSequenceTupleConditionally(int64_t db_oid,
                                              int64_t seq_oid,
                                              uint64_t psql_catalog_version,
                                              int64_t last_val,
                                              bool is_called,
                                              int64_t expected_last_val,
                                              bool expected_is_called,
                                              bool *skipped) {
  elog(DEBUG5, "PgGateAPI: PgGate_UpdateSequenceTupleConditionally %ld, %ld, %ld", db_oid, seq_oid, psql_catalog_version);
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

K2PgStatus PgGate_UpdateSequenceTuple(int64_t db_oid,
                                 int64_t seq_oid,
                                 uint64_t psql_catalog_version,
                                 int64_t last_val,
                                 bool is_called,
                                 bool* skipped) {
  elog(DEBUG5, "PgGateAPI: PgGate_UpdateSequenceTuple %ld, %ld, %ld", db_oid, seq_oid, psql_catalog_version);
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

K2PgStatus PgGate_ReadSequenceTuple(int64_t db_oid,
                               int64_t seq_oid,
                               uint64_t psql_catalog_version,
                               int64_t *last_val,
                               bool *is_called) {
  elog(DEBUG5, "PgGateAPI: PgGate_ReadSequenceTuple %ld, %ld, %ld", db_oid, seq_oid, psql_catalog_version);
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

K2PgStatus PgGate_DeleteSequenceTuple(int64_t db_oid, int64_t seq_oid) {
  elog(DEBUG5, "PgGateAPI: PgGate_DeleteSequenceTuple %ld, %ld", db_oid, seq_oid);
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

// TABLE -------------------------------------------------------------------------------------------

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
                              const std::vector<K2PGColumnDef>& columns) {
  elog(DEBUG5, "PgGateAPI: PgGate_NewCreateTable %s, %s, %s", database_name, schema_name, table_name);
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

K2PgStatus PgGate_NewAlterTable(K2PgOid database_oid,
                             K2PgOid table_oid,
                             K2PgStatement *handle){
  elog(DEBUG5, "PgGateAPI: PgGate_NewAlterTable %d, %d", database_oid, table_oid);
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

K2PgStatus PgGate_AlterTableAddColumn(K2PgStatement handle, const char *name, int order,
                                   const K2PgTypeEntity *attr_type, bool is_not_null){
  elog(DEBUG5, "PgGateAPI: PgGate_AlterTableAddColumn %s", name);
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

K2PgStatus PgGate_AlterTableRenameColumn(K2PgStatement handle, const char *oldname,
                                      const char *newname){
  elog(DEBUG5, "PgGateAPI: PgGate_AlterTableRenameColumn %s, %s", oldname, newname);
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

K2PgStatus PgGate_AlterTableDropColumn(K2PgStatement handle, const char *name){
  elog(DEBUG5, "PgGateAPI: PgGate_AlterTableDropColumn %s", name);
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

K2PgStatus PgGate_AlterTableRenameTable(K2PgStatement handle, const char *db_name,
                                     const char *newname){
  elog(DEBUG5, "PgGateAPI: PgGate_AlterTableRenameTable %s, %s", db_name, newname);
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

K2PgStatus PgGate_ExecAlterTable(K2PgStatement handle){
  elog(DEBUG5, "PgGateAPI: PgGate_ExecAlterTable");
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

K2PgStatus PgGate_NewDropTable(K2PgOid database_oid,
                            K2PgOid table_oid,
                            bool if_exist,
                            K2PgStatement *handle) {
  elog(DEBUG5, "PgGateAPI: PgGate_NewDropTable %d, %d", database_oid, table_oid);
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

K2PgStatus PgGate_ExecDropTable(K2PgStatement handle){
  elog(DEBUG5, "PgGateAPI: PgGate_ExecDropTable");
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

K2PgStatus PgGate_NewTruncateTable(K2PgOid database_oid,
                                K2PgOid table_oid,
                                K2PgStatement *handle){
  elog(DEBUG5, "PgGateAPI: PgGate_NewTruncateTable %d, %d", database_oid, table_oid);
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

K2PgStatus PgGate_ExecTruncateTable(K2PgStatement handle){
  elog(DEBUG5, "PgGateAPI: PgGate_ExecTruncateTable");
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

K2PgStatus PgGate_GetTableDesc(K2PgOid database_oid,
                            K2PgOid table_oid,
                            K2PgTableDesc *handle) {
  elog(DEBUG5, "PgGateAPI: PgGate_GetTableDesc %d, %d", database_oid, table_oid);
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

K2PgStatus PgGate_GetColumnInfo(K2PgTableDesc table_desc,
                             int16_t attr_number,
                             bool *is_primary,
                             bool *is_hash) {
  elog(DEBUG5, "PgGateAPI: PgGate_GetTableDesc %d", attr_number);
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

K2PgStatus PgGate_GetTableProperties(K2PgTableDesc table_desc,
                                  K2PgTableProperties *properties){
  elog(DEBUG5, "PgGateAPI: PgGate_GetTableProperties");
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

K2PgStatus PgGate_SetIsSysCatalogVersionChange(K2PgStatement handle){
  elog(DEBUG5, "PgGateAPI: PgGate_SetIsSysCatalogVersionChange");
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

K2PgStatus PgGate_SetCatalogCacheVersion(K2PgStatement handle, uint64_t catalog_cache_version){
  elog(DEBUG5, "PgGateAPI: PgGate_SetCatalogCacheVersion %ld", catalog_cache_version);
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

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
                              const std::vector<K2PGColumnDef>& columns){
  elog(DEBUG5, "PgGateAPI: PgGate_NewCreateIndex %s, %s, %s", database_name, schema_name, index_name);
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

K2PgStatus PgGate_NewDropIndex(K2PgOid database_oid,
                            K2PgOid index_oid,
                            bool if_exist,
                            K2PgStatement *handle){
  elog(DEBUG5, "PgGateAPI: PgGate_NewDropIndex %d, %d", database_oid, index_oid);
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

K2PgStatus PgGate_ExecDropIndex(K2PgStatement handle){
  elog(DEBUG5, "PgGateAPI: PgGate_ExecDropIndex");
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

K2PgStatus PgGate_WaitUntilIndexPermissionsAtLeast(
    const K2PgOid database_oid,
    const K2PgOid table_oid,
    const K2PgOid index_oid,
    const uint32_t target_index_permissions,
    uint32_t *actual_index_permissions) {
  elog(DEBUG5, "PgGateAPI: PgGate_WaitUntilIndexPermissionsAtLeast %d, %d, %d", database_oid, table_oid, index_oid);
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

K2PgStatus PgGate_AsyncUpdateIndexPermissions(
    const K2PgOid database_oid,
    const K2PgOid indexed_table_oid){
  elog(DEBUG5, "PgGateAPI: PgGate_AsyncUpdateIndexPermissions %d, %d", database_oid,  indexed_table_oid);
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

//--------------------------------------------------------------------------------------------------
// DML statements (select, insert, update, delete, truncate)
//--------------------------------------------------------------------------------------------------

// This function is for specifying the selected or returned expressions.
// - SELECT target_expr1, target_expr2, ...
// - INSERT / UPDATE / DELETE ... RETURNING target_expr1, target_expr2, ...
K2PgStatus PgGate_DmlAppendTarget(K2PgStatement handle, K2PgExpr target){
  elog(DEBUG5, "PgGateAPI: PgGate_DmlAppendTarget");
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

// Binding Columns: Bind column with a value (expression) in a statement.
// + This API is used to identify the rows you want to operate on. If binding columns are not
//   there, that means you want to operate on all rows (full scan). You can view this as a
//   a definitions of an initial rowset or an optimization over full-scan.
//
// + There are some restrictions on when BindColumn() can be used.
//   Case 1: INSERT INTO tab(x) VALUES(x_expr)
//   - BindColumn() can be used for BOTH primary-key and regular columns.
//   - This bind-column function is used to bind "x" with "x_expr", and "x_expr" that can contain
//     bind-variables (placeholders) and constants whose values can be updated for each execution
//     of the same allocated statement.
//
//   Case 2: SELECT / UPDATE / DELETE <WHERE key = "key_expr">
//   - BindColumn() can only be used for primary-key columns.
//   - This bind-column function is used to bind the primary column "key" with "key_expr" that can
//     contain bind-variables (placeholders) and constants whose values can be updated for each
//     execution of the same allocated statement.
//
// NOTE ON KEY BINDING
// - For Sequential Scan, the target columns of the bind are those in the main table.
// - For Primary Scan, the target columns of the bind are those in the main table.
// - For Index Scan, the target columns of the bind are those in the index table.
//   The index-scan will use the bind to find base-k2pgctid which is then use to read data from
//   the main-table, and therefore the bind-arguments are not associated with columns in main table.
K2PgStatus PgGate_DmlBindColumn(K2PgStatement handle, int attr_num, K2PgExpr attr_value){
  elog(DEBUG5, "PgGateAPI: PgGate_DmlBindColumn %d", attr_num);
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

K2PgStatus PgGate_DmlBindRangeConds(K2PgStatement handle, K2PgExpr range_conds) {
  elog(DEBUG5, "PgGateAPI: PgGate_DmlBindRangeConds");
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

K2PgStatus PgGate_DmlBindWhereConds(K2PgStatement handle, K2PgExpr where_conds) {
  elog(DEBUG5, "PgGateAPI: PgGate_DmlBindWhereConds");
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

// Binding Tables: Bind the whole table in a statement.  Do not use with BindColumn.
K2PgStatus PgGate_DmlBindTable(K2PgStatement handle){
  elog(DEBUG5, "PgGateAPI: PgGate_DmlBindTable");
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

// API for SET clause.
K2PgStatus PgGate_DmlAssignColumn(K2PgStatement handle,
                               int attr_num,
                               K2PgExpr attr_value){
  elog(DEBUG5, "PgGateAPI: PgGate_DmlAssignColumn %d", attr_num);
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

// This function is to fetch the targets in PgGate_DmlAppendTarget() from the rows that were defined
// by PgGate_DmlBindColumn().
K2PgStatus PgGate_DmlFetch(K2PgStatement handle, int32_t natts, uint64_t *values, bool *isnulls,
                        K2PgSysColumns *syscols, bool *has_data){
  elog(DEBUG5, "PgGateAPI: PgGate_DmlFetch %d", natts);
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

// Utility method that checks stmt type and calls either exec insert, update, or delete internally.
K2PgStatus PgGate_DmlExecWriteOp(K2PgStatement handle, int32_t *rows_affected_count){
  elog(DEBUG5, "PgGateAPI: PgGate_DmlExecWriteOp");
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

// This function returns the tuple id (k2pgctid) of a Postgres tuple.
K2PgStatus PgGate_DmlBuildPgTupleId(Oid db_oid, Oid table_id, const std::vector<K2PgAttributeDef>& attrs,
                                    uint64_t *k2pgctid){
  elog(DEBUG5, "PgGateAPI: PgGate_DmlBuildPgTupleId %d", attrs.size());
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

// DB Operations: WHERE(partially supported by K2-SKV)
// TODO: ORDER_BY, GROUP_BY, etc.

// INSERT ------------------------------------------------------------------------------------------

K2PgStatus PgGate_ExecInsert(K2PgOid database_oid,
                             K2PgOid table_oid,
                             bool upsert,
                             bool increment_catalog,
                             const std::vector<K2PgAttributeDef>& columns) {
  elog(DEBUG5, "PgGateAPI: PgGate_ExecInsert %d, %d", database_oid, table_oid);
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

// UPDATE ------------------------------------------------------------------------------------------
K2PgStatus PgGate_ExecUpdate(K2PgOid database_oid,
                             K2PgOid table_oid,
                             bool increment_catalog,
                             int* rows_affected,
                             const std::vector<K2PgAttributeDef>& columns) {
  elog(DEBUG5, "PgGateAPI: PgGate_ExecUpdate %u, %u", database_oid, table_oid);
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

// DELETE ------------------------------------------------------------------------------------------
K2PgStatus PgGate_ExecDelete(K2PgOid database_oid,
                             K2PgOid table_oid,
                             bool increment_catalog,
                             int* rows_affected,
                             const std::vector<K2PgAttributeDef>& columns) {
  elog(DEBUG5, "PgGateAPI: PgGate_ExecDelete %d, %d", database_oid, table_oid);
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

// SELECT ------------------------------------------------------------------------------------------
K2PgStatus PgGate_NewSelect(K2PgOid database_oid,
                         K2PgOid table_oid,
                         const K2PgPrepareParameters *prepare_params,
                         K2PgStatement *handle){
  elog(DEBUG5, "PgGateAPI: PgGate_NewSelect %d, %d", database_oid, table_oid);
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

// Set forward/backward scan direction.
K2PgStatus PgGate_SetForwardScan(K2PgStatement handle, bool is_forward_scan){
  elog(DEBUG5, "PgGateAPI: PgGate_SetForwardScan %d", is_forward_scan);
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

K2PgStatus PgGate_ExecSelect(K2PgStatement handle, const K2PgExecParameters *exec_params){
  elog(DEBUG5, "PgGateAPI: PgGate_ExecSelect");
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

// Transaction control -----------------------------------------------------------------------------

K2PgStatus PgGate_BeginTransaction(){
  elog(DEBUG5, "PgGateAPI: PgGate_BeginTransaction");
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

K2PgStatus PgGate_RestartTransaction(){
  elog(DEBUG5, "PgGateAPI: PgGate_RestartTransaction");
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

K2PgStatus PgGate_CommitTransaction(){
  elog(DEBUG5, "PgGateAPI: PgGate_CommitTransaction");
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

K2PgStatus PgGate_AbortTransaction(){
  elog(DEBUG5, "PgGateAPI: PgGate_AbortTransaction");
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

K2PgStatus PgGate_SetTransactionIsolationLevel(int isolation){
  elog(DEBUG5, "PgGateAPI: PgGate_SetTransactionIsolationLevel %d", isolation);
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

K2PgStatus PgGate_SetTransactionReadOnly(bool read_only){
  elog(DEBUG5, "PgGateAPI: PgGate_SetTransactionReadOnly %d", read_only);
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

K2PgStatus PgGate_SetTransactionDeferrable(bool deferrable){
  elog(DEBUG5, "PgGateAPI: PgGate_SetTransactionReadOnly %d", deferrable);
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

K2PgStatus PgGate_EnterSeparateDdlTxnMode(){
  elog(DEBUG5, "PgGateAPI: PgGate_EnterSeparateDdlTxnMode");
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

K2PgStatus PgGate_ExitSeparateDdlTxnMode(bool success){
  elog(DEBUG5, "PgGateAPI: PgGate_ExitSeparateDdlTxnMode");
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

//--------------------------------------------------------------------------------------------------
// Expressions.

// Column references.
K2PgStatus PgGate_NewColumnRef(K2PgStatement stmt, int attr_num, const K2PgTypeEntity *type_entity,
                            const K2PgTypeAttrs *type_attrs, K2PgExpr *expr_handle){
  elog(DEBUG5, "PgGateAPI: PgGate_NewColumnRef %d", attr_num);
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

// Constant expressions.
K2PgStatus PgGate_NewConstant(K2PgStatement stmt, const K2PgTypeEntity *type_entity,
                           uint64_t datum, bool is_null, K2PgExpr *expr_handle){
  elog(DEBUG5, "PgGateAPI: PgGate_NewConstant %ld, %d", datum, is_null);
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

K2PgStatus PgGate_NewConstantOp(K2PgStatement stmt, const K2PgTypeEntity *type_entity,
                           uint64_t datum, bool is_null, K2PgExpr *expr_handle, bool is_gt){
  elog(DEBUG5, "PgGateAPI: PgGate_NewConstantOp %lu, %d, %d", datum, is_null, is_gt);
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

// The following update functions only work for constants.
// Overwriting the constant expression with new value.
K2PgStatus PgGate_UpdateConstInt2(K2PgExpr expr, int16_t value, bool is_null){
  elog(DEBUG5, "PgGateAPI: PgGate_UpdateConstInt2 %d, %d", value, is_null);
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

K2PgStatus PgGate_UpdateConstInt4(K2PgExpr expr, int32_t value, bool is_null){
  elog(DEBUG5, "PgGateAPI: PgGate_UpdateConstInt4 %d, %d", value, is_null);
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

K2PgStatus PgGate_UpdateConstInt8(K2PgExpr expr, int64_t value, bool is_null){
  elog(DEBUG5, "PgGateAPI: PgGate_UpdateConstInt8 %ld, %d", value, is_null);
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

K2PgStatus PgGate_UpdateConstFloat4(K2PgExpr expr, float value, bool is_null){
  elog(DEBUG5, "PgGateAPI: PgGate_UpdateConstFloat4 %f, %d", value, is_null);
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

K2PgStatus PgGate_UpdateConstFloat8(K2PgExpr expr, double value, bool is_null){
  elog(DEBUG5, "PgGateAPI: PgGate_UpdateConstFloat8 %f, %d", value, is_null);
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

K2PgStatus PgGate_UpdateConstText(K2PgExpr expr, const char *value, bool is_null){
  elog(DEBUG5, "PgGateAPI: PgGate_UpdateConstText %s, %d", value, is_null);
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

K2PgStatus PgGate_UpdateConstChar(K2PgExpr expr, const char *value, int64_t bytes, bool is_null){
  elog(DEBUG5, "PgGateAPI: PgGate_UpdateConstChar %s, %ld, %d", value, bytes, is_null);
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

// Expressions with operators "=", "+", "between", "in", ...
K2PgStatus PgGate_NewOperator(K2PgStatement stmt, const char *opname,
                           const K2PgTypeEntity *type_entity,
                           K2PgExpr *op_handle){
  elog(DEBUG5, "PgGateAPI: PgGate_NewOperator %s", opname);
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

K2PgStatus PgGate_OperatorAppendArg(K2PgExpr op_handle, K2PgExpr arg){
  elog(DEBUG5, "PgGateAPI: PgGate_OperatorAppendArg");
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

// Referential Integrity Check Caching.
// Check if foreign key reference exists in cache.
bool PgGate_ForeignKeyReferenceExists(K2PgOid table_oid, const char* k2pgctid, int64_t k2pgctid_size) {
  elog(DEBUG5, "PgGateAPI: PgGate_ForeignKeyReferenceExists %d, %s, %ld", table_oid, k2pgctid, k2pgctid_size);
  false;
}

// Add an entry to foreign key reference cache.
K2PgStatus PgGate_CacheForeignKeyReference(K2PgOid table_oid, const char* k2pgctid, int64_t k2pgctid_size){
  elog(DEBUG5, "PgGateAPI: PgGate_CacheForeignKeyReference %d, %s, %ld", table_oid, k2pgctid, k2pgctid_size);
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

// Delete an entry from foreign key reference cache.
K2PgStatus PgGate_DeleteFromForeignKeyReferenceCache(K2PgOid table_oid, uint64_t k2pgctid){
  elog(DEBUG5, "PgGateAPI: PgGate_DeleteFromForeignKeyReferenceCache %d, %lu", table_oid, k2pgctid);
  K2PgStatus status {
      .pg_code = ERRCODE_FDW_OPERATION_NOT_SUPPORTED,
      .k2_code = 501,
      .msg = "Not implemented",
      .detail = ""
  };
  
  return status;
}

void PgGate_ClearForeignKeyReferenceCache() {
  elog(DEBUG5, "PgGateAPI: PgGate_ClearForeignKeyReferenceCache");
}

bool PgGate_IsInitDbModeEnvVarSet() {
  elog(DEBUG5, "PgGateAPI: PgGate_IsInitDbModeEnvVarSet");
  return false;
}

// This is called by initdb. Used to customize some behavior.
void PgGate_InitFlags() {
  elog(DEBUG5, "PgGateAPI: PgGate_InitFlags");
}

// Retrieves value of psql_max_read_restart_attempts gflag
int32_t PgGate_GetMaxReadRestartAttempts() {
  elog(DEBUG5, "PgGateAPI: PgGate_GetMaxReadRestartAttempts");
  return default_max_read_restart_attempts;
}

// Retrieves value of psql_output_buffer_size gflag
int32_t PgGate_GetOutputBufferSize() {
  elog(DEBUG5, "PgGateAPI: PgGate_GetOutputBufferSize");
  return default_output_buffer_size;
}

// Retrieve value of psql_disable_index_backfill gflag.
bool PgGate_GetDisableIndexBackfill() {
  elog(DEBUG5, "PgGateAPI: PgGate_GetDisableIndexBackfill");
  return default_disable_index_backfill;
}

bool PgGate_IsK2PgEnabled() {
//  return api_impl != nullptr;
  return true;
}

// Sets the specified timeout in the rpc service.
void PgGate_SetTimeout(int timeout_ms, void* extra) {
  elog(DEBUG5, "PgGateAPI: PgGate_SetTimeout %d", timeout_ms);
  if (timeout_ms <= 0) {
    return;
  }
  timeout_ms = std::min(timeout_ms, default_client_read_write_timeout_ms);
}

//--------------------------------------------------------------------------------------------------
// Thread-Local variables.

void* PgGate_GetThreadLocalCurrentMemoryContext() {
  elog(DEBUG5, "PgGateAPI: PgGate_GetThreadLocalCurrentMemoryContext");
  return PgGetThreadLocalCurrentMemoryContext();
}

void* PgGate_SetThreadLocalCurrentMemoryContext(void *memctx) {
  elog(DEBUG5, "PgGateAPI: PgGate_SetThreadLocalCurrentMemoryContext");
  return PgSetThreadLocalCurrentMemoryContext(memctx);
}

void PgGate_ResetCurrentMemCtxThreadLocalVars() {
  elog(DEBUG5, "PgGateAPI: PgGate_ResetCurrentMemCtxThreadLocalVars");
  PgResetCurrentMemCtxThreadLocalVars();
}

void* PgGate_GetThreadLocalStrTokPtr() {
  elog(DEBUG5, "PgGateAPI: PgGate_GetThreadLocalStrTokPtr");
  return PgGetThreadLocalStrTokPtr();
}

void PgGate_SetThreadLocalStrTokPtr(char *new_pg_strtok_ptr) {
  elog(DEBUG5, "PgGateAPI: PgGate_SetThreadLocalStrTokPtr %s", new_pg_strtok_ptr);
  PgSetThreadLocalStrTokPtr(new_pg_strtok_ptr);
}

void* PgGate_SetThreadLocalJumpBuffer(void* new_buffer) {
  elog(DEBUG5, "PgGateAPI: PgGate_SetThreadLocalJumpBuffer");
  return PgSetThreadLocalJumpBuffer(new_buffer);
}

void* PgGate_GetThreadLocalJumpBuffer() {
  elog(DEBUG5, "PgGateAPI: PgGate_GetThreadLocalJumpBuffer");
  return PgGetThreadLocalJumpBuffer();
}

void PgGate_SetThreadLocalErrMsg(const void* new_msg) {
  elog(DEBUG5, "PgGateAPI: PgGate_SetThreadLocalErrMsg %s", static_cast<const char*>(new_msg));
  PgSetThreadLocalErrMsg(new_msg);
}

const void* PgGate_GetThreadLocalErrMsg() {
  elog(DEBUG5, "PgGateAPI: PgGate_GetThreadLocalErrMsg");
  return PgGetThreadLocalErrMsg();
}

const K2PgTypeEntity *K2PgFindTypeEntity(int type_oid) {
  elog(DEBUG5, "PgGateAPI: K2PgFindTypeEntity %d", type_oid);
//  return api_impl->FindTypeEntity(type_oid);
  return nullptr;
}

K2PgDataType K2PgGetType(const K2PgTypeEntity *type_entity) {
  elog(DEBUG5, "PgGateAPI: K2PgGetType");
  if (type_entity) {
    return type_entity->k2pg_type;
  }
  return K2SQL_DATA_TYPE_UNKNOWN_DATA;
}

bool K2PgAllowForPrimaryKey(const K2PgTypeEntity *type_entity) {
  elog(DEBUG5, "PgGateAPI: K2PgAllowForPrimaryKey");
  if (type_entity) {
    return type_entity->allow_for_primary_key;
  }
  return false;
}

void K2PgAssignTransactionPriorityLowerBound(double newval, void* extra) {
  elog(DEBUG5, "PgGateAPI: K2PgAssignTransactionPriorityLowerBound %f", newval);
}

void K2PgAssignTransactionPriorityUpperBound(double newval, void* extra) {
  elog(DEBUG5, "PgGateAPI: K2PgAssignTransactionPriorityUpperBound %f", newval);
}

// the following APIs are called by pg_dump.c only
// TODO: check if we really need to implement them

K2PgStatus PgGate_InitPgGateBackend() {
  K2PgStatus status {
      .pg_code = ERRCODE_SUCCESSFUL_COMPLETION,
      .k2_code = 200,
      .msg = "OK",
      .detail = "InitPgGateBackend OK"
  };
  
  return status;
}

void PgGate_ShutdownPgGateBackend() {
}

} // extern "C"

}  // namespace gate
}  // namespace k2pg
