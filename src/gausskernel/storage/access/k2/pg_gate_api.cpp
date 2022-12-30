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

// When we mix certain C++ standard lib code and pg code there seems to be a macro conflict that
// will cause compiler errors in libintl.h. Including as the first thing fixes this.
#include <libintl.h>
#include <unordered_map>
#include <assert.h>
#include <atomic>
#include <memory>

#include "access/k2/pg_session.h"
#include "access/k2/pg_gate_api.h"
#include "access/k2/pg_memctx.h"
#include "access/k2/pg_ids.h"
#include "access/k2/k2_types.h"

#include "k2pg-internal.h"
#include "config.h"
#include "session.h"
#include "access/sysattr.h"
#include "access/k2/k2_util.h"
#include "access/k2/storage.h"
#include "access/k2/k2pg_util.h"

#include "utils/elog.h"
#include "utils/errcodes.h"
#include "pg_gate_defaults.h"
#include "pg_gate_thread_local.h"
#include "catalog/sql_catalog_client.h"
#include "catalog/sql_catalog_manager.h"

using namespace k2pg::gate;
namespace {

    class PgGate {
        public:
        PgGate() {
            catalog_manager_ = std::make_shared<k2pg::catalog::SqlCatalogManager>();
            catalog_manager_->Start();
        };

        ~PgGate() {
              catalog_manager_->Shutdown();
        };

        std::shared_ptr<k2pg::catalog::SqlCatalogClient> GetCatalogClient() {
              return k2pg::pg_session->GetCatalogClient();
        };

        std::shared_ptr<k2pg::catalog::SqlCatalogManager> GetCatalogManager() {
              return catalog_manager_;
        };

        private:
        std::shared_ptr<k2pg::catalog::SqlCatalogManager> catalog_manager_;

    };

    // use anonymous namespace to define a static variable that is not exposed outside of this file
    std::shared_ptr<PgGate> pg_gate;
    std::atomic<bool> pg_gate_initialized{false};
} // anonymous namespace

void PgGate_InitPgGate() {
    if (!pg_gate_initialized) {
        pg_gate_initialized.exchange(true);
        assert(pg_gate == nullptr && "PgGate should only be initialized once");
        elog(INFO, "K2 PgGate open");
        pg_gate = std::make_shared<PgGate>();
    } else {
        elog(INFO, "K2 PgGate has already been initialized");
    }
}

void PgGate_DestroyPgGate() {
    if (pg_gate_initialized) {
        if (pg_gate == nullptr) {
            elog(ERROR, "PgGate is destroyed or not initialized");
        } else {
            // no need to destroy pg_gate shared pointer since it won't be exit until the end of the process
            elog(INFO, "K2 PgGate destroyed");
        }
    }
}

// Initialize a session to process statements that come from the same client connection.
K2PgStatus PgGate_InitSession(const char *database_name) {
    elog(LOG, "PgGateAPI: PgGate_InitSession %s", database_name);
    assert(pg_gate != nullptr && "PgGate must be initialized");

    k2pg::TXMgr.endTxn(skv::http::dto::EndAction::Abort).get();

    std::shared_ptr<k2pg::catalog::SqlCatalogClient> catalog_client = std::make_shared<k2pg::catalog::SqlCatalogClient>(pg_gate->GetCatalogManager());
    k2pg::pg_session = std::make_shared<k2pg::PgSession>(catalog_client, database_name);
    return K2PgStatus::OK;
}

// Initialize K2PgMemCtx.
// - Postgres uses memory context to hold all of its allocated space. Once all associated operations
//   are done, the context is destroyed.
// - There K2PG objects are bound to Postgres operations. All of these objects' allocated
//   memory will be held by K2PgMemCtx, whose handle belongs to Postgres MemoryContext. Once all
//   Postgres operations are done, associated K2PG memory context (K2PgMemCtx) will be
//   destroyed together with Postgres memory context.
K2PgMemctx PgGate_CreateMemctx() {
    elog(DEBUG5, "PgGateAPI: PgGate_CreateMemctx");
    // Postgres will create PG Memctx when it first use the Memctx to allocate K2PG object.
    return k2pg::PgMemctx::Create();
}

K2PgStatus PgGate_DestroyMemctx(K2PgMemctx memctx) {
    elog(DEBUG5, "PgGateAPI: PgGate_DestroyMemctx");
    // Postgres will destroy PG Memctx by releasing the pointer.
    return k2pg::PgMemctx::Destroy(memctx);
}

K2PgStatus PgGate_ResetMemctx(K2PgMemctx memctx) {
    elog(DEBUG5, "PgGateAPI: PgGate_ResetMemctx");
    // Postgres reset PG Memctx when clearing a context content without clearing its nested context.
    return k2pg::PgMemctx::Reset(memctx);
}

// Invalidate the sessions table cache.
K2PgStatus PgGate_InvalidateCache() {
    elog(DEBUG5, "PgGateAPI: PgGate_InvalidateCache");
    k2pg::pg_session->InvalidateCache();
    return K2PgStatus::OK;
}

// Check if initdb has been already run.
K2PgStatus PgGate_IsInitDbDone(bool* initdb_done) {
    elog(DEBUG5, "PgGateAPI: PgGate_IsInitDbDone");
    return pg_gate->GetCatalogClient()->IsInitDbDone(initdb_done);
}

// Sets catalog_version to the local tserver's catalog version stored in shared
// memory, or an error if the shared memory has not been initialized (e.g. in initdb).
K2PgStatus PgGate_GetSharedCatalogVersion(uint64_t* catalog_version) {
    elog(DEBUG5, "PgGateAPI: PgGate_GetSharedCatalogVersion");
    return pg_gate->GetCatalogClient()->GetCatalogVersion(catalog_version);
}

//--------------------------------------------------------------------------------------------------
// DDL Statements
//--------------------------------------------------------------------------------------------------

// K2 InitPrimaryCluster
K2PgStatus PgGate_InitPrimaryCluster() {
    elog(LOG, "PgGateAPI: PgGate_InitPrimaryCluster");
    return pg_gate->GetCatalogClient()->InitPrimaryCluster();
}

K2PgStatus PgGate_FinishInitDB() {
    elog(LOG, "PgGateAPI: PgGate_FinishInitDB()");
    return pg_gate->GetCatalogClient()->FinishInitDB();
}

// DATABASE ----------------------------------------------------------------------------------------
// Connect database. Switch the connected database to the given "database_name".
K2PgStatus PgGate_ConnectDatabase(const char *database_name) {
    elog(LOG, "PgGateAPI: PgGate_ConnectDatabase %s", database_name);
    return k2pg::pg_session->ConnectDatabase(database_name);
}

// Create database.
K2PgStatus PgGate_ExecCreateDatabase(const char *database_name,
                                 K2PgOid database_oid,
                                 K2PgOid source_database_oid,
                                 K2PgOid next_oid) {
  elog(LOG, "PgGateAPI: PgGate_ExecCreateDatabase %s, %d, %d, %d",
         database_name, database_oid, source_database_oid, next_oid);
  return pg_gate->GetCatalogClient()->CreateDatabase(database_name,
      k2pg::PgObjectId::GetDatabaseUuid(database_oid),
      database_oid,
      source_database_oid != k2pg::kPgInvalidOid ? k2pg::PgObjectId::GetDatabaseUuid(source_database_oid) : "",
      "" /* creator_role_name */, next_oid);
}

// Drop database.
K2PgStatus PgGate_ExecDropDatabase(const char *database_name,
                                   K2PgOid database_oid) {
    elog(LOG, "PgGateAPI: PgGate_ExecDropDatabase %s, %d", database_name, database_oid);
    return pg_gate->GetCatalogClient()->DeleteDatabase(database_name,
        k2pg::PgObjectId::GetDatabaseUuid(database_oid));
}

// Alter database.
K2PgStatus PgGate_NewAlterDatabase(const char *database_name,
                               K2PgOid database_oid,
                               K2PgStatement *handle) {
  elog(LOG, "PgGateAPI: PgGate_NewAlterDatabase %s, %d", database_name, database_oid);
  return K2PgStatus::NotSupported;
}

K2PgStatus PgGate_AlterDatabaseRenameDatabase(K2PgStatement handle, const char *new_name) {
  elog(LOG, "PgGateAPI: PgGate_AlterDatabaseRenameDatabase %s", new_name);
  return K2PgStatus::NotSupported;
}

K2PgStatus PgGate_ExecAlterDatabase(K2PgStatement handle) {
  elog(LOG, "PgGateAPI: PgGate_ExecAlterDatabase");
  return K2PgStatus::NotSupported;
}

// Reserve oids.
K2PgStatus PgGate_ReserveOids(K2PgOid database_oid,
                           K2PgOid next_oid,
                           uint32_t count,
                           K2PgOid *begin_oid,
                           K2PgOid *end_oid) {
    elog(DEBUG5, "PgGateAPI: PgGate_ReserveOids %d, %d, %d", database_oid, next_oid, count);
    return pg_gate->GetCatalogClient()->ReservePgOids(database_oid, next_oid, count, begin_oid, end_oid);
}

K2PgStatus PgGate_GetCatalogMasterVersion(uint64_t *version) {
    elog(DEBUG5, "PgGateAPI: PgGate_GetCatalogMasterVersion");
    return pg_gate->GetCatalogClient()->GetCatalogVersion(version);
}

void PgGate_InvalidateTableCache(
    const K2PgOid database_oid,
    const K2PgOid table_oid) {
    elog(DEBUG5, "PgGateAPI: PgGate_InvalidateTableCache %d, %d", database_oid, table_oid);
    const k2pg::PgObjectId table_object_id(database_oid, table_oid);
    k2pg::pg_session->InvalidateTableCache(table_object_id);
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
    std::string table_uuid_str = table_uuid;
    const k2pg::PgObjectId table_object_id(table_uuid_str);
    k2pg::pg_session->InvalidateTableCache(table_object_id);
    return K2PgStatus::OK;
}

// Make ColumnSchema from column information
k2pg::ColumnSchema makeColumn(const std::string& col_name, int order, int type_oid, int attr_size, bool attr_byvalue, bool is_key, bool is_desc, bool is_nulls_first) {
    using SortingType = k2pg::ColumnSchema::SortingType;
    SortingType sorting_type = SortingType::kNotSpecified;
    if (is_key) {
        if (is_desc) {
            sorting_type = is_nulls_first ? SortingType::kDescending : SortingType::kDescendingNullsLast;
        } else {
            sorting_type = is_nulls_first ? SortingType::kAscending : SortingType::kAscendingNullsLast;
        }
    }
    bool is_nullable = !is_key;
    return k2pg::ColumnSchema(col_name, type_oid, attr_size, attr_byvalue, is_nullable, is_key, order, sorting_type);
}

std::tuple<k2pg::Status, bool, k2pg::Schema> makeSchema(const std::string& schema_name, const std::vector<K2PGColumnDef>& columns, bool add_primary_key) {
    std::vector<k2pg::ColumnSchema> k2pgcols;
    std::vector<k2pg::ColumnId> colIds;
    int num_key_columns = 0;
    const bool is_pg_catalog_table = (schema_name == "pg_catalog") || (schema_name == "information_schema");

    // Add internal primary key column to a Postgres table without a user-specified primary key.
    if (add_primary_key) {
        // For regular user table, k2pgrowid should be a hash key because k2pgrowid is a random uuid.
        k2pgcols.push_back(makeColumn("k2pgrowid",
                static_cast<int32_t>(k2pg::PgSystemAttrNum::kPgRowId),
                BYTEAOID,
                -1, false /* attr by value */,
                true /* is_key */, false /* is_desc */, false /* is_nulls_first */));
        num_key_columns++;
    }
    // Add key columns at the beginning
    for (auto& col : columns) {
        if (!col.is_key) {
            continue;
        }
        num_key_columns++;
        k2pgcols.push_back(makeColumn(col.attr_name, col.attr_num, col.type_oid, col.attr_size, col.attr_byvalue, col.is_key, col.is_desc, col.is_nulls_first));
    }
    // Add data columns
    for (auto& col : columns) {
        if (col.is_key) {
            continue;
        }

        k2pgcols.push_back(makeColumn(col.attr_name, col.attr_num, col.type_oid, col.attr_size, col.attr_byvalue, col.is_key, col.is_desc, col.is_nulls_first));
    }
    // Get column ids
    for (size_t i=0; i < k2pgcols.size(); i++) {
        colIds.push_back(k2pg::ColumnId(i));
    }
    k2pg::Schema schema;
    auto status = schema.Reset(k2pgcols, colIds, num_key_columns);
    return std::make_tuple(std::move(status), is_pg_catalog_table, std::move(schema));
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
                              bool is_shared_table,
                              bool if_not_exist,
                              bool add_primary_key,
                              const std::vector<K2PGColumnDef>& columns) {
    elog(LOG, "PgGateAPI: PgGate_NewCreateTable %s, %s, %s, shared: %d, if_not_exist: %d, add_primary_key: %d", database_name, schema_name, table_name,
        is_shared_table, if_not_exist, add_primary_key);
    auto [status, is_pg_catalog_table, schema] = makeSchema(schema_name, columns, add_primary_key);
    if (!status.IsOK()) {
        return status;
    }
    const k2pg::PgObjectId table_object_id(database_oid, table_oid);
    return pg_gate->GetCatalogClient()->CreateTable(database_name, table_name, table_object_id, schema, is_pg_catalog_table, is_shared_table, if_not_exist);
}

K2PgStatus PgGate_NewAlterTable(K2PgOid database_oid,
                             K2PgOid table_oid,
                             K2PgStatement *handle){
  elog(LOG, "PgGateAPI: PgGate_NewAlterTable %d, %d", database_oid, table_oid);
  return K2PgStatus::NotSupported;
}

K2PgStatus PgGate_AlterTableAddColumn(K2PgStatement handle, const char *name, int order,
                                   int type_oid, bool is_not_null){
  elog(LOG, "PgGateAPI: PgGate_AlterTableAddColumn %s", name);
  return K2PgStatus::NotSupported;
}

K2PgStatus PgGate_AlterTableRenameColumn(K2PgStatement handle, const char *oldname,
                                      const char *newname){
  elog(LOG, "PgGateAPI: PgGate_AlterTableRenameColumn %s, %s", oldname, newname);
  return K2PgStatus::NotSupported;
}

K2PgStatus PgGate_AlterTableDropColumn(K2PgStatement handle, const char *name){
  elog(LOG, "PgGateAPI: PgGate_AlterTableDropColumn %s", name);
  return K2PgStatus::NotSupported;
}

K2PgStatus PgGate_AlterTableRenameTable(K2PgStatement handle, const char *db_name,
                                     const char *newname){
  elog(LOG, "PgGateAPI: PgGate_AlterTableRenameTable %s, %s", db_name, newname);
  return K2PgStatus::NotSupported;
}

K2PgStatus PgGate_ExecAlterTable(K2PgStatement handle){
  elog(LOG, "PgGateAPI: PgGate_ExecAlterTable");
  return K2PgStatus::NotSupported;
}

K2PgStatus PgGate_ExecDropTable(K2PgOid database_oid,
                                K2PgOid table_oid,
                                bool if_exist){
  elog(LOG, "PgGateAPI: PgGate_ExecDropTable %d, %d", database_oid, table_oid);
  K2PgStatus status = pg_gate->GetCatalogClient()->DeleteTable(database_oid, table_oid);
  if (if_exist && status.k2_code == 404) {
      return K2PgStatus::OK;
  }
  return status;
}

K2PgStatus PgGate_GetTableDesc(K2PgOid database_oid,
                            K2PgOid table_oid,
                            K2PgTableDesc *handle) {
    elog(DEBUG5, "PgGateAPI: PgGate_GetTableDesc %d, %d", database_oid, table_oid);
    *handle = k2pg::pg_session->LoadTable(database_oid, table_oid).get();
    if (*handle == nullptr) {
        return K2PgStatus {
            .pg_code = ERRCODE_UNDEFINED_OBJECT,
            .k2_code = 404,
            .msg = "Table not found",
            .detail = ""
        };
    }
    return K2PgStatus::OK;
}

K2PgStatus PgGate_GetColumnInfo(K2PgTableDesc table_desc,
                             int16_t attr_number,
                             bool *is_primary) {
    elog(DEBUG5, "PgGateAPI: PgGate_GetColumnInfo %d", attr_number);
    return table_desc->GetColumnInfo(attr_number, is_primary);
}

K2PgStatus PgGate_SetIsSysCatalogVersionChange(K2PgStatement handle){
    elog(DEBUG5, "PgGateAPI: PgGate_SetIsSysCatalogVersionChange");
    // TODO: check if we don't need this
    handle->SetSysCatalogVersionChange();
    return pg_gate->GetCatalogClient()->IncrementCatalogVersion();
}

K2PgStatus PgGate_SetCatalogCacheVersion(K2PgStatement handle, uint64_t catalog_cache_version){
    elog(DEBUG5, "PgGateAPI: PgGate_SetCatalogCacheVersion %ld", catalog_cache_version);
    handle->SetCatalogCacheVersion(catalog_cache_version);
    return K2PgStatus::OK;
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
                              bool is_shared_index,
                              bool is_unique_index,
                              const bool skip_index_backfill,
                              bool if_not_exist,
                              std::vector<K2PGColumnDef>& columns){
    elog(LOG, "PgGateAPI: PgGate_NewCreateIndex %s, %s, %s, shared: %d, unique: %d", database_name, schema_name, index_name, is_shared_index, is_unique_index);
    // Add kPgUniqueIdxKeySuffix column to store key suffix for handling multiple NULL values in column
    // with unique index.
    // Value of this column is set to k2pgctid (same as k2pgbasectid) for index row in case index
    // is unique and at least one of its key column is NULL.
    // In all other case value of this column is NULL.
    if (is_unique_index) {
        K2PGColumnDef keysuffix_column {
            .attr_name = "k2pguniqueidxkeysuffix",
            .attr_num = k2pg::to_underlying(k2pg::PgSystemAttrNum::kPgUniqueIdxKeySuffix),
            .type_oid = BYTEAOID,
            .is_key = true,
            .is_desc = false,
            .is_nulls_first = columns[0].is_nulls_first // honor the settings in other keys
        };
        elog(LOG, "adding column k2pguniqueidxkeysuffix ... for index %s", index_name);
        columns.push_back(keysuffix_column);
    }

    K2PGColumnDef basectid_column {
        .attr_name = "k2pgidxbasectid",
        .attr_num = k2pg::to_underlying(k2pg::PgSystemAttrNum::kPgIdxBaseTupleId),
        .type_oid = BYTEAOID,
        .is_key = !is_unique_index,
        .is_desc = false,
        .is_nulls_first = columns[0].is_nulls_first // honor the settings in other keys
    };
    elog(LOG, "adding column k2pgidxbasectid ... for index %s", index_name);
    columns.push_back(basectid_column);

    auto [status, is_pg_catalog_table, schema] = makeSchema(schema_name, columns, false /* add_primary_key */);
    if (!status.IsOK()) {
        return status;
    }
    const k2pg::PgObjectId index_object_id(database_oid, index_oid);
    const k2pg::PgObjectId base_table_object_id(database_oid, table_oid);
    return pg_gate->GetCatalogClient()->CreateIndexTable(database_name, index_name, index_object_id, base_table_object_id, schema, is_unique_index, skip_index_backfill, is_pg_catalog_table, is_shared_index, if_not_exist);
}

K2PgStatus PgGate_NewDropIndex(K2PgOid database_oid,
                            K2PgOid index_oid,
                            bool if_exist,
                            K2PgStatement *handle){
  elog(LOG, "PgGateAPI: PgGate_NewDropIndex %d, %d", database_oid, index_oid);
  return K2PgStatus::NotSupported;
}

K2PgStatus PgGate_ExecDropIndex(K2PgStatement handle){
  elog(LOG, "PgGateAPI: PgGate_ExecDropIndex");
  return K2PgStatus::NotSupported;
}

//--------------------------------------------------------------------------------------------------
// DML statements (select, insert, update, delete, truncate)
//--------------------------------------------------------------------------------------------------
K2PgStatus PgGate_DmlFetch(K2PgScanHandle* handle, int32_t nattrs, uint64_t *values, bool *isnulls,
                        K2PgSysColumns *syscols, bool *has_data){
    elog(DEBUG5, "PgGateAPI: PgGate_DmlFetch handle: %p, nattrs: %d", handle, nattrs);

    *has_data = false;

    // First check if we need to wait for more records from our top-level query
    if (!handle->queryRecords.size() && handle->queryInFlight) {
        auto [status, resp] = handle->queryReq.get();
        handle->queryInFlight = false;
        if (!status.is2xxOK()) {
            return k2pg::K2StatusToK2PgStatus(std::move(status));
        }

        // Save the result records from the query
        for (skv::http::dto::SKVRecord::Storage& storage : resp.records) {
            std::shared_ptr<skv::http::dto::Schema> schema = handle->secondarySchema ? handle->secondarySchema : handle->primarySchema;
            skv::http::dto::SKVRecord record(handle->primaryTable->collection_name(), schema, std::move(storage));
            handle->queryRecords.push_back(std::move(record));
        }

        // Prefetch the next page in the query
        if (!resp.done) {
            handle->queryReq = k2pg::TXMgr.query(handle->query);
            handle->queryInFlight = true;
        }
    }

    // If we are doing a secondary index scan, try to keep a number of primary index read requests in flight
    if (handle->secondarySchema && handle->readReqs.size() < handle->maxParallelReads) {
        while (handle->readReqs.size() < handle->maxParallelReads && handle->queryRecords.size()) {
            try {
                skv::http::dto::SKVRecord readKey = makePrimaryKeyFromSecondary(handle->queryRecords.front(), handle->secondaryTable, handle->primarySchema);
                handle->queryRecords.pop_front();
                handle->readReqs.push_back(k2pg::TXMgr.read(std::move(readKey)));
            }
            catch (const std::exception& err) {
                K2PgStatus status {
                    .pg_code = ERRCODE_INTERNAL_ERROR,
                    .k2_code = 0,
                    .msg = "Error in makePrimaryKeyFromSecondary",
                    .detail = err.what()
                };

                return status;
            }
        }
    }

    if ((handle->secondarySchema && handle->readReqs.size() == 0) || (!handle->secondarySchema && handle->queryRecords.size() == 0)) {
        // No results left
        return K2PgStatus::OK;
    }

    skv::http::dto::SKVRecord resultRecord{};

    // Get one record from the result set, either from the read requests (for secondary index scan) or from the query results (for primary scan)
    if (handle->secondarySchema) {
        auto [status, resp] = handle->readReqs.front().get();
        handle->readReqs.pop_front();
        if (!status.is2xxOK()) {
            return k2pg::K2StatusToK2PgStatus(std::move(status));
        }
        resultRecord = std::move(resp);
    } else {
        resultRecord = std::move(handle->queryRecords.front());
        handle->queryRecords.pop_front();
    }

    // Last call helper to actually populate output result
    K2PgStatus status = populateDatumsFromSKVRecord(resultRecord, handle->primaryTable, nattrs, values, isnulls, syscols);
    if (status.IsOK()) {
        *has_data = true;
    }

    return status;
}

// This function returns the tuple id (k2pgctid) of a Postgres tuple.
K2PgStatus PgGate_DmlBuildPgTupleId(Oid db_oid, Oid table_oid, const std::vector<K2PgAttributeDef>& attrs,
                                    uint64_t *k2pgctid){
    elog(DEBUG5, "PgGateAPI: PgGate_DmlBuildPgTupleId db: %d, table %d, attr size: %lu", db_oid, table_oid, attrs.size());

    skv::http::dto::SKVRecord fullRecord;
    std::shared_ptr<k2pg::PgTableDesc> pg_table = k2pg::pg_session->LoadTable(db_oid, table_oid);
    K2PgStatus status = makeSKVRecordFromK2PgAttributes(db_oid, table_oid, attrs, fullRecord, pg_table);
    if (status.pg_code != ERRCODE_SUCCESSFUL_COMPLETION) {
        return status;
    }
    skv::http::dto::SKVRecord record = fullRecord.getSKVKeyRecord();


    // TODO can we remove some of the copies being done?
    skv::http::MPackWriter _writer;
    skv::http::Binary serializedStorage;
    _writer.write(record.getStorage());
    bool flushResult = _writer.flush(serializedStorage);
    if (!flushResult) {
        K2PgStatus err {
            .pg_code = ERRCODE_INTERNAL_ERROR,
            .k2_code = 0,
            .msg = "Serialization error in _writer flush",
            .detail = ""
        };
        return err;
    }

    // This comes from cstring_to_text_with_len which is used to create a proper datum
    // that is prepended with the data length. Doing it by hand here to avoid the extra copy
    char *datum = (char*)palloc(serializedStorage.size() + VARHDRSZ);
    SET_VARSIZE(datum, serializedStorage.size() + VARHDRSZ);
    memcpy(VARDATA(datum), serializedStorage.data(), serializedStorage.size());
    *k2pgctid = PointerGetDatum(datum);

    return K2PgStatus::OK;
}

// INSERT ------------------------------------------------------------------------------------------

K2PgStatus PgGate_ExecInsert(K2PgOid database_oid,
                             K2PgOid table_oid,
                             bool upsert,
                             bool increment_catalog,
                             std::vector<K2PgAttributeDef>& columns,
                             Datum* k2pgtupleid) {
    elog(DEBUG5, "PgGateAPI: PgGate_ExecInsert %d, %d", database_oid, table_oid);
    auto catalog = pg_gate->GetCatalogClient();

    skv::http::dto::SKVRecord record;
    std::shared_ptr<k2pg::PgTableDesc> pg_table = k2pg::pg_session->LoadTable(database_oid, table_oid);
    // check if the table has k2pgrowid system column
    if (pg_table->FindColumn(k2pg::to_underlying(k2pg::PgSystemAttrNum::kPgRowId)) != NULL) {
        // check if k2pgrowid has already been included in passed in columns
        bool kPgRowIdProvided = false;
        for (const auto& column: columns) {
            if (column.attr_num == k2pg::to_underlying(k2pg::PgSystemAttrNum::kPgRowId)) {
                kPgRowIdProvided = true;
                break;
            }
        }
        if (!kPgRowIdProvided) {
            // generate a row_id to populate the kPgRowId column
            std::string row_id = k2pg::pg_session->GenerateNewRowid();
            char* datum = (char*)(palloc0(row_id.size() + VARHDRSZ));
            memcpy(VARDATA(datum), row_id.data(), row_id.size());
            SET_VARSIZE(datum, row_id.size() + VARHDRSZ);
            K2PgAttributeDef kPgRowIdColumn {
                .attr_num = k2pg::to_underlying(k2pg::PgSystemAttrNum::kPgRowId),
                .value = {
                    .type_id = BYTEAOID,
                    .datum = PointerGetDatum(datum),
                    .is_null = false
                }
            };
            columns.push_back(std::move(kPgRowIdColumn));
        }
    }
    K2PgStatus status = makeSKVRecordFromK2PgAttributes(database_oid, table_oid, columns, record, pg_table);

    if (status.pg_code != ERRCODE_SUCCESSFUL_COMPLETION) {
        return status;
    }

    K2PgStatus tid_status = PgGate_DmlBuildPgTupleId(database_oid, table_oid, columns, k2pgtupleid);

    if (tid_status.pg_code != ERRCODE_SUCCESSFUL_COMPLETION) {
        return status;
    }

    for (auto& attribute : columns) {
        if (attribute.attr_num == K2PgTupleIdAttributeNumber && !attribute.value.is_null && attribute.value.datum == 0) {
            attribute.value.datum = *k2pgtupleid;
        }
    }

    auto [k2status] = k2pg::TXMgr.write(record, false, upsert ? skv::http::dto::ExistencePrecondition::None : skv::http::dto::ExistencePrecondition::NotExists).get();
    status = k2pg::K2StatusToK2PgStatus(std::move(k2status));
    if (status.pg_code != ERRCODE_SUCCESSFUL_COMPLETION) {
        return status;
    }

    if (increment_catalog) {
        status = catalog->IncrementCatalogVersion();
    }

    return status;
}

// UPDATE ------------------------------------------------------------------------------------------
K2PgStatus PgGate_ExecUpdate(K2PgOid database_oid,
                             K2PgOid table_oid,
                             bool increment_catalog,
                             int* rows_affected,
                             const std::vector<K2PgAttributeDef>& columns) {
    elog(DEBUG5, "PgGateAPI: PgGate_ExecUpdate %u, %u", database_oid, table_oid);

    auto catalog = pg_gate->GetCatalogClient();
    if (rows_affected) {
        *rows_affected = 0;
    }
    std::unique_ptr<skv::http::dto::SKVRecordBuilder> builder;

    // Get a builder with the keys serialzed. called function handles tupleId attribute if needed
    K2PgStatus status = makeSKVBuilderWithKeysSerialized(database_oid, table_oid, columns, builder);
    if (status.pg_code != ERRCODE_SUCCESSFUL_COMPLETION) {
        return status;
    }

    // Iterate through the passed attributes to determine which fields should be marked for update
    std::unordered_map<int, K2PgConstant> attr_map;
    std::vector<uint32_t> fieldsForUpdate;
    std::shared_ptr<k2pg::PgTableDesc> pg_table = k2pg::pg_session->LoadTable(database_oid, table_oid);
    for (const auto& column : columns) {
        k2pg::PgColumn *pg_column = pg_table->FindColumn(column.attr_num);
        if (pg_column == NULL) {
            K2PgStatus status {
                .pg_code = ERRCODE_INTERNAL_ERROR,
                .k2_code = 404,
                .msg = "Cannot find column with attr_num",
                .detail = "Load table failed"
            };
            return status;
        }
        // we have two extra fields, i.e., table_id and index_id, in skv key
        fieldsForUpdate.push_back(pg_column->index() + 2);
        attr_map[pg_column->index() + 2] = column.value;
    }

    // Serialize remaining non-key fields
    try {
        size_t offset = builder->getSchema()->partitionKeyFields.size();
        for (size_t i = offset; i < builder->getSchema()->fields.size(); ++i) {
            auto it = attr_map.find(i);
            if (it == attr_map.end()) {
                builder->serializeNull();
            } else {
                serializePGConstToK2SKV(*builder, it->second);
            }
        }
    }
    catch (const std::exception& err) {
        K2PgStatus status {
            .pg_code = ERRCODE_INTERNAL_ERROR,
            .k2_code = 0,
            .msg = "Serialization error in serializePgAttributesToSKV",
            .detail = err.what()
        };

        return status;
    }

    // Send the partialUpdate request to SKV
    skv::http::dto::SKVRecord record = builder->build();
    auto [k2status] = k2pg::TXMgr.partialUpdate(record, std::move(fieldsForUpdate)).get();
    if (!k2status.is2xxOK() && k2status.code != 412) { // 412 Precondition falied is not an error for PG in this case
        status = k2pg::K2StatusToK2PgStatus(std::move(k2status));
        return status;
    } else if (rows_affected && k2status.is2xxOK()) {
        *rows_affected = 1;
    }

    if (increment_catalog) {
        K2PgStatus catalog_status = catalog->IncrementCatalogVersion();
        if (!catalog_status.IsOK()) {
            return catalog_status;
        }
    }

    return K2PgStatus::OK;
}

// DELETE ------------------------------------------------------------------------------------------
K2PgStatus PgGate_ExecDelete(K2PgOid database_oid,
                             K2PgOid table_oid,
                             bool increment_catalog,
                             int* rows_affected,
                             const std::vector<K2PgAttributeDef>& columns) {
    elog(DEBUG5, "PgGateAPI: PgGate_ExecDelete %d, %d", database_oid, table_oid);

    auto catalog = pg_gate->GetCatalogClient();
    if (rows_affected) {
        *rows_affected = 0;
    }
    std::unique_ptr<skv::http::dto::SKVRecordBuilder> builder;

    // Get a builder with the keys serialized. called function handles tupleId attribute if needed
    K2PgStatus status = makeSKVBuilderWithKeysSerialized(database_oid, table_oid, columns, builder);
    if (status.pg_code != ERRCODE_SUCCESSFUL_COMPLETION) {
        return status;
    }

    // Serialize remaining non-key fields as null to create a valid SKVRecord
    try {
        size_t offset = builder->getSchema()->partitionKeyFields.size();
        for (size_t i = offset; i < builder->getSchema()->fields.size(); ++i) {
            builder->serializeNull();
        }
    }
    catch (const std::exception& err) {
        K2PgStatus status {
            .pg_code = ERRCODE_INTERNAL_ERROR,
            .k2_code = 0,
            .msg = "Serialization error in serializePgAttributesToSKV",
            .detail = err.what()
        };

        return status;
    }

    // Send the delete request to SKV
    skv::http::dto::SKVRecord record = builder->build();
    auto [k2status] = k2pg::TXMgr.write(record, true, skv::http::dto::ExistencePrecondition::Exists).get();
    if (!k2status.is2xxOK() && k2status.code != 412) { // 412 Precondition falied is not an error for PG in this case
        status = k2pg::K2StatusToK2PgStatus(std::move(k2status));
        return status;
    } else if (rows_affected && k2status.is2xxOK()) {
        *rows_affected = 1;
    }

    if (increment_catalog) {
        K2PgStatus catalog_status = catalog->IncrementCatalogVersion();
        if (!catalog_status.IsOK()) {
            return catalog_status;
        }
    }

    return K2PgStatus::OK;
}

// SELECT ------------------------------------------------------------------------------------------
K2PgStatus PgGate_NewSelect(K2PgOid database_oid,
                         K2PgOid table_oid,
                         K2PgSelectIndexParams idxp,
                         K2PgScanHandle **handle){
    elog(DEBUG5, "PgGateAPI: PgGate_NewSelect %d, %d", database_oid, table_oid);
    *handle = new K2PgScanHandle();
    GetCurrentK2Memctx()->Cache([ptr=*handle] () { delete ptr;});
    (*handle)->indexParams = std::move(idxp);
    (*handle)->maxParallelReads = k2pg::TXMgr.getConfig().get<uint32_t>("pggate.max_parallel_reads", 5);

    std::shared_ptr<k2pg::PgTableDesc> pg_table = k2pg::pg_session->LoadTable(database_oid, table_oid);
    if (pg_table == nullptr) {
        K2PgStatus status {
            .pg_code = ERRCODE_INTERNAL_ERROR,
            .k2_code = 404,
            .msg = "LoadTable failed",
            .detail = ""
        };
        return status;
    }

    (*handle)->primaryTable = pg_table;

    (*handle)->collectionName = pg_table->collection_name();
    const std::string& schemaName = pg_table->schema_name();

    auto [status, primarySchema] = k2pg::TXMgr.getSchema((*handle)->collectionName, schemaName).get();
    if (!status.is2xxOK()) {
        return k2pg::K2StatusToK2PgStatus(std::move(status));
    }
    (*handle)->primarySchema = primarySchema;

    if ((*handle)->indexParams.index_oid == kInvalidOid || (*handle)->indexParams.index_oid == table_oid) {
        return K2PgStatus::OK;
    }

    pg_table = k2pg::pg_session->LoadTable(database_oid, (*handle)->indexParams.index_oid);
    if (pg_table == nullptr) {
        K2PgStatus status {
            .pg_code = ERRCODE_INTERNAL_ERROR,
            .k2_code = 404,
            .msg = "LoadTable failed",
            .detail = ""
        };
        return status;
    }
    (*handle)->secondaryTable = pg_table;

    const std::string& secondarySchemaName = pg_table->schema_name();
    auto [secondaryStatus, secondarySchema] = k2pg::TXMgr.getSchema((*handle)->collectionName, secondarySchemaName).get();
    if (!secondaryStatus.is2xxOK()) {
        return k2pg::K2StatusToK2PgStatus(std::move(secondaryStatus));
    }
    (*handle)->secondarySchema = secondarySchema;

    if ((*handle)->indexParams.index_only_scan) {
        (*handle)->primaryTable = (*handle)->secondaryTable;
        (*handle)->secondaryTable = nullptr;
        (*handle)->primarySchema = (*handle)->secondarySchema;
        (*handle)->secondarySchema = nullptr;
    }

    return K2PgStatus::OK;
}

K2PgStatus PgGate_ExecSelect(
                K2PgScanHandle *handle,
                const std::vector<K2PgConstraintDef>& constraints,
                const std::vector<int>& targets_attrnum,
                bool forward_scan,
                const K2PgSelectLimitParams& limit_params) {
    using namespace skv::http::dto::expression;
    Expression range_conds{}, where_conds{};
    range_conds.op = Operation::AND;
    where_conds.op = Operation::AND;
    std::shared_ptr<k2pg::PgTableDesc> pg_table = handle->secondaryTable ? handle->secondaryTable : handle->primaryTable;
    elog(INFO, "PgGate_ExecSelect for table %s: %s with %ld constraints", pg_table->table_name().c_str(), pg_table->schema_name().c_str(), constraints.size());

    std::unordered_map<int, uint32_t> attr_to_offset;
    for (const auto& column : pg_table->columns()) {
        // we have two extra fields, i.e., table_id and index_id, in skv key
        attr_to_offset[column.attr_num()] = column.index() + 2;
    }

    std::shared_ptr<skv::http::dto::Schema> schema = handle->secondarySchema ? handle->secondarySchema : handle->primarySchema;
    for (const K2PgConstraintDef& constraint: constraints) {

        if (constraint.constraint == K2PG_CONSTRAINT_BETWEEN) {
            // Special case for BETWEEN since SKV does not have a 1:1 match
            K2PgConstraintDef gteConstraint = constraint;
            gteConstraint.constraint = K2PG_CONSTRAINT_GTE;
            gteConstraint.constants.pop_back();
            Expression gteExpr = buildScanExpr(handle, gteConstraint, attr_to_offset);
            K2PgConstraintDef lteConstraint = constraint;
            lteConstraint.constraint = K2PG_CONSTRAINT_LTE;
            lteConstraint.constants[0] = lteConstraint.constants[1];
            gteConstraint.constants.pop_back();
            Expression lteExpr = buildScanExpr(handle, lteConstraint, attr_to_offset);

            uint32_t offset = attr_to_offset[constraint.attr_num];
            if (offset < schema->partitionKeyFields.size()) {
                range_conds.expressionChildren.push_back(std::move(gteExpr));
                range_conds.expressionChildren.push_back(std::move(lteExpr));
            } else {
                where_conds.expressionChildren.push_back(std::move(gteExpr));
                where_conds.expressionChildren.push_back(std::move(lteExpr));
            }
        }
        else if (constraint.constraint == K2PG_CONSTRAINT_IN) {
            // Special case for IN since SKV does not have a 1:1 match
            std::vector<Expression> expr_to_add;
            for (const K2PgConstant& constant : constraint.constants) {
                K2PgConstraintDef eqConstraint {
                    .attr_num = constraint.attr_num,
                    .constraint = K2PG_CONSTRAINT_EQ,
                    .constants = std::vector<K2PgConstant>{constant}
                };
                Expression expr = buildScanExpr(handle, eqConstraint, attr_to_offset);
                expr_to_add.push_back(std::move(expr));
            }

            Expression or_expr{};
            or_expr.op = Operation::OR;
            or_expr.expressionChildren = std::move(expr_to_add);
            // Expressions with OR must always be where clause not range
            where_conds.expressionChildren.push_back(std::move(or_expr));
        }
        else {
            // Normal case of 1 constant expression that has 1:1 map to SKV expression
            Expression expr = buildScanExpr(handle, constraint, attr_to_offset);
            if (expr.op == Operation::UNKNOWN) {
                // Unsupported pg type or operation, it will be processed by pg and not pushed down
                continue;
            }

            uint32_t offset = attr_to_offset[constraint.attr_num];
            if (offset < schema->partitionKeyFields.size()) {
                range_conds.expressionChildren.push_back(std::move(expr));
            } else {
                where_conds.expressionChildren.push_back(std::move(expr));
            }
        }
    }

    uint32_t base_table_oid = pg_table->base_table_oid();
    uint32_t index_id = pg_table->index_oid();

    skv::http::dto::SKVRecordBuilder start(handle->collectionName, schema);
    skv::http::dto::SKVRecordBuilder end(handle->collectionName, schema);
    bool isRangeScan = false;

    try {
        start.serializeNext<int64_t>((int64_t)base_table_oid);
        end.serializeNext<int64_t>((int64_t)base_table_oid);
        start.serializeNext<int64_t>((int64_t)index_id);
        end.serializeNext<int64_t>((int64_t)index_id);

        BuildRangeRecords(range_conds, where_conds.expressionChildren, start, end, isRangeScan);
    }
    catch (const std::exception& err) {
        K2PgStatus status {
            .pg_code = ERRCODE_INTERNAL_ERROR,
            .k2_code = 0,
            .msg = "K2 Serialization error in ExecSelect",
            .detail = err.what()
        };

        return status;
    }

    // Check if we are trying to range scan an index that does not support it
    bool convertToFullScan = false;
    if (isRangeScan) {
        for (const K2PgConstraintDef& constraint: constraints) {
            if (constraint.constants.size() && !K2PgAllowForPrimaryKey(constraint.constants[0].type_id, constraint.constants[0].attr_size, constraint.constants[0].attr_byvalue)) {
                elog(WARNING, "Trying to range scan wih an unsupported type, converting to full scan");
                convertToFullScan = true;
                break;
            }
        }

        for(int i=0; i < schema->partitionKeyFields.size(); ++i) {
            if (schema->fields[i].descending) {
                elog(WARNING, "Trying to range scan wih a descending key, converting to full scan");
                convertToFullScan = true;
                break;
            }
        }
    }
    if (convertToFullScan) {
        start = skv::http::dto::SKVRecordBuilder(handle->collectionName, schema);
        end = skv::http::dto::SKVRecordBuilder(handle->collectionName, schema);

        try {
            start.serializeNext<int64_t>((int64_t)base_table_oid);
            end.serializeNext<int64_t>((int64_t)base_table_oid);
            start.serializeNext<int64_t>((int64_t)index_id);
            end.serializeNext<int64_t>((int64_t)index_id);
        }
        catch (const std::exception& err) {
            K2PgStatus status {
                .pg_code = ERRCODE_INTERNAL_ERROR,
                .k2_code = 0,
                .msg = "K2 Serialization error in ExecSelect",
                .detail = err.what()
            };

            return status;
        }
    }

    std::vector<std::string> projection;
    if (schema == handle->primarySchema) {
        std::unordered_set<std::string> projected;
        // Always project key fields so that we can construct a tupleid
        for (size_t i=0; i < schema->partitionKeyFields.size(); ++i) {
            projection.push_back(schema->fields[i].name);
            projected.insert(schema->fields[i].name);
        }

        for (int target_attr : targets_attrnum) {
            // Virtual column, not a valid target for k2 query
            if (target_attr == (int)k2pg::PgSystemAttrNum::kPgTupleId) {
                continue;
            }

            auto offset_it = attr_to_offset.find(target_attr);
            if (offset_it == attr_to_offset.end()) {
                K2PgStatus status {
                    .pg_code = ERRCODE_INTERNAL_ERROR,
                    .k2_code = 0,
                    .msg = "Unknown target_attr in target list",
                    .detail = ""
                };

                return status;
            }

            uint32_t offset = offset_it->second;
            if (projected.find(schema->fields[offset].name) == projected.end()) {
                projection.push_back(schema->fields[offset].name);
                projected.insert(schema->fields[offset].name);
            }
        }
    }

    int limit = -1;
    if (!limit_params.limit_use_default && limit_params.limit_count > 0) {
        limit = limit_params.limit_count + limit_params.limit_offset;
    }

    if (!where_conds.expressionChildren.size()) {
        where_conds = Expression();
    }

    auto [status, query] = k2pg::TXMgr.createQuery(start.build(), end.build(), std::move(where_conds), std::move(projection), limit, !forward_scan).get();
    if (!status.is2xxOK()) {
        K2LOG_ERT(k2log::k2pg, "error creating query: {}", status);
        return k2pg::K2StatusToK2PgStatus(std::move(status));
    }
    handle->query = query;
    // prefetch first page
    handle->queryReq = k2pg::TXMgr.query(handle->query);
    handle->queryInFlight = true;
    return K2PgStatus::OK;
}

// Transaction control -----------------------------------------------------------------------------

K2PgStatus PgGate_BeginTransaction(){
  elog(DEBUG5, "PgGateAPI: PgGate_BeginTransaction");
  return K2PgStatus::NotSupported;
}

K2PgStatus PgGate_RestartTransaction(){
  elog(DEBUG5, "PgGateAPI: PgGate_RestartTransaction");
  return K2PgStatus::NotSupported;
}

K2PgStatus PgGate_CommitTransaction(){
  elog(DEBUG5, "PgGateAPI: PgGate_CommitTransaction");
  auto [status] = k2pg::TXMgr.endTxn(skv::http::dto::EndAction::Commit).get();
  return k2pg::K2StatusToK2PgStatus(std::move(status));
}

K2PgStatus PgGate_AbortTransaction(){
  elog(DEBUG5, "PgGateAPI: PgGate_AbortTransaction");
  return K2PgStatus::NotSupported;
}

K2PgStatus PgGate_SetTransactionIsolationLevel(int isolation){
  elog(DEBUG5, "PgGateAPI: PgGate_SetTransactionIsolationLevel %d", isolation);
  return K2PgStatus::NotSupported;
}

K2PgStatus PgGate_SetTransactionReadOnly(bool read_only){
  elog(DEBUG5, "PgGateAPI: PgGate_SetTransactionReadOnly %d", read_only);
  return K2PgStatus::NotSupported;
}

K2PgStatus PgGate_SetTransactionDeferrable(bool deferrable){
  elog(DEBUG5, "PgGateAPI: PgGate_SetTransactionReadOnly %d", deferrable);
  return K2PgStatus::NotSupported;
}

K2PgStatus PgGate_EnterSeparateDdlTxnMode(){
  elog(DEBUG5, "PgGateAPI: PgGate_EnterSeparateDdlTxnMode");
  return K2PgStatus::NotSupported;
}

K2PgStatus PgGate_ExitSeparateDdlTxnMode(bool success){
  elog(DEBUG5, "PgGateAPI: PgGate_ExitSeparateDdlTxnMode");
  return K2PgStatus::NotSupported;
}

//--------------------------------------------------------------------------------------------------

// Referential Integrity Check Caching.
// Check if foreign key reference exists in cache.
bool PgGate_ForeignKeyReferenceExists(K2PgOid table_oid, const char* k2pgctid, int64_t k2pgctid_size) {
    elog(DEBUG5, "PgGateAPI: PgGate_ForeignKeyReferenceExists %d, %p, %ld", table_oid, k2pgctid, k2pgctid_size);
    return k2pg::pg_session->ForeignKeyReferenceExists(table_oid, std::string(k2pgctid, k2pgctid_size));
}

// Add an entry to foreign key reference cache.
K2PgStatus PgGate_CacheForeignKeyReference(K2PgOid table_oid, const char* k2pgctid, int64_t k2pgctid_size){
    elog(DEBUG5, "PgGateAPI: PgGate_CacheForeignKeyReference %d, %p, %ld", table_oid, k2pgctid, k2pgctid_size);
    return k2pg::pg_session->CacheForeignKeyReference(table_oid, std::string(k2pgctid, k2pgctid_size));
}

// Delete an entry from foreign key reference cache.
K2PgStatus PgGate_DeleteFromForeignKeyReferenceCache(K2PgOid table_oid, uint64_t k2pgctid){
    elog(DEBUG5, "PgGateAPI: PgGate_DeleteFromForeignKeyReferenceCache %d, %lu", table_oid, k2pgctid);
    k2pg::UntoastedDatum data = k2pg::UntoastedDatum(k2pgctid);
    int64_t bytes = VARSIZE(data.untoasted); // includes header len VARHDRSZ
    char *value = (char*)data.untoasted;
    return k2pg::pg_session->DeleteForeignKeyReference(table_oid, std::string(value, bytes));
}

void PgGate_ClearForeignKeyReferenceCache() {
    elog(DEBUG5, "PgGateAPI: PgGate_ClearForeignKeyReferenceCache");
    k2pg::pg_session->InvalidateForeignKeyReferenceCache();
}

bool PgGate_IsK2PgEnabled() {
    return pg_gate_initialized && pg_gate != nullptr;
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

bool K2PgAllowForPrimaryKey(int type_oid, int attr_size, bool attr_byvalue) {
    elog(DEBUG5, "PgGateAPI: K2PgAllowForPrimaryKey");
    skv::http::dto::FieldType skv_type = k2pg::OidToK2Type(type_oid, attr_size, attr_byvalue);
    switch (skv_type) {
        case skv::http::dto::FieldType::STRING:
        case skv::http::dto::FieldType::INT16T:
        case skv::http::dto::FieldType::INT32T:
        case skv::http::dto::FieldType::INT64T:
        case skv::http::dto::FieldType::BOOL:
            return k2pg::isPushdownType(type_oid, attr_size, attr_byvalue);
            break;
        default:
            return false;
    }

    return false;
}

void K2PgAssignTransactionPriorityLowerBound(double newval, void* extra) {
  elog(DEBUG5, "PgGateAPI: K2PgAssignTransactionPriorityLowerBound %f", newval);
}

void K2PgAssignTransactionPriorityUpperBound(double newval, void* extra) {
  elog(DEBUG5, "PgGateAPI: K2PgAssignTransactionPriorityUpperBound %f", newval);
}
