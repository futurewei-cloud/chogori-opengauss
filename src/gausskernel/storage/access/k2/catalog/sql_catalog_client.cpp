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

#include "sql_catalog_client.h"

namespace k2pg {
namespace catalog {

sh::Status SqlCatalogClient::IsInitDbDone(bool* isDone) {
  auto start = k2::Clock::now();
  auto [status, done] = catalog_manager_->IsInitDbDone();
  K2LOG_D(log::catalog, "IsInitDbDone took {}", k2::Clock::now() - start);
  if(!status.is2xxOK()) {
    return status;
  }
  *isDone = done;
  return status;
}

sh::Status SqlCatalogClient::InitPrimaryCluster() {
  return catalog_manager_->InitPrimaryCluster();
}

sh::Status SqlCatalogClient::FinishInitDB() {
  return catalog_manager_->FinishInitDB();
}

sh::Status SqlCatalogClient::CreateDatabase(const std::string& database_name,
                                 const std::string& database_id,
                                 uint32_t database_oid,
                                 const std::string& source_database_id,
                                 const std::string& creator_role_name,
                                 const std::optional<uint32_t>& next_pg_oid) {
  CreateDatabaseRequest request {
    .databaseName = database_name,
    .databaseId = database_id,
    .databaseOid = database_oid,
    .sourceDatabaseId = source_database_id,
    .creatorRoleName = creator_role_name,
    .nextPgOid = next_pg_oid
  };
  auto start = k2::Clock::now();
  auto result = catalog_manager_->CreateDatabase(request);
  K2LOG_D(log::catalog, "CreateDatabase {} took {}", database_name, k2::Clock::now() - start);
  return std::get<0>(result);
}

sh::Status SqlCatalogClient::DeleteDatabase(const std::string& database_name, const std::string& database_id) {
  return catalog_manager_->DeleteDatabase(database_name, database_id);
}

sh::Status SqlCatalogClient::UseDatabase(const std::string& database_name) {
  auto start = k2::Clock::now();
  auto status = catalog_manager_->UseDatabase(database_name);
  K2LOG_D(log::catalog, "UseDatabase {} took {}", database_name, k2::Clock::now() - start);
  return status;
}

sh::Status SqlCatalogClient::CreateTable(
    const std::string& database_name,
    const std::string& table_name,
    const PgObjectId& table_object_id,
    PgSchema& schema,
    bool is_pg_catalog_table,
    bool is_shared_table,
    bool if_not_exist) {
  CreateTableRequest request {
    .databaseName = database_name,
    .databaseOid = table_object_id.GetDatabaseOid(),
    .tableName = table_name,
    .tableOid = table_object_id.GetObjectOid(),
    .schema = schema,
    .isSysCatalogTable = is_pg_catalog_table,
    .isSharedTable = is_shared_table,
    .isNotExist = if_not_exist
  };
  auto start = k2::Clock::now();
  auto response = catalog_manager_->CreateTable(request);
  K2LOG_D(log::catalog, "CreateTable {} in {} took {}", table_name, database_name, k2::Clock::now() - start);
  return std::get<0>(response);
}

sh::Status SqlCatalogClient::CreateIndexTable(
    const std::string& database_name,
    const std::string& table_name,
    const PgObjectId& table_object_id,
    const PgObjectId& base_table_object_id,
    PgSchema& schema,
    bool is_unique_index,
    bool skip_index_backfill,
    bool is_pg_catalog_table,
    bool is_shared_table,
    bool if_not_exist) {
  CreateIndexTableRequest request {
    .databaseName = database_name,
    .databaseOid = table_object_id.GetDatabaseOid(),
    .tableName = table_name,
    .tableOid = table_object_id.GetObjectOid(),
    // index and the base table should be in the same database, i.e., database
    .baseTableOid = base_table_object_id.GetObjectOid(),
    .schema = schema,
    .isUnique = is_unique_index,
    .skipIndexBackfill = skip_index_backfill,
    .isSysCatalogTable = is_pg_catalog_table,
    .isSharedTable = is_shared_table,
    .isNotExist = if_not_exist
  };
  auto start = k2::Clock::now();
  auto response = catalog_manager_->CreateIndexTable(request);
  K2LOG_D(log::catalog, "CreateIndexTable {} in {} took {}", table_name, database_name, k2::Clock::now() - start);
  return std::get<0>(response);
}

sh::Status SqlCatalogClient::DeleteTable(const PgOid database_oid, const PgOid table_oid, bool wait) {
  auto response = catalog_manager_->DeleteTable(database_oid, table_oid);
  return std::get<0>(response);
}

sh::Status SqlCatalogClient::DeleteIndexTable(const PgOid database_oid, const PgOid table_oid, PgOid *base_table_oid, bool wait) {
  auto [status, response] = catalog_manager_->DeleteIndex(database_oid, table_oid);
  if (!status.is2xxOK()) {
     return status;
  }
  *base_table_oid = response.baseIndexTableOid;
  // TODO: add wait logic once we refactor the catalog manager APIs to be asynchronous for state/response
  return status;
}

sh::Status SqlCatalogClient::OpenTable(const PgOid database_oid, const PgOid table_oid, std::shared_ptr<TableInfo>* table) {
  auto start = k2::Clock::now();
  auto [status, tableInfo] = catalog_manager_->GetTableSchema(database_oid, table_oid);
  K2LOG_D(log::catalog, "GetTableSchema ({} : {}) took {}", database_oid, table_oid, k2::Clock::now() - start);
  if (!status.is2xxOK()) {
     return status;
  }

  table->swap(tableInfo);
  return status;
}

sh::Status SqlCatalogClient::ReservePgOids(const PgOid database_oid,
                                  const uint32_t next_oid,
                                  const uint32_t count,
                                  uint32_t* begin_oid,
                                  uint32_t* end_oid) {
    auto databaseId = PgObjectId::GetDatabaseUuid(database_oid);
    auto start = k2::Clock::now();
    auto [status, response] = catalog_manager_->ReservePgOid(databaseId, next_oid, count);
    K2LOG_D(log::catalog, "ReservePgOid took {}", k2::Clock::now() - start);
    if (!status.is2xxOK()) {
        return status;
    }
    *begin_oid = response.beginOid;
    *end_oid = response.endOid;
    return status;
}

sh::Status SqlCatalogClient::GetCatalogVersion(uint64_t *pg_catalog_version) {
  auto start = k2::Clock::now();
  *pg_catalog_version = catalog_manager_->GetCatalogVersion();
  K2LOG_D(log::catalog, "GetCatalogVersion took {}", k2::Clock::now() - start);
  return sh::Statuses::S200_OK;
}

sh::Status SqlCatalogClient::IncrementCatalogVersion() {
  auto start = k2::Clock::now();
  auto result  = catalog_manager_->IncrementCatalogVersion();
  K2LOG_D(log::catalog, "IncrementCatalogVersion took {}", k2::Clock::now() - start);
  return std::get<0>(result);
}
} // namespace catalog
}  // namespace k2pg
