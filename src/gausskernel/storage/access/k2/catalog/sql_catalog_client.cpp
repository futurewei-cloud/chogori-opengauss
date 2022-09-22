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
#include "access/k2/k2_util.h"
#include "sql_catalog_client.h"
#include "sql_catalog_manager.h"
#include "access/k2/pg_tabledesc.h"

namespace k2pg {
namespace catalog {

// TODO: change to report time metrics from catalog manager
Status SqlCatalogClient::IsInitDbDone(bool* isDone) {
    auto [status, done] = catalog_manager_->IsInitDbDone();
    if (!status.is2xxOK()) {
        return k2pg::K2StatusToK2PgStatus(std::move(status));
    }
    *isDone = done;
    return k2pg::Status::OK;
}

Status SqlCatalogClient::InitPrimaryCluster() {
    return k2pg::K2StatusToK2PgStatus(catalog_manager_->InitPrimaryCluster());
}

Status SqlCatalogClient::FinishInitDB() {
    return k2pg::K2StatusToK2PgStatus(catalog_manager_->FinishInitDB());
}

Status SqlCatalogClient::CreateDatabase(const std::string& database_name,
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
    auto result = catalog_manager_->CreateDatabase(request);
    return k2pg::K2StatusToK2PgStatus(std::move(std::get<0>(result)));
}

Status SqlCatalogClient::DeleteDatabase(const std::string& database_name, const std::string& database_id) {
    return k2pg::K2StatusToK2PgStatus(catalog_manager_->DeleteDatabase(database_name, database_id));
}

Status SqlCatalogClient::UseDatabase(const std::string& database_name) {
    return k2pg::K2StatusToK2PgStatus(catalog_manager_->UseDatabase(database_name));
}

Status SqlCatalogClient::CreateTable(
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
    return k2pg::K2StatusToK2PgStatus(std::get<0>(catalog_manager_->CreateTable(request)));
}

Status SqlCatalogClient::CreateIndexTable(
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
    return k2pg::K2StatusToK2PgStatus(std::get<0>(catalog_manager_->CreateIndexTable(request)));
}

Status SqlCatalogClient::DeleteTable(const PgOid database_oid, const PgOid table_oid, bool wait) {
    auto response = catalog_manager_->DeleteTable(database_oid, table_oid);
    return k2pg::K2StatusToK2PgStatus(std::move(std::get<0>(response)));
}

Status SqlCatalogClient::DeleteIndexTable(const PgOid database_oid, const PgOid table_oid, PgOid *base_table_oid, bool wait) {
    auto [status, response] = catalog_manager_->DeleteIndex(database_oid, table_oid);
    if (!status.is2xxOK()) {
        return k2pg::K2StatusToK2PgStatus(std::move(status));
    }
    *base_table_oid = response.baseIndexTableOid;
    // TODO: add wait logic once we refactor the catalog manager APIs to be asynchronous for state/response
    return k2pg::Status::OK;
}

std::shared_ptr<TableInfo> SqlCatalogClient::OpenTable(const PgOid database_oid, const PgOid table_oid) {
    auto [status, tableInfo] = catalog_manager_->GetTableSchema(database_oid, table_oid);
    if (!status.is2xxOK()) {
        K2LOG_W(log::catalog, "Failed to get TableSchema {} : {} due to {}", database_oid, table_oid, status);
        return nullptr;
    }

    return tableInfo;
}

Status SqlCatalogClient::OpenTable(const PgOid database_oid, const PgOid table_oid, std::shared_ptr<TableInfo>* table) {
    auto [status, tableInfo] = catalog_manager_->GetTableSchema(database_oid, table_oid);
    if (!status.is2xxOK()) {
        return k2pg::K2StatusToK2PgStatus(std::move(status));
    }

    table->swap(tableInfo);
    return k2pg::Status::OK;
}

Status SqlCatalogClient::ReservePgOids(const PgOid database_oid,
                                  const uint32_t next_oid,
                                  const uint32_t count,
                                  uint32_t* begin_oid,
                                  uint32_t* end_oid) {
    auto databaseId = PgObjectId::GetDatabaseUuid(database_oid);
    auto [status, response] = catalog_manager_->ReservePgOid(databaseId, next_oid, count);
    if (!status.is2xxOK()) {
        return k2pg::K2StatusToK2PgStatus(std::move(status));
    }
    *begin_oid = response.beginOid;
    *end_oid = response.endOid;
    return k2pg::Status::OK;
}

Status SqlCatalogClient::GetCatalogVersion(uint64_t *pg_catalog_version) {
    *pg_catalog_version = catalog_manager_->GetCatalogVersion();
    return k2pg::Status::OK;
}

Status SqlCatalogClient::IncrementCatalogVersion() {
    return k2pg::K2StatusToK2PgStatus(std::get<0>(catalog_manager_->IncrementCatalogVersion()));
}

sh::Status SqlCatalogClient::GetAttrNumToSKVOffset(uint32_t database_oid, uint32_t relation_oid, std::unordered_map<int, uint32_t>& attr_to_offset) {
    auto [status, tableInfo] = catalog_manager_->GetTableSchema(database_oid, relation_oid);
    if (!status.is2xxOK()) {
        return status;
    }
    PgTableDesc desc(tableInfo);
    auto colmap = desc.GetAttrNumToColMap();
    for (const auto& [attr_num, col] : colmap) {
        attr_to_offset[attr_num] = col->index() + numDefaultPartitionKeys;
    }
    return sh::Statuses::S200_OK;
}

sh::Status SqlCatalogClient::GetBaseTableOID(uint32_t database_oid, uint32_t relation_oid, uint32_t& base_table_oid) {
    auto [status, tableInfo] = catalog_manager_->GetTableSchema(database_oid, relation_oid);
    if (!status.is2xxOK()) {
        return status;
    }
    PgTableDesc desc(tableInfo);
    base_table_oid = desc.base_table_oid();
    return sh::Statuses::S200_OK;
}

sh::Status SqlCatalogClient::GetCollectionNameAndSchemaName(uint32_t database_oid, uint32_t relation_oid, std::string& collectionName, std::string& schemaName) {
    auto [status, tableInfo] = catalog_manager_->GetTableSchema(database_oid, relation_oid);
    if (!status.is2xxOK()) {
        return status;
    }
    PgTableDesc desc(tableInfo);
    collectionName = desc.collection_name();
    schemaName = desc.schema_name();
    return  sh::Statuses::S200_OK;
}

} // namespace catalog
}  // namespace k2pg
