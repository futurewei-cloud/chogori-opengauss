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

#include "sql_catalog_manager.h"

namespace k2pg {
namespace catalog {

SqlCatalogManager::SqlCatalogManager() {
}

SqlCatalogManager::~SqlCatalogManager() {
}

sh::Status SqlCatalogManager::Start() {
    K2LOG_I(log::catalog, "Starting Catalog Manager...");
    K2ASSERT(log::catalog, !initted_.load(std::memory_order_acquire), "Already started");
    // load cluster info
    auto [status, clusterInfo] = cluster_info_handler_.GetClusterInfo(cluster_id_);
    if (!status.is2xxOK()) {
        AbortTransaction();
        if (status == sh::Statuses:: S404_Not_Found) {
            K2LOG_W(log::catalog, "Empty cluster info record, likely primary cluster is not initialized. Only operation allowed is primary cluster initialization");
            // it is ok, but only InitPrimaryCluster can be executed on the SqlCatalogrMager, keep initted_ to be false;
            return sh::Statuses::S200_OK;
        } else {
            K2LOG_E(log::catalog, "Failed to read cluster info record due to {}", status);
            return status;
        }
    }

    init_db_done_.store(clusterInfo.initdb_done, std::memory_order_relaxed);
    catalog_version_.store(clusterInfo.catalog_version, std::memory_order_relaxed);
    K2LOG_I(log::catalog, "Loaded cluster info record succeeded, init_db_done: {}, catalog_version: {}", init_db_done_, catalog_version_);
    // end the current transaction so that we use a different one for later operations
   CommitTransaction();

    // load databases
    auto [list_status, infos]  = database_info_handler_.ListDatabases();
    CommitTransaction();

    if (list_status.is2xxOK()) {
        if (!infos.empty()) {
            for (auto& di : infos) {
                // cache databases by database id and database name
                auto ns_ptr = std::make_shared<DatabaseInfo>(std::move(di));
                database_id_map_[ns_ptr->database_id] = ns_ptr;
                database_name_map_[ns_ptr->database_name] = ns_ptr;
                K2LOG_I(log::catalog, "Loaded database id: {}, name: {}", ns_ptr->database_id, ns_ptr->database_name);
                }
            } else {
                K2LOG_D(log::catalog, "databases are empty");
            }
    } else {
        K2LOG_E(log::catalog, "Failed to load databases due to {}", list_status);
        return list_status;
    }

    // TODO: Periodically sync catalog version from k2
    // only start background tasks in normal mode, i.e., not in InitDB mode
    // if (init_db_done_) {
    //     std::function<void()> catalog_version_task([this]{
    //         CheckCatalogVersion();
    //     });
    //     catalog_version_task_ = std::make_unique<SingleThreadedPeriodicTask>(catalog_version_task, "catalog-version-task",
    //         CatalogConsts::catalog_manager_background_task_initial_wait,
    //         CatalogConsts::catalog_manager_background_task_sleep_interval);
    //     catalog_version_task_->Start();
    // }

    initted_.store(true, std::memory_order_release);
    K2LOG_I(log::catalog, "Catalog Manager started up successfully");
    return sh::Statuses::S200_OK;
}

void SqlCatalogManager::Shutdown() {
    K2LOG_I(log::catalog, "SQL CatalogManager shutting down...");

    bool expected = true;
    if (initted_.compare_exchange_strong(expected, false, std::memory_order_acq_rel)) {
        // TODO: Uncomment if use background task
        // // shut down steps
        // if (catalog_version_task_ != nullptr) {
        //     catalog_version_task_.reset(nullptr);
        // }
    }

    K2LOG_I(log::catalog, "SQL CatalogManager shut down complete. Bye!");
}

// Called only once during PG initDB
// TODO: handle partial failure(maybe simply fully cleanup) to allow retry later
sh::Status SqlCatalogManager::InitPrimaryCluster() {
    K2LOG_D(log::catalog, "SQL CatalogManager initialize primary Cluster!");

    K2ASSERT(log::catalog, !initted_.load(std::memory_order_acquire), "Already started");

    // step 1/4 create the SKV collection for the primary
    auto&& [ccResult] = TXMgr.createCollection(skv_collection_name_primary_cluster, primary_cluster_id).get();
    if (!ccResult.is2xxOK()) {
        K2LOG_E(log::catalog, "Failed to create SKV collection during initialization primary PG cluster due to {}", ccResult);
        return ccResult;
    }

    // step 2/4 Init Cluster info, including create the SKVSchema in the primary cluster's SKVCollection for cluster_info and insert current cluster info into
    //      Note: Initialize cluster info's init_db column with TRUE
    ClusterInfo cluster_info{cluster_id_, catalog_version_, false /*init_db_done*/};

    auto status = cluster_info_handler_.InitClusterInfo(cluster_info);
    if (!status.is2xxOK()) {
        AbortTransaction();
        K2LOG_E(log::catalog, "Failed to initialize cluster info due to {}", status);
        return status;
    }

    // step 3/4 Init database_info - create the SKVSchema in the primary cluster's SKVcollection for database_info
    status = database_info_handler_.InitDatabaseTable();
    if (!status.is2xxOK()) {
        K2LOG_E(log::catalog, "Failed to initialize creating database table due to {}", status);
        return status;
    }
   CommitTransaction();
    // step 4/4 re-start this catalog manager so it can execute other APIs
    status = Start();
    if (status.is2xxOK()) {
        // check things are ready
        K2ASSERT(log::catalog, initted_.load(std::memory_order_acquire), "Not initialized");
        K2LOG_D(log::catalog, "SQL CatalogManager successfully initialized primary Cluster!");
    }
    else {
        K2LOG_E(log::catalog, "Failed to create SKV collection during initialization primary PG cluster due to {}", status);
    }

    return status;
}

sh::Status SqlCatalogManager::FinishInitDB() {
    K2LOG_D(log::catalog, "Setting initDbDone to be true...");
    if (!init_db_done_) {
        // check the latest cluster info on SKV
        auto [status, clusterInfo] = GetClusterInfo(false);
        if (!status.is2xxOK()) {
            AbortTransaction();
            K2LOG_E(log::catalog, "Cannot read cluster info record on SKV due to {}", status);
            return status;
        }

        if (clusterInfo.initdb_done) {
           CommitTransaction();
            init_db_done_.store(true, std::memory_order_relaxed);
            K2LOG_D(log::catalog, "InitDbDone is already true on SKV");
            return sh::Statuses::S200_OK;
        }

        K2LOG_D(log::catalog, "Updating cluster info with initDbDone to be true");
        clusterInfo.initdb_done = true;
        status = cluster_info_handler_.UpdateClusterInfo(clusterInfo);
        if (!status.is2xxOK()) {
            AbortTransaction();
            K2LOG_E(log::catalog, "Failed to update cluster info due to {}", status);
            return status;
        }
       CommitTransaction();
        init_db_done_.store(true, std::memory_order_relaxed);
        K2LOG_D(log::catalog, "Set initDbDone to be true successfully");
    } else {
        K2LOG_D(log::catalog, "InitDb is true already");
    }
    return sh::Statuses::S200_OK;
}

sh::Response<bool> SqlCatalogManager::IsInitDbDone() {
    if (!init_db_done_) {
        auto [status, clusterInfo] = GetClusterInfo();
        if (!status.is2xxOK()) {
            K2LOG_E(log::catalog, "Failed to check IsInitDbDone from SKV due to {}", status);
            return std::make_tuple(status, false);
        }

        K2LOG_D(log::catalog, "Checked IsInitDbDone from SKV {}", clusterInfo.initdb_done);
        if (clusterInfo.initdb_done) {
            init_db_done_.store(clusterInfo.initdb_done, std::memory_order_relaxed);
        }
        if (clusterInfo.catalog_version > catalog_version_) {
            catalog_version_.store(clusterInfo.catalog_version, std::memory_order_relaxed);
        }

    }
    K2LOG_D(log::catalog, "Get InitDBDone successfully {}", (bool)init_db_done_);
    return std::make_tuple(sh::Statuses::S200_OK, (bool)init_db_done_);
}

void SqlCatalogManager::CheckCatalogVersion() {
    std::lock_guard<std::mutex> l(lock_);
    K2LOG_D(log::catalog, "Checking catalog version...");
    auto [status, clusterInfo] = GetClusterInfo();
    if (!status.is2xxOK()) {
        K2LOG_E(log::catalog, "Failed to check cluster info due to {}", status);
        return;
    }
    if (clusterInfo.catalog_version > catalog_version_) {
        catalog_version_ = clusterInfo.catalog_version;
        K2LOG_D(log::catalog, "Updated catalog version to {}", catalog_version_);
    }
}


uint64_t SqlCatalogManager::GetCatalogVersion() {
    std::lock_guard<std::mutex> l(lock_);
    return catalog_version_;
}

sh::Response<ClusterInfo> SqlCatalogManager::GetClusterInfo(bool commit) {
    auto response = cluster_info_handler_.GetClusterInfo(cluster_id_);
    if (commit)
       CommitTransaction();
    return response;
}

sh::Response<uint64_t> SqlCatalogManager::IncrementCatalogVersion() {
    std::lock_guard<std::mutex> l(lock_);
    // TODO: use a background thread to fetch the ClusterInfo record periodically instead of fetching it for each call
    auto [status, clusterInfo] = GetClusterInfo(false);
    if (!status.is2xxOK()) {
        K2LOG_E(log::catalog, "Failed to check cluster info due to {}", status);
        AbortTransaction();
        return sh::Response<uint64_t>(status, 0);
    }

    K2LOG_D(log::catalog, "Found SKV catalog version: {}", clusterInfo.catalog_version);
    catalog_version_ = clusterInfo.catalog_version + 1;
    // need to update the catalog version on SKV
    // the update frequency could be reduced once we have a single or a quorum of catalog managers
    ClusterInfo new_cluster_info{cluster_id_, catalog_version_, init_db_done_};
    status = cluster_info_handler_.UpdateClusterInfo(new_cluster_info);
    if (!status.is2xxOK()) {
        AbortTransaction();
        catalog_version_ = clusterInfo.catalog_version;
        K2LOG_D(log::catalog, "Failed to update catalog version due to {}, revert catalog version to {}", status, catalog_version_);
        return std::make_tuple(status, clusterInfo.catalog_version);
    }
   CommitTransaction();
    K2LOG_D(log::catalog, "Increase catalog version to {}", catalog_version_);
    return std::make_tuple(status, (uint64_t) catalog_version_.load());
}


sh::Response<std::shared_ptr<DatabaseInfo>>  SqlCatalogManager::CreateDatabase(const CreateDatabaseRequest& request) {
    K2LOG_D(log::catalog,
    "Creating database with name: {}, id: {}, oid: {}, source_id: {}, nextPgOid: {}",
    request.databaseName, request.databaseId, request.databaseOid, request.sourceDatabaseId, request.nextPgOid.value_or(-1));
    std::lock_guard<std::mutex> l(lock_);
        // step 1/3:  check input conditions
        //      check if the target database has already been created, if yes, return already present
        //      check the source database is already there, if it present in the create requet
    std::shared_ptr<DatabaseInfo> database_info = CheckAndLoadDatabaseByName(request.databaseName);
    if (database_info != nullptr) {
        K2LOG_E(log::catalog, "Database {} has already existed", request.databaseName);
        return std::make_tuple(sh::Statuses::S409_Conflict(fmt::format("Database {} has already existed", request.databaseName)), std::shared_ptr<DatabaseInfo>());
    }

    std::shared_ptr<DatabaseInfo> source_database_info = nullptr;
    uint32_t t_nextPgOid;
    // validate source database id and check source database to set nextPgOid properly
    if (!request.sourceDatabaseId.empty()) {
        source_database_info = CheckAndLoadDatabaseById(request.sourceDatabaseId);
        if (source_database_info == nullptr) {
            K2LOG_E(log::catalog, "Failed to find source databases {}", request.sourceDatabaseId);
            return std::make_tuple(sh::Statuses::S404_Not_Found(fmt::format("Source database {} not found", request.sourceDatabaseId)), std::shared_ptr<DatabaseInfo>());
        }
        t_nextPgOid = source_database_info->next_pg_oid;
    } else {
        t_nextPgOid = request.nextPgOid.value();
    }

    // step 2/3: create new database(database), total 3 sub-steps

    // step 2.1 create new SKVCollection
    //   Note: using unique immutable databaseId as SKV collection name
    //   TODO: pass in other collection configurations/parameters later.
    K2LOG_D(log::catalog, "Creating SKV collection for database {}", request.databaseId);
    auto [ccResult] = TXMgr.createCollection(request.databaseId, request.databaseName).get();
    if (!ccResult.is2xxOK()) {
        K2LOG_E(log::catalog, "Failed to create SKV collection {} due to {}", request.databaseId, ccResult);
        return std::make_tuple(ccResult, std::shared_ptr<DatabaseInfo>());
    }

    // step 2.2 Add new database(database) entry into default cluster database table and update in-memory cache
    std::shared_ptr<DatabaseInfo> new_ns = std::make_shared<DatabaseInfo>();
    new_ns->database_id = request.databaseId;
    new_ns->database_name =  request.databaseName;
    new_ns->database_oid = request.databaseOid;
    new_ns->next_pg_oid = request.nextPgOid.value();
    // persist the new database record
    K2LOG_D(log::catalog, "Adding database {} on SKV", request.databaseId);
    auto status = database_info_handler_.UpsertDatabase(*new_ns);
    if (!status.is2xxOK()) {
        K2LOG_E(log::catalog, "Failed to add database {}, due to {}", request.databaseId, status);
        AbortTransaction();
        return std::make_tuple(std::move(status), std::shared_ptr<DatabaseInfo>());
    }
    // cache databases by database id and database name
    database_id_map_[new_ns->database_id] = new_ns;
    database_name_map_[new_ns->database_name] = new_ns;

    // step 2.3 Add new system tables for the new database(database)
    K2LOG_D(log::catalog, "Creating system tables for target database {}", new_ns->database_id);
    if (auto status = table_info_handler_.CreateMetaTables(new_ns->database_id); !status.is2xxOK()) {
        K2LOG_E(log::catalog, "Failed to create meta tables for target database {} due to {}",
        new_ns->database_id, status);
        AbortTransaction();
        return std::make_tuple(status, std::shared_ptr<DatabaseInfo>());
    }

    // step 3/3: If source database(database) is present in the request, copy all the rest of tables from source database(database)
    if (!request.sourceDatabaseId.empty()) {
        // Commit txn so that operations on new collection are done with a timestamp > collection create time
        auto [commit_status] = k2pg::TXMgr.endTxn(skv::http::dto::EndAction::Commit).get();
        if (!commit_status.is2xxOK()) {
            K2LOG_E(log::catalog, "Failed to commit txn due to {}", commit_status);
            return std::make_tuple(commit_status, std::shared_ptr<DatabaseInfo>());
        }

        K2LOG_D(log::catalog, "Creating database from source database {}", request.sourceDatabaseId);
        // get the source table ids
        K2LOG_D(log::catalog, "Listing table ids from source database {}", request.sourceDatabaseId);
        auto [status, tableIds] = table_info_handler_.ListTableIds(source_database_info->database_id, true);
        if (!status.is2xxOK()) {
            K2LOG_E(log::catalog, "Failed to list table ids for database {} due to {}", source_database_info->database_id, status);
            AbortTransaction();
            return std::make_tuple(status, std::shared_ptr<DatabaseInfo>());
        }
        K2LOG_D(log::catalog, "Found {} table ids from source database {}", tableIds.size(), request.sourceDatabaseId);
        int num_index = 0;
        for (auto& source_table_id : tableIds) {
            // copy the source table metadata to the target table
            K2LOG_D(log::catalog, "Copying from source table {}", source_table_id);
            auto [status, result] = table_info_handler_.CopyTable(
                new_ns->database_id,
                new_ns->database_name,
                new_ns->database_oid,
                source_database_info->database_id,
                source_database_info->database_name,
                source_table_id);
            if (!status.is2xxOK()) {
                K2LOG_E(log::catalog, "Failed to copy from source table {} due to {}", source_table_id, status);
                AbortTransaction();
                return std::make_tuple(status, std::shared_ptr<DatabaseInfo>());
            }
            num_index += result.num_index;
        }
        CommitTransaction();
        K2LOG_D(log::catalog, "Finished copying {} tables and {} indexes from source database {} to {}",
                tableIds.size(), num_index, source_database_info->database_id, new_ns->database_id);
    }

    CommitTransaction();
    K2LOG_D(log::catalog, "Created database {}", new_ns->database_id);
    return std::make_pair(sh::Statuses::S200_OK, new_ns);
}

sh::Response<std::vector<DatabaseInfo>> SqlCatalogManager::ListDatabases() {
    K2LOG_D(log::catalog, "Listing databases...");
    auto [status, databaseInfos] = database_info_handler_.ListDatabases();
    if (!status.is2xxOK()) {
        AbortTransaction();
        K2LOG_E(log::catalog, "Failed to list databases due to {}", status);
        return std::make_pair(status, databaseInfos);
    }
   CommitTransaction();
    if (databaseInfos.empty()) {
        K2LOG_W(log::catalog, "No databases are found");
    } else {
        std::lock_guard<std::mutex> l(lock_);
        UpdateDatabaseCache(databaseInfos);
        K2LOG_D(log::catalog, "Found {} databases", databaseInfos.size());
    }
    return std::make_pair(status, databaseInfos);
 }

sh::Response<std::shared_ptr<DatabaseInfo>> SqlCatalogManager::GetDatabase(const std::string& databaseName, const std::string& databaseId) {
    K2LOG_D(log::catalog, "Getting database with name: {}, id: {}", databaseName, databaseId);
    std::lock_guard<std::mutex> l(lock_);
    if (std::shared_ptr<DatabaseInfo> database_info = GetCachedDatabaseById(databaseId); database_info != nullptr) {
        return std::make_pair(sh::Statuses::S200_OK, database_info);
    }
    // TODO: use a background task to refresh the database caches to avoid fetching from SKV on each call
    auto [status, databaseInfo] = database_info_handler_.GetDatabase(databaseId);
    if (!status.is2xxOK()) {
        AbortTransaction();
        K2LOG_E(log::catalog, "Failed to get database {}, due to {}", databaseId, status);
        return sh::Response<std::shared_ptr<DatabaseInfo>>(status, nullptr);
    }
    CommitTransaction();
    auto di = std::make_shared<DatabaseInfo>(std::move(databaseInfo));
    // update database caches
    database_id_map_[di->database_id] = di;
    database_name_map_[di->database_name] = di;
    K2LOG_D(log::catalog, "Found database {}", databaseId);
    return std::make_pair(status, di);
}

sh::Status SqlCatalogManager::DeleteDatabase(const std::string& databaseName, const std::string& databaseId) {
    K2LOG_D(log::catalog, "Deleting database with name: {}, id: {}", databaseName, databaseId);
    std::lock_guard<std::mutex> l(lock_);
    // TODO: use a background task to refresh the database caches to avoid fetching from SKV on each call
    auto [status, database_info] = database_info_handler_.GetDatabase(databaseId);
    CommitTransaction();
    if (!status.is2xxOK()) {
        K2LOG_E(log::catalog, "Failed to get deletion target database {} {}.", databaseId, status);
        return status;
    }
    // No need to delete table data or metadata, it will be dropped with the SKV collection
    status  = database_info_handler_.DeleteDatabase(database_info);
    if (!!status.is2xxOK()) {
        AbortTransaction();
        return status;
    }
   CommitTransaction();
    // remove database from local cache
    database_id_map_.erase(database_info.database_id);
    database_name_map_.erase(database_info.database_name);

    // // DropCollection will remove the K2 collection, all of its schemas, and all of its data.
    // // It is non-transactional with no rollback ability, but that matches PG's drop database semantics.
    // TODO: Add drop collection
    // auto drop_result = k2_adapter_->DropCollection(database_info->GetDatabaseId()).get();

    // response.status = k2pg::gate::K2Adapter::K2StatusToK2PgStatus(drop_result);
    return status;
}

sh::Status SqlCatalogManager::UseDatabase(const std::string& databaseName) {
    std::lock_guard<std::mutex> l(lock_);
    // check if the database exists
    std::shared_ptr<DatabaseInfo> database_info = CheckAndLoadDatabaseByName(databaseName);
    if (database_info == nullptr) {
        K2LOG_E(log::catalog, "Cannot find database {}", databaseName);
        return sh::Statuses::S404_Not_Found(fmt::format("Cannot find database {}", databaseName));
    }

    // TODO: Do following operation in backround
    // preload tables for a database
    // thread_pool_.enqueue([this, request] () {
    K2LOG_I(log::catalog, "Preloading database {}", databaseName);
    if (auto status = CacheTablesFromStorage(databaseName, true /*isSysTableIncluded*/); !status.is2xxOK()) {
        K2LOG_W(log::catalog, "Failed to preloading database {} due to {}", databaseName, status);
    }
    //     });
    return sh::Statuses::S200_OK;
}

sh::Response<std::shared_ptr<TableInfo>> SqlCatalogManager::CreateTable(const CreateTableRequest& request) {
    K2LOG_D(log::catalog,
    "Creating table ns name: {}, ns oid: {}, table name: {}, table oid: {}, systable: {}, shared: {}",
    request.databaseName, request.databaseOid, request.tableName, request.tableOid, request.isSysCatalogTable, request.isSharedTable);
    std::lock_guard<std::mutex> l(lock_);
    std::shared_ptr<DatabaseInfo> database_info = CheckAndLoadDatabaseByName(request.databaseName);
    if (database_info == nullptr) {
        K2LOG_E(log::catalog, "Cannot find databaseName {}", request.databaseName);
        return std::make_tuple(sh::Statuses::S404_Not_Found, std::shared_ptr<TableInfo>());
    }

    // check if the Table has already existed or not
    std::shared_ptr<TableInfo> table_info = GetCachedTableInfoByName(database_info->database_id, request.tableName);
    if (table_info != nullptr) {
        // only create table when it does not exist
        if (request.isNotExist) {
            return std::make_tuple(sh::Statuses::S200_OK, table_info);
        }
        return std::make_tuple(sh::Statuses::S400_Bad_Request, table_info);
    }

    // new table
    uint32_t schema_version = request.schema.version();
    K2ASSERT(log::catalog, schema_version == 0, "Schema version was not initialized to be zero");
    schema_version++;
    // generate a string format table id based database object oid and table oid
    std::string uuid = PgObjectId::GetTableUuid(request.databaseOid, request.tableOid);
    Schema table_schema = request.schema;
    table_schema.set_version(schema_version);
    std::shared_ptr<TableInfo> new_table_info = std::make_shared<TableInfo>(database_info->database_id, request.databaseName,
        request.tableOid, request.tableName, uuid, table_schema);
    new_table_info->set_is_sys_table(request.isSysCatalogTable);
    new_table_info->set_is_shared_table(request.isSharedTable);
    new_table_info->set_next_column_id(table_schema.max_col_id() + 1);

    K2LOG_D(log::catalog, "Create or update table id: {}, name: {} in {}, shared: {}", new_table_info->table_id(), request.tableName,
        database_info->database_id, request.isSharedTable);
    try {
        auto status = table_info_handler_.CreateOrUpdateTable(database_info->database_id, new_table_info);
        if (!status.is2xxOK()) {
            // abort the transaction
            AbortTransaction();
            K2LOG_E(log::catalog, "Failed to create table id: {}, name: {} in {}, due to {}", new_table_info->table_id(), new_table_info->table_name(),
                database_info->database_id, status);
            return std::make_tuple(status, new_table_info);
        }

        // commit transactions
        CommitTransaction();
        K2LOG_D(log::catalog, "Created table id: {}, name: {} in {}, with schema version {}", new_table_info->table_id(), new_table_info->table_name(),
            database_info->database_id, schema_version);
        // update table caches
        UpdateTableCache(new_table_info);
        return std::make_tuple(status, new_table_info);
    }  catch (const std::exception& e) {
        AbortTransaction();
        K2LOG_E(log::catalog, "Failed to create table {} in {} due to {}", request.tableName, database_info->database_id, e.what());
        return std::make_tuple(sh::Statuses::S500_Internal_Server_Error(e.what()), std::shared_ptr<TableInfo>());
    }
}

sh::Response<std::shared_ptr<IndexInfo>> SqlCatalogManager::CreateIndexTable(const CreateIndexTableRequest& request) {
    K2LOG_D(log::catalog, "Creating index ns name: {}, ns oid: {}, index name: {}, index oid: {}, base table oid: {}",
    request.databaseName, request.databaseOid, request.tableName, request.tableOid, request.baseTableOid);
    std::lock_guard<std::mutex> l(lock_);
    std::shared_ptr<DatabaseInfo> database_info = CheckAndLoadDatabaseByName(request.databaseName);
    if (database_info == nullptr) {
        K2LOG_E(log::catalog, "Cannot find databaseName {}", request.databaseName);
        return std::make_tuple(sh::Statuses::S404_Not_Found, std::shared_ptr<IndexInfo>());
    }
    // generate table uuid from database oid and table oid
    std::string base_table_uuid = PgObjectId::GetTableUuid(request.databaseOid, request.baseTableOid);
    std::string base_table_id = PgObjectId::GetTableId(request.baseTableOid);

    // check if the base table exists or not
    std::shared_ptr<TableInfo> base_table_info = GetCachedTableInfoById(base_table_uuid);
    // try to fetch the table from SKV if not found
    if (base_table_info == nullptr) {
        auto [status, tableInfo] = table_info_handler_.GetTable(database_info->database_id, database_info->database_name, base_table_id);
        if (status.is2xxOK() && tableInfo != nullptr) {
            // update table cache
            UpdateTableCache(tableInfo);
            base_table_info = tableInfo;
        }
    }

    if (base_table_info == nullptr) {
        AbortTransaction();
        // cannot find the base table
        K2LOG_E(log::catalog, "Cannot find base table {} for index {} in {}", base_table_id, request.tableName, database_info->database_id);
        return std::make_tuple(sh::Statuses::S404_Not_Found, std::shared_ptr<IndexInfo>());
    }

    CreateIndexTableParams index_params;
    index_params.index_name = request.tableName;
    index_params.table_oid = request.tableOid;
    index_params.index_schema = request.schema;
    index_params.is_unique = request.isUnique;
    index_params.is_shared = request.isSharedTable;
    index_params.is_not_exist = request.isNotExist;
    index_params.skip_index_backfill = request.skipIndexBackfill;
    index_params.index_permissions = IndexPermissions::INDEX_PERM_READ_WRITE_AND_DELETE;

        // create the table index
    auto [status, indexInfo] = table_info_handler_.CreateIndexTable(database_info, base_table_info, index_params);
    if (!status.is2xxOK()) {
        AbortTransaction();
        K2LOG_E(log::catalog, "Failed to create index ns name: {}, ns oid: {}, index name: {}, index oid: {}, base table oid: {}",
                request.databaseName, request.databaseOid, request.tableName,
                request.tableOid, request.baseTableOid);

        return std::make_tuple(status, indexInfo);
    }
    K2ASSERT(log::catalog, indexInfo != nullptr, "Table index can't be null");
    K2LOG_D(log::catalog, "Updating cache for table id: {}, name: {} in {}", indexInfo->table_id(), indexInfo->table_name(), database_info->database_id);
    // update table cache
    UpdateTableCache(base_table_info);

    // update index cache
    AddIndexCache(indexInfo);

    // commit and return the new index table
    CommitTransaction();

    K2LOG_D(log::catalog, "Created index ns name: {}, ns oid: {}, index name: {}, index oid: {}, base table oid: {}",
      request.databaseName, request.databaseOid, request.tableName, request.tableOid, request.baseTableOid);
        return std::make_tuple(status, indexInfo);
}

// Get (base) table schema - if passed-in id is that of a index, return base table schema, if is that of a table, return its table schema
sh::Response<std::shared_ptr<TableInfo>> SqlCatalogManager::GetTableSchema(const uint32_t databaseOid,  uint32_t tableOid) {
    // generate table id from database oid and table oid
    std::string table_uuid = PgObjectId::GetTableUuid(databaseOid, tableOid);
    std::string table_id = PgObjectId::GetTableId(tableOid);
    K2LOG_D(log::catalog, "Get table schema ns oid: {}, table oid: {}, table id: {}",
        databaseOid, tableOid, table_id);
    std::lock_guard<std::mutex> l(lock_);
    // check the table schema from cache
    std::shared_ptr<TableInfo> table_info = GetCachedTableInfoById(table_uuid);
    if (table_info != nullptr) {
        K2LOG_D(log::catalog, "Returned cached table schema name: {}, id: {}", table_info->table_name(), table_info->table_id());
        return std::make_tuple(sh::Statuses::S200_OK, table_info);
    }

    // check if passed in id is that of an index and if so return base table info by index uuid
    table_info = GetCachedBaseTableInfoByIndexId(databaseOid, table_uuid);
    if (table_info != nullptr) {
        K2LOG_D(log::catalog, "Returned cached table schema name: {}, id: {} for index {}",
                table_info->table_name(), table_info->table_id(), table_uuid);
        return std::make_tuple(sh::Statuses::S200_OK, table_info);
    }

    // TODO: refactor following SKV lookup code(till cache update) into tableHandler class
    // Can't find the id from cache above, now look into storage.
    std::string database_id = PgObjectId::GetDatabaseUuid(databaseOid);
    std::shared_ptr<DatabaseInfo> database_info = CheckAndLoadDatabaseById(database_id);
    if (database_info == nullptr) {
        K2LOG_E(log::catalog, "Cannot find database {}", database_id);
        return std::make_tuple(sh::Statuses:: S404_Not_Found, std::shared_ptr<TableInfo>());
    }
    std::shared_ptr<IndexInfo> index_info = GetCachedIndexInfoById(table_uuid);
    auto [status, tableInfo] = table_info_handler_.GetTableSchema(database_info, table_id, index_info,
      [this] (const std::string &db_id) { return CheckAndLoadDatabaseById(db_id); }
        );

    if (status.is2xxOK() && tableInfo != nullptr) {
        // update table cache
        UpdateTableCache(tableInfo);
        return std::make_tuple(sh::Statuses::S200_OK, tableInfo);
    }
    return std::make_pair(status, tableInfo);
}

sh::Status SqlCatalogManager::CacheTablesFromStorage(const std::string& databaseName, bool isSysTableIncluded) {
    K2LOG_D(log::catalog, "cache tables for database {}", databaseName);

    std::shared_ptr<DatabaseInfo> database_info = CheckAndLoadDatabaseByName(databaseName);
    if (database_info == nullptr) {
        K2LOG_E(log::catalog, "Cannot find database {}", databaseName);
        return sh::Statuses:: S404_Not_Found;
    }
    auto [status, tableInfos] = table_info_handler_.ListTables(database_info->database_id,
        database_info->database_name, isSysTableIncluded);
    if (!status.is2xxOK()) {
        AbortTransaction();
        return status;
    }
    CommitTransaction();
    K2LOG_D(log::catalog, "Found {} tables in database {}", tableInfos.size(), databaseName);
    for (auto& tableInfo : tableInfos) {
        K2LOG_D(log::catalog, "Caching table name: {}, id: {} in {}", tableInfo->table_name(), tableInfo->table_id(), database_info->database_id);
        UpdateTableCache(tableInfo);
    }
    return status;
}

sh::Response<DeleteTableResponse> SqlCatalogManager::DeleteTable(const uint32_t databaseOid, uint32_t tableOid) {
    DeleteTableResponse response;
    K2LOG_D(log::catalog, "Deleting table {} in database {}", tableOid, databaseOid);
    std::string database_id = PgObjectId::GetDatabaseUuid(databaseOid);
    std::string table_uuid = PgObjectId::GetTableUuid(databaseOid, tableOid);
    std::string table_id = PgObjectId::GetTableId(tableOid);
    response.databaseId = database_id;
    response.tableId = table_id;
    std::lock_guard<std::mutex> l(lock_);

    std::shared_ptr<TableInfo> table_info = GetCachedTableInfoById(table_uuid);
    if (table_info == nullptr) {
        // try to find table from SKV by looking at database first
        std::shared_ptr<DatabaseInfo> database_info = CheckAndLoadDatabaseById(database_id);
        if (database_info == nullptr) {
            K2LOG_E(log::catalog, "Cannot find database {}", database_id);
            return std::make_tuple(sh::Statuses:: S404_Not_Found, response);
        }

        // fetch the table from K2
        auto [status, tableInfo] = table_info_handler_.GetTable(database_info->database_id, database_info->database_name,
                table_id);
        if (!status.is2xxOK()) {
            AbortTransaction();
            return std::make_tuple(status, response);
        }

        if (tableInfo == nullptr) {
            AbortTransaction();
            return std::make_tuple(sh::Statuses:: S404_Not_Found(fmt::format("Cannot find table {}", table_id)), response);
        }

        table_info = tableInfo;
    }

    // delete indexes and the table itself
    // delete table data
    if (auto status = table_info_handler_.DeleteTableData(database_id, table_info); !status.is2xxOK()) {
        AbortTransaction();
        return std::make_tuple(status, response);
    }

    // delete table schema metadata
    if (auto status = table_info_handler_.DeleteTableMetadata(database_id, table_info); !status.is2xxOK()) {
        AbortTransaction();
        return std::make_tuple(status, response);
    }
    CommitTransaction();

    // clear table cache after table deletion
    ClearTableCache(table_info);
    return std::make_tuple(sh::Statuses::S200_OK, response);
}

sh::Response<DeleteIndexResponse> SqlCatalogManager::DeleteIndex(const uint32_t databaseOid, uint32_t tableOid) {
    K2LOG_D(log::catalog, "Deleting index {} in ns {}", tableOid, databaseOid);
    DeleteIndexResponse response;
    std::string database_id = PgObjectId::GetDatabaseUuid(databaseOid);
    std::string table_uuid = PgObjectId::GetTableUuid(databaseOid, tableOid);
    std::string table_id = PgObjectId::GetTableId(tableOid);
    response.databaseId = database_id;
    std::shared_ptr<DatabaseInfo> database_info = CheckAndLoadDatabaseById(database_id);
    if (database_info == nullptr) {
        K2LOG_E(log::catalog, "Cannot find database {}", database_id);
        return std::make_tuple(sh::Statuses:: S404_Not_Found, response);
    }
    std::shared_ptr<IndexInfo> index_info = GetCachedIndexInfoById(table_uuid);
    std::string base_table_id;
    if (index_info == nullptr) {
        auto [status, baseTableId] = table_info_handler_.GetBaseTableId(database_id, table_id);
        if (!status.is2xxOK()) {
            AbortTransaction();
            return std::make_tuple(status, response);
        }
        base_table_id = baseTableId;
    } else {
        base_table_id = index_info->base_table_id();
    }

    std::shared_ptr<TableInfo> base_table_info = GetCachedTableInfoById(base_table_id);
    // try to fetch the table from SKV if not found
    if (base_table_info == nullptr) {
        auto [status, tableInfo] = table_info_handler_.GetTable(database_id, database_info->database_name,
                    base_table_id);
        if (!status.is2xxOK()) {
            AbortTransaction();
            return std::make_tuple(status, response);
        }

        if (tableInfo == nullptr) {
            AbortTransaction();
            return std::make_tuple(sh::Statuses:: S404_Not_Found, response);
        }

        base_table_info = tableInfo;
    }

    // delete index data
    if (auto status = table_info_handler_.DeleteIndexData(database_id, table_id); status.is2xxOK()) {
        AbortTransaction();
        return std::make_tuple(status, response);
    }

    // delete index metadata
    if (auto status = table_info_handler_.DeleteIndexMetadata(database_id, table_id); status.is2xxOK()) {
        AbortTransaction();
        return std::make_tuple(status, response);
    }

    CommitTransaction();
    // remove index from the table_info object
    base_table_info->drop_index(table_id);
    // update table cache with the index removed, index cache is updated accordingly
    UpdateTableCache(base_table_info);
    response.baseIndexTableOid = base_table_info->table_oid();
    return std::make_tuple(sh::Statuses::S200_OK, response);
}

sh::Response<ReservePgOidsResponse> SqlCatalogManager::ReservePgOid(const std::string& databaseId, uint32_t nextOid, uint32_t count) {
    ReservePgOidsResponse response;
    K2LOG_D(log::catalog, "Reserving PgOid with nextOid: {}, count: {}, for ns: {}",
            nextOid, count, databaseId);
    auto [status, databaseInfo] = database_info_handler_.GetDatabase(databaseId);
    if (!status.is2xxOK()) {
        AbortTransaction();
        K2LOG_E(log::catalog, "Failed to get database {}", databaseId);
        return std::make_tuple(status, response);
    }

    uint32_t begin_oid = databaseInfo.next_pg_oid;
    if (begin_oid < nextOid) {
        begin_oid = nextOid;
    }
    if (begin_oid == std::numeric_limits<uint32_t>::max()) {
        AbortTransaction();
        K2LOG_W(log::catalog, "No more object identifier is available for Postgres database {}", databaseId);
        return std::make_tuple(sh::Statuses::S400_Bad_Request, response);
    }

    uint32_t end_oid = begin_oid + count;
    if (end_oid < begin_oid) {
        end_oid = std::numeric_limits<uint32_t>::max(); // Handle wraparound.
    }
    response.databaseId = databaseId;
    response.beginOid = begin_oid;
    response.endOid = end_oid;

    // update the database record on SKV
    // We use read and write in the same transaction so that K23SI guarantees that concurrent SKV records on SKV
    // won't override each other and won't lose the correctness of PgNextOid
    databaseInfo.next_pg_oid = end_oid;
    K2LOG_D(log::catalog, "Updating nextPgOid on SKV to {} for database {}", end_oid, databaseId);
    if (auto status = database_info_handler_.UpsertDatabase(databaseInfo); !status.is2xxOK()) {
        AbortTransaction();
        K2LOG_E(log::catalog, "Failed to update nextPgOid on SKV due to {}", status);
        return std::make_tuple(status, response);
    }

    CommitTransaction();
    K2LOG_D(log::catalog, "Reserved PgOid succeeded for database {}", databaseId);
        // update database caches after persisting to SKV successfully
    auto updated_ns = std::make_shared<DatabaseInfo>(databaseInfo);
    std::lock_guard<std::mutex> l(lock_);
    database_id_map_[updated_ns->database_id] = updated_ns;
    database_name_map_[updated_ns->database_name] = updated_ns;
    return std::make_tuple(sh::Statuses::S200_OK, response);
}

// update table caches
void SqlCatalogManager::UpdateTableCache(std::shared_ptr<TableInfo> table_info) {
    table_uuid_map_[table_info->table_uuid()] = table_info;
    // TODO: add logic to remove table with old name if rename table is called
    TableNameKey key = std::make_pair(table_info->database_id(), table_info->table_name());
    table_name_map_[key] = table_info;
    // update the corresponding index cache
    UpdateIndexCacheForTable(table_info);
}

// remove table info from table cache and its related indexes from index cache
void SqlCatalogManager::ClearTableCache(std::shared_ptr<TableInfo> table_info) {
    ClearIndexCacheForTable(table_info->table_id());
    table_uuid_map_.erase(table_info->table_uuid());
    TableNameKey key = std::make_pair(table_info->database_id(), table_info->table_name());
    table_name_map_.erase(key);
}

// clear index infos for a table in the index cache
void SqlCatalogManager::ClearIndexCacheForTable(const std::string& base_table_id) {
    std::vector<std::string> index_uuids;
    for (std::pair<std::string, std::shared_ptr<IndexInfo>> pair : index_uuid_map_) {
        // first find all indexes that belong to the table
        if (base_table_id == pair.second->base_table_id()) {
            index_uuids.push_back(pair.second->table_uuid());
        }
    }
    // delete the indexes in cache
    for (std::string index_uuid : index_uuids) {
        index_uuid_map_.erase(index_uuid);
    }
}

void SqlCatalogManager::UpdateIndexCacheForTable(std::shared_ptr<TableInfo> table_info) {
    // clear existing index informaton first
    ClearIndexCacheForTable(table_info->table_id());
    // add the new indexes to the index cache
    if (table_info->has_secondary_indexes()) {
        for (std::pair<std::string, IndexInfo> pair : table_info->secondary_indexes()) {
            AddIndexCache(std::make_shared<IndexInfo>(pair.second));
        }
    }
}

void SqlCatalogManager::AddIndexCache(std::shared_ptr<IndexInfo> index_info) {
    index_uuid_map_[index_info->table_uuid()] = index_info;
}

std::shared_ptr<TableInfo> SqlCatalogManager::GetCachedTableInfoById(const std::string& table_uuid) {
    if (!table_uuid_map_.empty()) {
        const auto itr = table_uuid_map_.find(table_uuid);
        if (itr != table_uuid_map_.end()) {
            return itr->second;
        }
    }
    return nullptr;
}

std::shared_ptr<TableInfo> SqlCatalogManager::GetCachedTableInfoByName(const std::string& database_id, const std::string& table_name) {
    if (!table_name_map_.empty()) {
        TableNameKey key = std::make_pair(database_id, table_name);
        const auto itr = table_name_map_.find(key);
        if (itr != table_name_map_.end()) {
            return itr->second;
        }
    }
    return nullptr;
}

std::shared_ptr<IndexInfo> SqlCatalogManager::GetCachedIndexInfoById(const std::string& index_uuid) {
    if (!index_uuid_map_.empty()) {
        const auto itr = index_uuid_map_.find(index_uuid);
        if (itr != index_uuid_map_.end()) {
            return itr->second;
        }
    }
    return nullptr;
}

std::shared_ptr<TableInfo> SqlCatalogManager::GetCachedBaseTableInfoByIndexId(uint32_t databaseOid, const std::string& index_uuid) {
    std::shared_ptr<IndexInfo> index_info = nullptr;
    if (!index_uuid_map_.empty()) {
        const auto itr = index_uuid_map_.find(index_uuid);
        if (itr != index_uuid_map_.end()) {
            index_info = itr->second;
        }
    }
    if (index_info == nullptr) {
        return nullptr;
    }

    // get base table uuid from database oid and base table id
    uint32_t base_table_oid = PgObjectId::GetTableOidByTableUuid(index_info->base_table_id());
    if (base_table_oid == kPgInvalidOid) {
        K2LOG_W(log::catalog, "Invalid base table id {}", index_info->base_table_id());
        return nullptr;
    }
    std::string base_table_uuid = PgObjectId::GetTableUuid(databaseOid, base_table_oid);
    return GetCachedTableInfoById(base_table_uuid);
}

// update database caches
void SqlCatalogManager::UpdateDatabaseCache(const std::vector<DatabaseInfo>& database_infos) {
    database_id_map_.clear();
    database_name_map_.clear();
    for (const auto& ns : database_infos) {
        auto ns_ptr = std::make_shared<DatabaseInfo>(ns);
        database_id_map_[ns_ptr->database_id] = ns_ptr;
        database_name_map_[ns_ptr->database_name] = ns_ptr;
    }
}


std::shared_ptr<DatabaseInfo> SqlCatalogManager::GetCachedDatabaseById(const std::string& database_id) {
    if (!database_id_map_.empty()) {
        const auto itr = database_id_map_.find(database_id);
        if (itr != database_id_map_.end()) {
            return itr->second;
        }
    }
    return nullptr;
}

std::shared_ptr<DatabaseInfo> SqlCatalogManager::GetCachedDatabaseByName(const std::string& database_name) {
    if (!database_name_map_.empty()) {
        const auto itr = database_name_map_.find(database_name);
        if (itr != database_name_map_.end()) {
            return itr->second;
        }
    }
    return nullptr;
}


void SqlCatalogManager::LoadDatabases() {
    auto [status, databaseInfos] = database_info_handler_.ListDatabases();
    CommitTransaction();
    if (status.is2xxOK() && !databaseInfos.empty()) {
        // update database caches
        UpdateDatabaseCache(databaseInfos);
    }
}


std::shared_ptr<DatabaseInfo> SqlCatalogManager::CheckAndLoadDatabaseByName(const std::string& database_name) {
    std::shared_ptr<DatabaseInfo> database_info = GetCachedDatabaseByName(database_name);
    if (database_info == nullptr) {
        // try to refresh databases from SKV in case that the requested database is created by another catalog manager instance
        // this could be avoided by use a single or a quorum of catalog managers
        LoadDatabases();
        // recheck database
        database_info = GetCachedDatabaseByName(database_name);

    }
    return database_info;
}

std::shared_ptr<DatabaseInfo> SqlCatalogManager::CheckAndLoadDatabaseById(const std::string& database_id) {
    std::shared_ptr<DatabaseInfo> database_info = GetCachedDatabaseById(database_id);
    if (database_info == nullptr) {
        // try to refresh databases from SKV in case that the requested database is created by another catalog manager instance
            // this could be avoided by use a single or a quorum of catalog managers
        LoadDatabases();
        database_info = GetCachedDatabaseById(database_id);
    }
    return database_info;
}

} // namespace catalog
}  // namespace k2pg
