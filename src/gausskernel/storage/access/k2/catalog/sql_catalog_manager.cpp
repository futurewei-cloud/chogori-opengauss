/*
MIT License

Copyright(c) 2020 Futurewei Cloud

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

    if (auto status = NewTransaction(); !status.is2xxOK()) {
        return status;
    }

    // load cluster info
    auto [status, clusterInfo] = cluster_info_handler_.GetClusterInfo(cluster_id_);
    if (!status.is2xxOK()) {
        TXMgr.EndTxn(sh::dto::EndAction::Abort);
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
    TXMgr.EndTxn(sh::dto::EndAction::Commit);

    // load databases
    if (auto status = NewTransaction(); !status.is2xxOK()) return status;
    auto [list_status, infos]  = database_info_handler_.ListDatabases();
    TXMgr.EndTxn(sh::dto::EndAction::Commit);

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
    auto&& [ccResult] = TXMgr.CreateCollection(skv_collection_name_primary_cluster, primary_cluster_id);
    if (!ccResult.is2xxOK()) {
        K2LOG_E(log::catalog, "Failed to create SKV collection during initialization primary PG cluster due to {}", ccResult);
        return ccResult;
    }

    if (auto status = NewTransaction(); !status.is2xxOK()) return status;

    // step 2/4 Init Cluster info, including create the SKVSchema in the primary cluster's SKVCollection for cluster_info and insert current cluster info into
    //      Note: Initialize cluster info's init_db column with TRUE
    ClusterInfo cluster_info{cluster_id_, catalog_version_, false /*init_db_done*/};

    auto status = cluster_info_handler_.InitClusterInfo(cluster_info);
    if (!status.is2xxOK()) {
        TXMgr.EndTxn(sh::dto::EndAction::Abort);
        K2LOG_E(log::catalog, "Failed to initialize cluster info due to {}", status);
        return status;
    }

    // step 3/4 Init database_info - create the SKVSchema in the primary cluster's SKVcollection for database_info
    status = database_info_handler_.InitDatabaseTable();
    if (!status.is2xxOK()) {
        K2LOG_E(log::catalog, "Failed to initialize creating database table due to {}", status);
        return status;
    }
    TXMgr.EndTxn(sh::dto::EndAction::Commit);
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
            TXMgr.EndTxn(sh::dto::EndAction::Abort);
            K2LOG_E(log::catalog, "Cannot read cluster info record on SKV due to {}", status);
            return status;
        }

        if (clusterInfo.initdb_done) {
            TXMgr.EndTxn(sh::dto::EndAction::Commit);
            init_db_done_.store(true, std::memory_order_relaxed);
            K2LOG_D(log::catalog, "InitDbDone is already true on SKV");
            return sh::Statuses::S200_OK;
        }

        K2LOG_D(log::catalog, "Updating cluster info with initDbDone to be true");
        clusterInfo.initdb_done = true;
        status = cluster_info_handler_.UpdateClusterInfo(clusterInfo);
        if (!status.is2xxOK()) {
            TXMgr.EndTxn(sh::dto::EndAction::Abort);
            K2LOG_E(log::catalog, "Failed to update cluster info due to {}", status);
            return status;
        }
        TXMgr.EndTxn(sh::dto::EndAction::Commit);
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
    return catalog_version_;
}

sh::Response<ClusterInfo> SqlCatalogManager::GetClusterInfo(bool commit) {
    if (auto status = NewTransaction(); !status.is2xxOK())
        return std::make_tuple(status, ClusterInfo{});
    auto response = cluster_info_handler_.GetClusterInfo(cluster_id_);
    if (commit)
        TXMgr.EndTxn(sh::dto::EndAction::Commit);
    return response;
}

sh::Response<uint64_t> SqlCatalogManager::IncrementCatalogVersion() {
    std::lock_guard<std::mutex> l(lock_);
    // TODO: use a background thread to fetch the ClusterInfo record periodically instead of fetching it for each call
    auto [status, clusterInfo] = GetClusterInfo(false);
    if (!status.is2xxOK()) {
        K2LOG_E(log::catalog, "Failed to check cluster info due to {}", status);
        TXMgr.EndTxn(sh::dto::EndAction::Abort);
        return sh::Response<uint64_t>(status, 0);
    }

    K2LOG_D(log::catalog, "Found SKV catalog version: {}", clusterInfo.catalog_version);
    catalog_version_ = clusterInfo.catalog_version + 1;
    // need to update the catalog version on SKV
    // the update frequency could be reduced once we have a single or a quorum of catalog managers
    ClusterInfo new_cluster_info{cluster_id_, catalog_version_, init_db_done_};
    status = cluster_info_handler_.UpdateClusterInfo(new_cluster_info);
    if (!status.is2xxOK()) {
        TXMgr.EndTxn(sh::dto::EndAction::Abort);
        catalog_version_ = clusterInfo.catalog_version;
        K2LOG_D(log::catalog, "Failed to update catalog version due to {}, revert catalog version to {}", status, catalog_version_);
        return std::make_tuple(status, clusterInfo.catalog_version);
    }
    TXMgr.EndTxn(sh::dto::EndAction::Commit);
    K2LOG_D(log::catalog, "Increase catalog version to {}", catalog_version_);
    return std::make_tuple(status, (uint64_t) catalog_version_.load());
}


sh::Response<std::shared_ptr<DatabaseInfo>>  SqlCatalogManager::CreateDatabase(const CreateDatabaseRequest& request) {
    K2LOG_D(log::catalog,
    "Creating database with name: {}, id: {}, oid: {}, source_id: {}, nextPgOid: {}",
    request.databaseName, request.databaseId, request.databaseOid, request.sourceDatabaseId, request.nextPgOid.value_or(-1));
        // step 1/3:  check input conditions
        //      check if the target database has already been created, if yes, return already present
        //      check the source database is already there, if it present in the create requet
    std::shared_ptr<DatabaseInfo> database_info = CheckAndLoadDatabaseByName(request.databaseName);
    if (database_info != nullptr) {
        K2LOG_E(log::catalog, "Database {} has already existed", request.databaseName);
        return std::make_pair(sh::Statuses::S409_Conflict(fmt::format("Database {} has already existed", request.databaseName)), nullptr);
    }

    std::shared_ptr<DatabaseInfo> source_database_info = nullptr;
    uint32_t t_nextPgOid;
    // validate source database id and check source database to set nextPgOid properly
    if (!request.sourceDatabaseId.empty()) {
        source_database_info = CheckAndLoadDatabaseById(request.sourceDatabaseId);
        if (source_database_info == nullptr) {
            K2LOG_E(log::catalog, "Failed to find source databases {}", request.sourceDatabaseId);
            return std::make_pair(sh::Statuses::S404_Not_Found(fmt::format("Source database {} not found", request.sourceDatabaseId)), nullptr);
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
    auto [ccResult] = TXMgr.CreateCollection(request.databaseId, request.databaseName);
    if (!ccResult.is2xxOK()) {
        K2LOG_E(log::catalog, "Failed to create SKV collection {} due to {}", request.databaseId, ccResult);
        return std::make_pair(ccResult, nullptr);
    }

    // step 2.2 Add new database(database) entry into default cluster database table and update in-memory cache
    std::shared_ptr<DatabaseInfo> new_ns = std::make_shared<DatabaseInfo>();
    new_ns->database_id = request.databaseId;
    new_ns->database_name =  request.databaseName;
    new_ns->database_oid = request.databaseOid;
    new_ns->next_pg_oid = request.nextPgOid.value();
    // persist the new database record
    K2LOG_D(log::catalog, "Adding database {} on SKV", request.databaseId);
    if (auto status = NewTransaction(); !status.is2xxOK()) return std::make_pair(status, nullptr);
    auto status = database_info_handler_.UpsertDatabase(*new_ns);
    if (!status.is2xxOK()) {
        K2LOG_E(log::catalog, "Failed to add database {}, due to {}", request.databaseId, status);
        TXMgr.EndTxn(sh::dto::EndAction::Abort);
        return std::make_pair(std::move(status), nullptr);
    }
    // cache databases by database id and database name
    database_id_map_[new_ns->database_id] = new_ns;
    database_name_map_[new_ns->database_name] = new_ns;

    // step 2.3 Add new system tables for the new database(database)
    // TODO: Add system tables
    // step 3/3: If source database(database) is present in the request, copy all the rest of tables from source database(database)
    // TODO: Copy tables from source database
    // TODO: Support multiple txns
    // target_txnHandler->CommitTransaction();
    // ns_txnHandler->CommitTransaction();
    TXMgr.EndTxn(sh::dto::EndAction::Commit);
    K2LOG_D(log::catalog, "Created database {}", new_ns->database_id);
    return std::make_pair(sh::Statuses::S200_OK, new_ns);
}


sh::Response<std::vector<DatabaseInfo>> SqlCatalogManager::ListDatabases() {
    K2LOG_D(log::catalog, "Listing databases...");
    if (auto status = NewTransaction(); !status.is2xxOK())
        return std::make_pair(status, std::vector<DatabaseInfo>());
    auto [status, databaseInfos] = database_info_handler_.ListDatabases();
    if (!status.is2xxOK()) {
        TXMgr.EndTxn(sh::dto::EndAction::Abort);
        K2LOG_E(log::catalog, "Failed to list databases due to {}", status);
        return std::make_pair(status, databaseInfos);
    }
    TXMgr.EndTxn(sh::dto::EndAction::Commit);
    if (databaseInfos.empty()) {
        K2LOG_W(log::catalog, "No databases are found");
    } else {
        UpdateDatabaseCache(databaseInfos);
        K2LOG_D(log::catalog, "Found {} databases", databaseInfos.size());
    }
    return std::make_pair(status, databaseInfos);
 }

sh::Response<std::shared_ptr<DatabaseInfo>> SqlCatalogManager::GetDatabase(const std::string& databaseName, const std::string& databaseId) {
    K2LOG_D(log::catalog, "Getting database with name: {}, id: {}", databaseName, databaseId);
    if (std::shared_ptr<DatabaseInfo> database_info = GetCachedDatabaseById(databaseId); database_info != nullptr) {
        return std::make_pair(sh::Statuses::S200_OK, database_info);
    }
    if (auto status = NewTransaction(); !status.is2xxOK())
        return sh::Response<std::shared_ptr<DatabaseInfo>>(status, nullptr);
    // TODO: use a background task to refresh the database caches to avoid fetching from SKV on each call
    auto [status, databaseInfo] = database_info_handler_.GetDatabase(databaseId);
    if (!status.is2xxOK()) {
        TXMgr.EndTxn(sh::dto::EndAction::Abort);
        K2LOG_E(log::catalog, "Failed to get database {}, due to {}", databaseId, status);
        return sh::Response<std::shared_ptr<DatabaseInfo>>(status, nullptr);
    }
    TXMgr.EndTxn(sh::dto::EndAction::Commit);
    auto di = std::make_shared<DatabaseInfo>(std::move(databaseInfo));
    // update database caches
    database_id_map_[di->database_id] = di;
    database_name_map_[di->database_name] = di;
    K2LOG_D(log::catalog, "Found database {}", databaseId);
    return std::make_pair(status, di);
}

sh::Status SqlCatalogManager::DeleteDatabase(const std::string& databaseName, const std::string& databaseId) {
    K2LOG_D(log::catalog, "Deleting database with name: {}, id: {}", databaseName, databaseId);
    if (auto status = NewTransaction(); !status.is2xxOK()) return status;
    // TODO: use a background task to refresh the database caches to avoid fetching from SKV on each call
    auto [status, database_info] = database_info_handler_.GetDatabase(databaseId);
    TXMgr.EndTxn(sh::dto::EndAction::Commit);
    if (!status.is2xxOK()) {
        K2LOG_E(log::catalog, "Failed to get deletion target database {} {}.", databaseId, status);
        return status;
    }
    // No need to delete table data or metadata, it will be dropped with the SKV collection
    if (auto status = NewTransaction(); status.is2xxOK()) return status;
    status  = database_info_handler_.DeleteDatabase(database_info);
    if (!!status.is2xxOK()) {
        TXMgr.EndTxn(sh::dto::EndAction::Abort);
        return status;
    }
    TXMgr.EndTxn(sh::dto::EndAction::Commit);
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
    // check if the database exists
    std::shared_ptr<DatabaseInfo> database_info = CheckAndLoadDatabaseByName(databaseName);
    if (database_info == nullptr) {
        K2LOG_E(log::catalog, "Cannot find database {}", databaseName);
        return sh::Statuses::S404_Not_Found(fmt::format("Cannot find database {}", databaseName));
    }

    // TODO:
    // // preload tables for a database
    // thread_pool_.enqueue([this, request] () {
    //         K2LOG_I(log::catalog, "Preloading database {}", request.databaseName);
    //         Status result = CacheTablesFromStorage(request.databaseName, true /*isSysTableIncluded*/);
    //         if (!result.ok()) {
    //           K2LOG_W(log::catalog, "Failed to preloading database {} due to {}", request.databaseName, result.code());
    //         }
    //     });
    return sh::Statuses::S200_OK;
}

// update database caches
void SqlCatalogManager::UpdateDatabaseCache(const std::vector<DatabaseInfo>& database_infos) {
    std::lock_guard<std::mutex> l(lock_);
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

sh::Status SqlCatalogManager::NewTransaction() {
    auto result = TXMgr.BeginTxn({});
    return std::get<0>(result);
}

void SqlCatalogManager::LoadDatabases() {
    if (auto status = NewTransaction(); status.is2xxOK()) return;
    auto [status, databaseInfos] = database_info_handler_.ListDatabases();
    TXMgr.EndTxn(sh::dto::EndAction::Commit);
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
