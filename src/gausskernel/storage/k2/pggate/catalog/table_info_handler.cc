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

#include "table_info_handler.h"

#include <stdexcept>

namespace k2pg {
namespace sql {
namespace catalog {

TableInfoHandler::TableInfoHandler(std::shared_ptr<K2Adapter> k2_adapter)
 {
    table_meta_SKVSchema_ = std::make_shared<sh::dto::Schema>(skv_schema_table_meta);
    tablecolumn_meta_SKVSchema_ = std::make_shared<sh::dto::Schema>(skv_schema_tablecolumn_meta);
    indexcolumn_meta_SKVSchema_ = std::make_shared<sh::dto::Schema>(skv_schema_indexcolumn_meta);
    k2_adapter_ = k2_adapter;
}

TableInfoHandler::~TableInfoHandler() {
}

CreateMetaTablesResult TableInfoHandler::CreateMetaTables(std::shared_ptr<PgTxnHandler> txnHandler, const std::string& collection_name) {
    CreateMetaTablesResult response;
    try {
        auto [status] = k2_adapter_->CreateSchema(collection_name, table_meta_SKVSchema_);
        if (!status.is2xxOK()) {
            K2LOG_E(log::catalog, "Failed to create schema for {} in {}, due to {}",
                table_meta_SKVSchema_->name, collection_name, status);
            response.status = K2Adapter::K2StatusToK2PgStatus(status);
            return response;
        }

        std::tie(status) = k2_adapter_->CreateSchema(collection_name, tablecolumn_meta_SKVSchema_);
        if (!status.is2xxOK()) {
            K2LOG_E(log::catalog, "Failed to create schema for {} in {}, due to {}",
                tablecolumn_meta_SKVSchema_->name, collection_name, status);
            response.status = K2Adapter::K2StatusToK2PgStatus(status);
            return response;
        }

        std::tie(status) = k2_adapter_->CreateSchema(collection_name, indexcolumn_meta_SKVSchema_);
        if (!status.is2xxOK()) {
            K2LOG_E(log::catalog, "Failed to create schema for {} in {}, due to {}",
                indexcolumn_meta_SKVSchema_->name, collection_name, status);
            response.status = K2Adapter::K2StatusToK2PgStatus(status);
            return response;
        }

        response.status = Status(); // OK
        K2LOG_D(log::catalog, "Created SKV schemas for meta tables successfully");
    }
    catch (const std::exception& e) {
		response.status = STATUS_FORMAT(RuntimeError, "{}", e.what());
    }
    return response;
}

CreateUpdateTableResult TableInfoHandler::CreateOrUpdateTable(std::shared_ptr<PgTxnHandler> txnHandler, const std::string& collection_name, std::shared_ptr<TableInfo> table) {
    CreateUpdateTableResult response;
    try {
        // persist system catalog entries to sys tables on individual DB collection
        PersistTableMetaResult table_meta_result = PersistTableMeta(txnHandler, collection_name, table);
        if (!table_meta_result.status.ok()) {
            response.status = std::move(table_meta_result.status);
            return response;
        }

        // we only create(a new version of) the table(and its indexes) SKV schemas only when the table is not a "shared" table.
        // For now, we do not have scenario that a shared table need to upgrade schema.
        if (CatalogConsts::is_on_physical_collection(table->database_id(), table->is_shared())) {
            K2LOG_D(log::catalog, "Persisting table SKV schema id: {}, name: {} in {}", table->table_id(), table->table_name(), table->database_id());
            // persist SKV table and index schemas
            CreateSKVSchemaResult skv_schema_result = CreateTableSKVSchema(txnHandler, collection_name, table);
            if (!skv_schema_result.status.ok()) {
                response.status = std::move(skv_schema_result.status);
                K2LOG_E(log::catalog, "Failed to persist table SKV schema id: {}, name: {}, in {} due to {}", table->table_id(), table->table_name(),
                    table->database_id(), response.status);
                return response;
            }
        } else {
            K2LOG_D(log::catalog, "Skip persisting table SKV schema id: {}, name: {} in {}, shared: {}", table->table_id(), table->table_name(),
                table->database_id(), table->is_shared());
        }
        response.status = Status(); // OK
    }
    catch (const std::exception& e) {
		response.status = STATUS_FORMAT(RuntimeError, "{}", e.what());
    }
    return response;
}

GetTableResult TableInfoHandler::GetTable(std::shared_ptr<PgTxnHandler> txnHandler, const std::string& collection_name, const std::string& database_name,
        const std::string& table_id) {
    GetTableResult response;
    try {
        K2LOG_D(log::catalog, "Fetch table schema in skv collection: {}, db name: {}, table id: {}", collection_name, database_name, table_id);
        // table meta data on super tables that we owned are always on individual collection even for shared tables/indexes
        // (but their actual skv schema and table content are not)
        sh::dto::SKVRecord table_meta_record;
        response.status = FetchTableMetaSKVRecord(txnHandler, collection_name, table_id, table_meta_record);
        if (!response.status.ok()) {
            return response;
        }
        std::vector<sh::dto::SKVRecord> table_column_records = FetchTableColumnMetaSKVRecords(txnHandler, collection_name, table_id);
        std::shared_ptr<TableInfo> table_info = BuildTableInfo(collection_name, database_name, table_meta_record, table_column_records);
        // check all the indexes whose BaseTableId is table_id
        std::vector<sh::dto::SKVRecord> index_records = FetchIndexMetaSKVRecords(txnHandler, collection_name, table_id);
        if (!index_records.empty()) {
            // table has indexes defined
            for (auto& index_record : index_records) {
                // Fetch and build each index info
                IndexInfo index_info = BuildIndexInfo(txnHandler, collection_name, index_record);
                // populate index to table_info
                table_info->add_secondary_index(index_info.table_id(), index_info);
            }
        }

        response.status = Status(); // OK
        response.tableInfo = table_info;
    }
    catch (const std::exception& e) {
        response.status = STATUS_FORMAT(RuntimeError, "{}", e.what());
    }
    return response;
}

ListTablesResult TableInfoHandler::ListTables(std::shared_ptr<PgTxnHandler> txnHandler, const std::string& collection_name, const std::string& database_name, bool isSysTableIncluded) {
    ListTablesResult response;
    try {
        ListTableIdsResult list_result = ListTableIds(txnHandler, collection_name, isSysTableIncluded);
        if (!list_result.status.ok()) {
            response.status = std::move(list_result.status);
            return response;
        }
        for (auto& table_id : list_result.tableIds) {
            GetTableResult result = GetTable(txnHandler, collection_name, database_name, table_id);
            if (!result.status.ok()) {
                response.status = std::move(result.status);
                return response;
            }
            response.tableInfos.push_back(std::move(result.tableInfo));
        }
        response.status = Status(); // OK
    }
    catch (const std::exception& e) {
		response.status = STATUS_FORMAT(RuntimeError, "{}", e.what());
    }

    return response;
}

ListTableIdsResult TableInfoHandler::ListTableIds(std::shared_ptr<PgTxnHandler> txnHandler, const std::string& collection_name, bool isSysTableIncluded) {
    ListTableIdsResult response;
    try {
        
        auto&& startKey =  buildRangeRecord(collection_name, table_meta_SKVSchema_, CatalogConsts::oid_table_meta, 0/*index_oid*/, std::nullopt);
        auto&& endKey =  buildRangeRecord(collection_name, table_meta_SKVSchema_, CatalogConsts::oid_table_meta, 0/*index_oid*/, std::nullopt);
        
        // TODO: consider to use the same additional table_id /index_id fields for the fixed system tables
        // find all the tables (not include indexes)
        std::vector<sh::dto::expression::Value> values;
        values.emplace_back(sh::dto::expression::makeValueReference("IsIndex"));
        values.emplace_back(sh::dto::expression::makeValueLiteral<bool>(false));
        sh::dto::expression::Expression filterExpr = sh::dto::expression::makeExpression(sh::dto::expression::Operation::EQ, std::move(values), {});

        auto txn = txnHandler->GetTxn();
        auto [status, query] = k2_adapter_->CreateScanRead(txn, collection_name,  table_meta_SKVSchema_->name, startKey, endKey, std::move(filterExpr));;
        
        if (!status.is2xxOK()) {
            K2LOG_E(log::catalog, "Failed to create scan read due to {}", status);
            response.status = K2Adapter::K2StatusToK2PgStatus(status);
            return response;
        }

        bool done = false;
        do {
            auto&& [status, result]  = k2_adapter_->ScanRead(txn, query);
            if (!status.is2xxOK()) {
                K2LOG_E(log::catalog, "Failed to run scan read due to {}", status);
                response.status = K2Adapter::K2StatusToK2PgStatus(status);
                return response;
            }
            done = result.done;
            for (sh::dto::SKVRecord::Storage& storage : result.records) {
                // TODO(ahsank): Query response should return schema or shcema version to support multi version schema
                sh::dto::SKVRecord record(collection_name, table_meta_SKVSchema_, std::move(storage), true);
                // deserialize table meta
                // SchemaTableId
                record.deserializeNext<int64_t>();
                // SchemaIndexId
                record.deserializeNext<int64_t>();
                // TableId
                std::string table_id = record.deserializeNext<sh::String>().value();
                // TableName
                record.deserializeNext<sh::String>();
                // TableOid
                record.deserializeNext<int64_t>();
                // TableUuid
                record.deserializeNext<sh::String>();
                // IsSysTable
                bool is_sys_table = record.deserializeNext<bool>().value();
                if (isSysTableIncluded) {
                    response.tableIds.push_back(std::move(table_id));
                } else {
                    if (!is_sys_table) {
                        response.tableIds.push_back(std::move(table_id));
                    }
                }
            }
            // if the query is not done, the query itself is updated with the pagination token for the next call
        } while (!done);

        response.status = Status(); // OK
    }
    catch (const std::exception& e) {
		response.status = STATUS_FORMAT(RuntimeError, "{}", e.what());
    }
    return response;
}

CopyTableResult TableInfoHandler::CopyTable(std::shared_ptr<PgTxnHandler> target_txnHandler,
            const std::string& target_coll_name,
            const std::string& target_database_name,
            uint32_t target_database_oid,
            std::shared_ptr<PgTxnHandler> source_txnHandler,
            const std::string& source_coll_name,
            const std::string& source_database_name,
            const std::string& source_table_id) {
    CopyTableResult response;
    try {
        GetTableResult table_result = GetTable(source_txnHandler, source_coll_name, source_database_name, source_table_id);
        if (!table_result.status.ok()) {
            response.status = std::move(table_result.status);
            return response;
        }

        // step 1/2 create target table (including secondary indexes) in target database, cloning the table info/meta from source table info/meta
        std::shared_ptr<TableInfo> source_table = table_result.tableInfo;
        PgOid source_table_oid = table_result.tableInfo->table_oid();
        std::string target_table_uuid = PgObjectId::GetTableUuid(target_database_oid, table_result.tableInfo->table_oid());
        std::shared_ptr<TableInfo> target_table = TableInfo::Clone(table_result.tableInfo, target_coll_name,
                target_database_name, target_table_uuid, table_result.tableInfo->table_name());

        CreateUpdateTableResult create_result = CreateOrUpdateTable(target_txnHandler, target_coll_name, target_table);
        if (!create_result.status.ok()) {
            response.status = std::move(create_result.status);
            return response;
        }

        // step 2/2 copy all data rows (when the table is not a shared table across databases)
        if(source_table->is_shared()) {  // skip data copy if it is shared
            K2LOG_D(log::catalog, "Skip copying shared table {} in {}", source_table_id, source_coll_name);
            if(source_table->has_secondary_indexes()) {
                // the indexes for a shared table should be shared as well
                for (std::pair<TableId, IndexInfo> secondary_index : source_table->secondary_indexes()) {
                    K2ASSERT(log::catalog, secondary_index.second.is_shared(), "Index for a shared table must be shared");
                    K2LOG_D(log::catalog, "Skip copying shared index {} in {}", secondary_index.first, source_coll_name);
                }
            }
        } else {  // copy all base table and index rows(SKV record in K2)
            CopySKVTableResult copy_skv_table_result = CopySKVTable(target_txnHandler, target_coll_name, target_table->table_id(), target_table->schema().version(),
                source_txnHandler, source_coll_name, source_table_id, source_table->schema().version(), source_table_oid, 0 /*index_oid*/);
            if (!copy_skv_table_result.status.ok()) {
                response.status = std::move(copy_skv_table_result.status);
                return response;
            }

            if(source_table->has_secondary_indexes()) {
                std::unordered_map<std::string, IndexInfo*> target_index_name_map;
                for (std::pair<TableId, IndexInfo> secondary_index : target_table->secondary_indexes()) {
                    target_index_name_map[secondary_index.second.table_name()] = &secondary_index.second;
                }
                for (std::pair<TableId, IndexInfo> secondary_index : source_table->secondary_indexes()) {
                    K2ASSERT(log::catalog, !secondary_index.second.is_shared(), "Index for a non-shared table must not be shared");
                    // search for target index by name
                    auto found = target_index_name_map.find(secondary_index.second.table_name());
                    if (found == target_index_name_map.end()) {
                        response.status = STATUS_FORMAT(NotFound, "Cannot find target index {}", secondary_index.second.table_name());
                        return response;
                    }
                    IndexInfo* target_index = found->second;
                    CopySKVTableResult copy_skv_index_result = CopySKVTable(target_txnHandler, target_coll_name, secondary_index.first, secondary_index.second.version(),
                        source_txnHandler, source_coll_name, target_index->table_id(), target_index->version(), source_table_oid/*baseTableId*/, target_index->table_oid()/*index_oid, should be source, but same as target index oid*/);
                    if (!copy_skv_index_result.status.ok()) {
                        response.status = std::move(copy_skv_index_result.status);
                        return response;
                    }
                    response.num_index++;
                }
            }
        }

        K2LOG_D(log::catalog, "Copied table from {} in {} to {} in {}", source_table_id, source_coll_name, target_table->table_id(), target_coll_name);
        response.tableInfo = target_table;
        response.status = Status(); // OK
    } catch (const std::exception& e) {
        response.status = STATUS_FORMAT(RuntimeError, "{}", e.what());
    }
    return response;
}

CopySKVTableResult TableInfoHandler::CopySKVTable(std::shared_ptr<PgTxnHandler> target_txnHandler,
            const std::string& target_coll_name,
            const std::string& target_schema_name,
            uint32_t target_version,
            std::shared_ptr<PgTxnHandler> source_txnHandler,
            const std::string& source_coll_name,
            const std::string& source_schema_name,
            uint32_t source_version,
            PgOid source_table_oid,
            PgOid source_index_oid) {
    CopySKVTableResult response;
    // check target SKV schema

    auto [target_status, target_schema] = k2_adapter_->GetSchema(target_coll_name, target_schema_name, target_version);
    if (!target_status.is2xxOK()) {
        K2LOG_E(log::catalog, "Failed to get SKV schema for table {} in {} with version {} due to {}",
            target_schema_name, target_coll_name, target_version,target_status);
        response.status = K2Adapter::K2StatusToK2PgStatus(target_status);
        return response;
    }

    // check the source SKV schema
    auto [source_status, source_schema] = k2_adapter_->GetSchema(source_coll_name, source_schema_name, source_version);
    if (!source_status.is2xxOK()) {
        K2LOG_E(log::catalog, "Failed to get SKV schema for table {} in {} with version {} due to {}",
            source_schema_name, source_coll_name, source_version, source_status);
        response.status = K2Adapter::K2StatusToK2PgStatus(source_status);
        return response;
    }
    auto startScanRecord = buildRangeRecord(source_coll_name, source_schema, source_table_oid, source_index_oid, std::nullopt);
    auto endScanRecord = buildRangeRecord(source_coll_name, source_schema, source_table_oid, source_index_oid, std::nullopt);
    auto txn = source_txnHandler->GetTxn();
    // create scan for source table
    auto [status, query]  = k2_adapter_->CreateScanRead(txn, source_coll_name, source_schema->name, startScanRecord, endScanRecord);
    if (!status.is2xxOK()) {
        K2LOG_E(log::catalog, "Failed to create scan read for {} in {} due to {}", source_schema_name, source_coll_name, status.message);
        response.status = K2Adapter::K2StatusToK2PgStatus(status);
        return response;
    }

    // scan the source table
    int count = 0;
    bool done = false;
    do {
        auto [status, query_result] = k2_adapter_->ScanRead(txn, query);
        if (!status.is2xxOK()) {
            K2LOG_E(log::catalog, "Failed to run scan read for table {} in {} due to {}",
                source_schema_name, source_coll_name, status);
            response.status = K2Adapter::K2StatusToK2PgStatus(status);
            return response;
        }

        for (sh::dto::SKVRecord::Storage& storage : query_result.records) {
            // clone and persist SKV record to target table
            sh::dto::SKVRecord target_record(target_coll_name, target_schema, std::move(storage), true); 
            auto [upsert_status] = k2_adapter_->UpsertRecord(target_txnHandler->GetTxn(), target_record);
            if (!upsert_status.is2xxOK())
            {
                K2LOG_E(log::catalog, "Failed to upsert target_record due to {}", upsert_status);
                response.status = K2Adapter::K2StatusToK2PgStatus(upsert_status);
                return response;
            }
            count++;
        }
        done = query_result.done;
    } while (!done);
    K2LOG_I(log::catalog, "Finished copying {} in {} to {} in {} with {} records", source_schema_name, source_coll_name, target_schema_name, target_coll_name, count);
    response.status = Status(); // OK
    return response;
}


// A SKV Schema of perticular version is not mutable, thus, we only create a new specified version if that version doesn't exists yet
CreateSKVSchemaResult TableInfoHandler::CreateTableSKVSchema(std::shared_ptr<PgTxnHandler> txnHandler, const std::string& collection_name, std::shared_ptr<TableInfo> table) {
    CreateSKVSchemaResult response;
    try {
        // use table id (string) instead of table name as the schema name
        std::shared_ptr<sh::dto::Schema> outSchema = nullptr;
        auto [status, schema] = k2_adapter_->GetSchema(collection_name, table->table_id(), table->schema().version());
        if (status.is2xxOK()) //  schema of specified version already exists, no-op.
        {
            K2LOG_W(log::catalog, "SKV schema {} with specified version already exists in {}, skipping creation", table->table_id(), table->schema().version(), collection_name);
            response.status = Status();  // response.created = false;
            return response;
        }

        // !result.status.is2xxOK() here and 404 Not Found is ok, as we are going to create it, otherwise error out
        if (status.code != 404)  {
            K2LOG_E(log::catalog, "Failed to getSchema {} in collection {} due to {}", table->table_id(), collection_name, status);
            response.status = K2Adapter::K2StatusToK2PgStatus(status);
            return response;
        }

        // build the SKV schema from TableInfo, i.e., PG table schema
        std::shared_ptr<sh::dto::Schema> tablecolumn_schema = DeriveSKVSchemaFromTableInfo(table);
        auto [createStatus] = k2_adapter_->CreateSchema(collection_name, tablecolumn_schema);
        if (!createStatus.is2xxOK()) {
            K2LOG_E(log::catalog, "Failed to create schema for {} in {}, due to {}",
                tablecolumn_schema->name, collection_name, status);
            response.status = K2Adapter::K2StatusToK2PgStatus(createStatus);
            return response;
        }

        if (table->has_secondary_indexes()) {
            std::vector<std::shared_ptr<sh::dto::Schema>> index_schemas = DeriveIndexSchemas(table);
            for (std::shared_ptr<sh::dto::Schema> index_schema : index_schemas) {
                if (!index_schema) {
                    K2LOG_W(log::catalog, "Failed to create index schema in {} due to unsupported feature",
                        collection_name);
                    response.status = STATUS_PG(InvalidArgument, "Unsupported feature in secondary index");
                    return response;
                }

                // TODO use sequential SKV writes for now, could optimize this later
                std::tie(createStatus) = k2_adapter_->CreateSchema(collection_name, index_schema);
                if (!createStatus.is2xxOK()) {
                    K2LOG_E(log::catalog, "Failed to create index schema for {} in {}, due to {}",
                        index_schema->name, collection_name, status);
                    response.status = K2Adapter::K2StatusToK2PgStatus(createStatus);
                    return response;
                }
            }
        }

        response.status = Status(); // OK
    }
    catch (const std::exception& e) {
		response.status = STATUS_FORMAT(RuntimeError, "{}", e.what());
    }

    return response;
}

CreateSKVSchemaResult TableInfoHandler::CreateIndexSKVSchema(std::shared_ptr<PgTxnHandler> txnHandler, const std::string& collection_name,
        std::shared_ptr<TableInfo> table, const IndexInfo& index_info) {
    CreateSKVSchemaResult response;
    try {
        std::shared_ptr<sh::dto::Schema> index_schema = DeriveIndexSchema(index_info);
        auto [createStatus] = k2_adapter_->CreateSchema(collection_name, index_schema);
        if (!createStatus.is2xxOK()) {
            K2LOG_E(log::catalog, "Failed to create index schema for {} in {}, due to {}",
                index_schema->name, collection_name, createStatus);
            response.status = K2Adapter::K2StatusToK2PgStatus(createStatus);
            return response;
        }
        response.status = K2Adapter::K2StatusToK2PgStatus(createStatus);
    }
    catch (const std::exception& e) {
		response.status = STATUS_FORMAT(RuntimeError, "{}", e.what());
    }
    return response;
}

PersistTableMetaResult TableInfoHandler::PersistTableMeta(std::shared_ptr<PgTxnHandler> txnHandler, const std::string& collection_name, std::shared_ptr<TableInfo> table) {
    PersistTableMetaResult response;
    try {
        // use sequential SKV writes for now, could optimize this later
        sh::dto::SKVRecord tablelist_table_record = DeriveTableMetaRecord(collection_name, table);

        auto [status] = k2_adapter_->UpsertRecord(txnHandler->GetTxn(), tablelist_table_record);
        if (!status.is2xxOK())
        {
            K2LOG_E(log::catalog, "Failed to upsert tablelist_table_record due to {}", status);
            response.status = K2Adapter::K2StatusToK2PgStatus(status);
            return response;
        }
        std::vector<sh::dto::SKVRecord> table_column_records = DeriveTableColumnMetaRecords(collection_name, table);
        for (auto& table_column_record : table_column_records) {
            std::tie(status) = k2_adapter_->UpsertRecord(txnHandler->GetTxn(), table_column_record);
            if (!status.is2xxOK())
            {
                K2LOG_E(log::catalog, "Failed to upsert table_column_record  due to {}",  status);
                response.status = K2Adapter::K2StatusToK2PgStatus(status);
                return response;
            }
        }
        if (table->has_secondary_indexes()) {
            for(const auto& pair : table->secondary_indexes()) {
                PersistIndexMetaResult index_result = PersistIndexMeta(txnHandler, collection_name, table, pair.second);
                if (!index_result.status.ok()) {
                    response.status = std::move(index_result.status);
                    return response;
                }
            }
        }
        response.status = Status(); //OK
    }
    catch (const std::exception& e) {
        response.status = STATUS_FORMAT(RuntimeError, "{}", e.what());
    }
    return response;
}

PersistIndexMetaResult TableInfoHandler::PersistIndexMeta(std::shared_ptr<PgTxnHandler> txnHandler, const std::string& collection_name, std::shared_ptr<TableInfo> table,
        const IndexInfo& index_info) {
    PersistIndexMetaResult response;
    try {
        sh::dto::SKVRecord tablelist_index_record = DeriveTableMetaRecordOfIndex(collection_name, index_info, table->is_sys_table(), table->next_column_id());
        K2LOG_D(log::catalog, "Persisting SKV record tablelist_index_record id: {}, name: {}",
            index_info.table_id(), index_info.table_name());
        auto [status] = k2_adapter_->UpsertRecord(txnHandler->GetTxn(), tablelist_index_record);
        if (!status.is2xxOK())
        {
            K2LOG_E(log::catalog, "Failed to upsert tablelist_index_record due to {}", status);
            response.status = K2Adapter::K2StatusToK2PgStatus(status);
            return response;
        }

        std::vector<sh::dto::SKVRecord> index_column_records = DeriveIndexColumnMetaRecords(collection_name, index_info, table->schema());
        for (auto& index_column_record : index_column_records) {
            K2LOG_D(log::catalog, "Persisting SKV record index_column_record id: {}, name: {}",
                index_info.table_id(), index_info.table_name());
            std::tie(status) = k2_adapter_->UpsertRecord(txnHandler->GetTxn(), index_column_record);
            if (!status.is2xxOK())
            {
                K2LOG_E(log::catalog, "Failed to upsert index_column_record due to {}", status);
                response.status = K2Adapter::K2StatusToK2PgStatus(status);
                return response;
            }
        }

        response.status = Status(); // OK
    }
    catch (const std::exception& e) {
		response.status = STATUS_FORMAT(RuntimeError, "{}", e.what());
    }
    return response;
}

// Delete table_info and the related index_info from three meta tables (in this db)
DeleteTableResult TableInfoHandler::DeleteTableMetadata(std::shared_ptr<PgTxnHandler> txnHandler, const std::string& collection_name, std::shared_ptr<TableInfo> table) {
    DeleteTableResult response;
    try {
        // first delete indexes
        std::vector<sh::dto::SKVRecord> index_records = FetchIndexMetaSKVRecords(txnHandler, collection_name, table->table_id());
        if (!index_records.empty()) {
            for (sh::dto::SKVRecord& record : index_records) {
                // SchemaTableId
                record.deserializeNext<int64_t>();
                // SchemaIndexId
                record.deserializeNext<int64_t>();
                // get table id for the index
                std::string index_id = record.deserializeNext<sh::String>().value();
                // delete meta of index including that of index columns
                DeleteIndexResult index_result = DeleteIndexMetadata(txnHandler, collection_name, index_id);
                if (!index_result.status.ok()) {
                    response.status = std::move(index_result.status);
                    return response;
                }
            }
        }

        // then delete the table metadata itself
        // first, fetch the table columns
        std::vector<sh::dto::SKVRecord> table_columns = FetchTableColumnMetaSKVRecords(txnHandler, collection_name, table->table_id());
        for (auto& record : table_columns) {
            auto [status] = k2_adapter_->DeleteRecord(txnHandler->GetTxn(), record);
            if (!status.is2xxOK()) {
                response.status = K2Adapter::K2StatusToK2PgStatus(status);
                return response;
            }
        }
        // fetch table meta
        sh::dto::SKVRecord table_meta;
        response.status = FetchTableMetaSKVRecord(txnHandler, collection_name, table->table_id(), table_meta);
        if (!response.status.ok())
        {
            return response;
        }
        // then delete table meta record
        auto [status] = k2_adapter_->DeleteRecord(txnHandler->GetTxn(), table_meta);
        if (!status.is2xxOK()) {
            K2LOG_E(log::catalog, "Failed to delete tablemeta {} in Collection {}, due to {}",
                table->table_id(), collection_name, status);
            response.status = K2Adapter::K2StatusToK2PgStatus(status);
            return response;
        }

        response.status = Status(); // OK

    }
    catch (const std::exception& e) {
		response.status = STATUS_FORMAT(RuntimeError, "{}", e.what());
    }
    return response;
}

// Delete the actual table records from SKV that are stored with the SKV schema name to be table_id as in table_info
DeleteTableResult TableInfoHandler::DeleteTableData(std::shared_ptr<PgTxnHandler> txnHandler, const std::string& collection_name, std::shared_ptr<TableInfo> table) {
    DeleteTableResult response;
    try {
        // TODO: add a task to delete the actual data from SKV

        response.status = Status(); // OK
    }
    catch (const std::exception& e) {
		response.status = STATUS_FORMAT(RuntimeError, "{}", e.what());
    }
    return response;
}

// Delete index_info from tablemeta and indexcolumnmeta tables
DeleteIndexResult TableInfoHandler::DeleteIndexMetadata(std::shared_ptr<PgTxnHandler> txnHandler, const std::string& collection_name, const std::string& index_id) {
    DeleteIndexResult response;
    try {
        // fetch and delete index columns first
        std::vector<sh::dto::SKVRecord> index_columns = FetchIndexColumnMetaSKVRecords(txnHandler, collection_name, index_id);
        for (auto& record : index_columns) {
            auto [status] = k2_adapter_->DeleteRecord(txnHandler->GetTxn(), record);
            if (!status.is2xxOK()) {
                response.status = K2Adapter::K2StatusToK2PgStatus(status);
                return response;
            }
        }

        // fetch and delete index's table meta
        sh::dto::SKVRecord index_table_meta;
        response.status = FetchTableMetaSKVRecord(txnHandler, collection_name, index_id, index_table_meta);
        if (!response.status.ok())
        {
            return response;
        }

        auto [status] = k2_adapter_->DeleteRecord(txnHandler->GetTxn(), index_table_meta);
        if (!status.is2xxOK()) {
            K2LOG_E(log::catalog, "Failed to delete indexhead {} in Collection {}, due to {}",
                index_id, collection_name, status);
            response.status = K2Adapter::K2StatusToK2PgStatus(status);
            return response;
        }

        response.status = Status(); // OK
    }
    catch (const std::exception& e) {
		response.status = STATUS_FORMAT(RuntimeError, "{}", e.what());
    }

    return response;
}

// Delete the actual index records from SKV that are stored with the SKV schema name to be table_id as in index_info
DeleteIndexResult TableInfoHandler::DeleteIndexData(std::shared_ptr<PgTxnHandler> txnHandler, const std::string& collection_name, const std::string& index_id) {
    DeleteIndexResult response;
    try {
        // TODO: add a task to delete the actual data from SKV

        response.status = Status(); // OK
    }
    catch (const std::exception& e) {
		response.status = STATUS_FORMAT(RuntimeError, "{}", e.what());
    }
    return response;
}

GetBaseTableIdResult TableInfoHandler::GetBaseTableId(std::shared_ptr<PgTxnHandler> txnHandler, const std::string& collection_name, const std::string& index_id) {
    GetBaseTableIdResult response;
    try {
        // exception would be thrown if the record could not be found
        sh::dto::SKVRecord index_table_meta;
        response.status = FetchTableMetaSKVRecord(txnHandler, collection_name, index_id, index_table_meta);
        if (!response.status.ok())
        {
            return response;
        }
        // SchemaTableId
        index_table_meta.deserializeNext<int64_t>();
        // SchemaIndexId
        index_table_meta.deserializeNext<int64_t>();
        // TableId
        index_table_meta.deserializeNext<sh::String>();
        // TableName
        index_table_meta.deserializeNext<sh::String>();
        // TableOid
        index_table_meta.deserializeNext<int64_t>();
        // TableUuid
        index_table_meta.deserializeNext<sh::String>();
        // IsSysTable
        index_table_meta.deserializeNext<bool>();
        // IsShared
        index_table_meta.deserializeNext<bool>();
        // IsIndex
        index_table_meta.deserializeNext<bool>();
        // IsUnique
        index_table_meta.deserializeNext<bool>();
        // BaseTableId
        response.baseTableId = index_table_meta.deserializeNext<sh::String>().value();
        response.status = Status(); // OK
    } catch (const std::exception& e) {
		response.status = STATUS_FORMAT(RuntimeError, "{}", e.what());
    }

    return response;
}

GetTableTypeInfoResult TableInfoHandler::GetTableTypeInfo(std::shared_ptr<PgTxnHandler> txnHandler, const std::string& collection_name, const std::string& table_id)
{
    GetTableTypeInfoResult response;
    try {
        sh::dto::SKVRecord record;
        response.status = FetchTableMetaSKVRecord(txnHandler, collection_name, table_id, record);
        if (!response.status.ok())
        {
            return response;
        }
        // SchemaTableId
        record.deserializeNext<int64_t>();
        // SchemaIndexId
        record.deserializeNext<int64_t>();
        // TableId
        record.deserializeNext<sh::String>();
        // TableName
        record.deserializeNext<sh::String>();
        // TableOid
        record.deserializeNext<int64_t>();
        // TableUuid
        record.deserializeNext<sh::String>();
        // IsSysTable
        record.deserializeNext<bool>();
        // IsShared
        response.isShared = record.deserializeNext<bool>().value();
        // IsIndex
        response.isIndex = record.deserializeNext<bool>().value();
        response.status = Status(); // OK
    } catch (const std::exception& e) {
		response.status = STATUS_FORMAT(RuntimeError, "{}", e.what());
    }

    return response;
}

CreateIndexTableResult TableInfoHandler::CreateIndexTable(std::shared_ptr<PgTxnHandler> txnHandler, std::shared_ptr<DatabaseInfo> database_info, std::shared_ptr<TableInfo> base_table_info, CreateIndexTableParams &index_params) {
    CreateIndexTableResult result;
    std::string index_table_id = PgObjectId::GetTableId(index_params.table_oid);
    std::string index_table_uuid = PgObjectId::GetTableUuid(database_info->GetDatabaseOid(), index_params.table_oid);

    K2LOG_D(log::catalog, "Creating index ns name: {}, index name: {}, base table oid: {}",
            database_info->GetDatabaseId(), index_params.index_name, base_table_info->table_oid());

    if (base_table_info->has_secondary_indexes()) {
        const IndexMap& index_map = base_table_info->secondary_indexes();
        const auto itr = index_map.find(index_table_id);
        // the index has already been defined
        if (itr != index_map.end()) {
            // return if 'create .. if not exist' clause is specified
            if (index_params.is_not_exist) {
                const IndexInfo& index_info = itr->second;
                result.indexInfo = std::make_shared<IndexInfo>(index_info);
                result.status = Status(); // OK;
                return result;
            } else {
                // return index already present error if index already exists
                result.status = STATUS_FORMAT(AlreadyPresent, "index {} has already existed in ns {}",
                        index_table_id, database_info->GetDatabaseId());
                K2LOG_E(log::catalog,"index {} has already existed in ns {}", index_table_id, database_info->GetDatabaseId());
                return result;
            }
        }
    }
    try {
       // use default index permission, could be customized by user/api
        IndexInfo new_index_info = BuildIndexInfo(base_table_info, database_info->GetDatabaseId(), index_params.table_oid, index_table_uuid,
                index_params.index_schema, index_params.is_unique, index_params.is_shared, index_params.index_permissions);

        K2LOG_D(log::catalog, "Persisting index table id: {}, name: {} in {}", new_index_info.table_id(), new_index_info.table_name(), database_info->GetDatabaseId());
        // persist the index metadata to the system catalog SKV tables
        PersistIndexMetaResult index_meta_result = PersistIndexMeta(txnHandler, database_info->GetDatabaseId(), base_table_info, new_index_info);
        if (!index_meta_result.status.ok())
        {
                result.status = std::move(index_meta_result.status);
                K2LOG_E(log::catalog, "Failed to persist index meta {}, name: {}, in {} due to {}", new_index_info.table_id(), new_index_info.table_name(),
                    database_info->GetDatabaseId(), result.status);
                return result;
        }

        if (CatalogConsts::is_on_physical_collection(database_info->GetDatabaseId(), new_index_info.is_shared())) {
            K2LOG_D(log::catalog, "Persisting index SKV schema id: {}, name: {} in {}", new_index_info.table_id(), new_index_info.table_name(), database_info->GetDatabaseId());
            // create a SKV schema to insert the actual index data
            CreateSKVSchemaResult skv_schema_result = CreateIndexSKVSchema(txnHandler, database_info->GetDatabaseId(), base_table_info, new_index_info);
            if (!skv_schema_result.status.ok()) {
                result.status = std::move(skv_schema_result.status);
                K2LOG_E(log::catalog, "Failed to persist index SKV schema id: {}, name: {}, in {} due to {}", new_index_info.table_id(), new_index_info.table_name(),
                    database_info->GetDatabaseId(), result.status);
                return result;
            }
        } else {
            K2LOG_D(log::catalog, "Skip persisting index SKV schema id: {}, name: {} in {}, shared: {}", new_index_info.table_id(), new_index_info.table_name(),
                database_info->GetDatabaseId(), new_index_info.is_shared());
        }

        // update the base table with the new index
        base_table_info->add_secondary_index(new_index_info.table_id(), new_index_info);

        if (!index_params.skip_index_backfill) {
            // TODO: add logic to backfill the index
            K2LOG_W(log::catalog, "Index backfill is not supported yet");
        }

        result.status = Status();
        result.indexInfo = std::make_shared<IndexInfo>(new_index_info);
    } catch (const std::exception& e) {
        result.status = STATUS_FORMAT(RuntimeError, "Failed to create index {} due to {} in {}",
            index_params.index_name, e.what(), database_info->GetDatabaseId());
        K2LOG_E(log::catalog, "Failed to create index {} in {}",
            index_params.index_name, database_info->GetDatabaseId());
    }
    return result;
}


GetTableSchemaResult TableInfoHandler::GetTableSchema(std::shared_ptr<PgTxnHandler> txnHandler, std::shared_ptr<DatabaseInfo> database_info, const std::string& table_id, std::shared_ptr<IndexInfo> index_info, std::function<std::shared_ptr<DatabaseInfo>(const std::string&)> fnc_db, std::function<std::shared_ptr<PgTxnHandler>()> fnc_tx)
{
    GetTableSchemaResult result;
    std::shared_ptr<PgTxnHandler> localTxnHandler = txnHandler;
    std::shared_ptr<DatabaseInfo> local_database_info = database_info;

    K2LOG_D(log::catalog, "Checking if table {} is an index or not", table_id);
    GetTableTypeInfoResult table_type_info_result = GetTableTypeInfo(localTxnHandler, local_database_info->GetDatabaseId(), table_id);
    if (!table_type_info_result.status.ok()) {
        localTxnHandler->AbortTransaction();
        K2LOG_E(log::catalog, "Failed to check table {} in ns {}, due to {}",
                table_id, local_database_info->GetDatabaseId(), table_type_info_result.status);
        result.status = std::move(table_type_info_result.status);
        result.tableInfo = nullptr;
        return result;
    }

    // check the physical collection for a table
    std::string physical_collection = CatalogConsts::physical_collection(local_database_info->GetDatabaseId(), table_type_info_result.isShared);
    if (table_type_info_result.isShared) {
        // check if the shared table is stored on a different collection
        if (physical_collection.compare(database_info->GetDatabaseId()) != 0) {
            // shared table is on a different collection, first finish the existing collection
            localTxnHandler->CommitTransaction();
            K2LOG_I(log::catalog, "Shared table {} is not in {} but in {} instead", table_id, database_info->GetDatabaseId(), physical_collection);
            // load the shared database info
            local_database_info = fnc_db(physical_collection);
            if (database_info == nullptr) {
                K2LOG_E(log::catalog, "Cannot find database {} for shared table {}", physical_collection, table_id);
                result.status = STATUS_FORMAT(NotFound, "Cannot find database {} for shared table {}", physical_collection, table_id);
                return result;
            }
            // start a new transaction for the shared table collection since SKV does not support cross collection transaction yet
            localTxnHandler = fnc_tx();
        }
    }

    if (!table_type_info_result.isIndex) {
        K2LOG_D(log::catalog, "Fetching table schema {} in ns {}", table_id, physical_collection);
        // the table id belongs to a table
        GetTableResult table_result = GetTable(localTxnHandler, physical_collection, local_database_info->GetDatabaseName(),
            table_id);
        if (!table_result.status.ok()) {
            localTxnHandler->AbortTransaction();
            K2LOG_E(log::catalog, "Failed to check table {} in ns {}, due to {}",
                table_id, physical_collection, table_result.status);
            result.status = std::move(table_result.status);
            result.tableInfo = nullptr;
            return result;
        }
        if (table_result.tableInfo == nullptr) {
            localTxnHandler->CommitTransaction();
            K2LOG_E(log::catalog, "Failed to find table {} in ns {}", table_id, physical_collection);
            result.status = STATUS_FORMAT(NotFound, "Failed to find table {} in ns {}", table_id, physical_collection);
            result.tableInfo = nullptr;
            return result;
        }

        result.tableInfo = table_result.tableInfo;
        localTxnHandler->CommitTransaction();
        result.status = Status(); // OK;

        K2LOG_D(log::catalog, "Returned schema for table name: {}, id: {}",
            result.tableInfo->table_name(), result.tableInfo->table_id());
        return result;
    }
    std::string base_table_id;
    if (index_info == nullptr) {
        // not founnd in cache, try to check the base table id from SKV
        GetBaseTableIdResult table_id_result = GetBaseTableId(localTxnHandler, physical_collection, table_id);
        if (!table_id_result.status.ok()) {
            localTxnHandler->AbortTransaction();
            K2LOG_E(log::catalog, "Failed to check base table id for index {} in {}, due to {}",
                table_id, physical_collection, table_id_result.status.code());
            result.status = std::move(table_id_result.status);
            result.tableInfo = nullptr;
            return result;
        }
        base_table_id = table_id_result.baseTableId;
    } else {
        base_table_id = index_info->base_table_id();
    }

    if (base_table_id.empty()) {
        // cannot find the id as either a table id or an index id
        localTxnHandler->AbortTransaction();
        K2LOG_E(log::catalog, "Failed to find base table id for index {} in {}", table_id, physical_collection);
        result.status = STATUS_FORMAT(NotFound, "Failed to find base table for index {} in ns {}", table_id, physical_collection);
        result.tableInfo = nullptr;
        return result;
    }

    K2LOG_D(log::catalog, "Fetching base table schema {} for index {} in {}", base_table_id, table_id, physical_collection);
    GetTableResult base_table_result = GetTable(localTxnHandler, physical_collection, local_database_info->GetDatabaseName(),
            base_table_id);
    if (!base_table_result.status.ok()) {
        localTxnHandler->AbortTransaction();
        result.status = std::move(base_table_result.status);
        result.tableInfo = nullptr;
        return result;
    }
    localTxnHandler->CommitTransaction();
    result.status = Status(); // OK;
    result.tableInfo = base_table_result.tableInfo;
    // update table cache
    K2LOG_D(log::catalog, "Returned base table schema id: {}, name {}, for index: {}",
        base_table_id, result.tableInfo->table_name(), table_id);
    return result;
}

IndexInfo TableInfoHandler::BuildIndexInfo(std::shared_ptr<TableInfo> base_table_info, std::string index_name, uint32_t table_oid, std::string index_uuid,
                const Schema& index_schema, bool is_unique, bool is_shared, IndexPermissions index_permissions) {
    std::vector<IndexColumn> columns;
    for (ColumnId col_id: index_schema.column_ids()) {
        int col_idx = index_schema.find_column_by_id(col_id);
        if (col_idx == Schema::kColumnNotFound) {
            throw std::runtime_error("Cannot find column with id " + col_id);
        }
        const ColumnSchema& col_schema = index_schema.column(col_idx);
        int32_t base_column_id = -1;
        if (col_schema.name().compare("k2pguniqueidxkeysuffix") != 0 && col_schema.name().compare("k2pgidxbasectid") != 0) {
            // skip checking "k2pguniqueidxkeysuffix" and "k2pgidxbasectid" on base table, which only exist on index table
            std::pair<bool, ColumnId> pair = base_table_info->schema().FindColumnIdByName(col_schema.name());
            if (!pair.first) {
                throw std::runtime_error("Cannot find column id in base table with name " + col_schema.name());
            }
            base_column_id = pair.second;
        }
        K2LOG_D(log::catalog,
            "Index column id: {}, name: {}, type: {}, is_primary: {}, is_hash: {}, order: {}",
            col_id, col_schema.name(), col_schema.type()->id(), col_schema.is_primary(), col_schema.is_hash(), col_schema.order());
        // TODO: change all Table schema and index schema to use is_hash and is_range directly instead of is_primary
        bool is_range = false;
        if (col_schema.is_primary() && !col_schema.is_hash()) {
            is_range = true;
        }
        IndexColumn col(col_id, col_schema.name(), col_schema.type()->id(), col_schema.is_nullable(),
                col_schema.is_hash(), is_range, col_schema.order(), col_schema.sorting_type(), base_column_id);
        columns.push_back(col);
    }
    IndexInfo index_info(index_name, table_oid, index_uuid, base_table_info->table_id(), index_schema.version(),
            is_unique, is_shared, columns, index_permissions);
    return index_info;
}

void TableInfoHandler::AddDefaultPartitionKeys(std::shared_ptr<sh::dto::Schema> schema) {
    // "TableId"
    sh::dto::SchemaField table_id_field;
    table_id_field.type = sh::dto::FieldType::INT64T;
    table_id_field.name = CatalogConsts::TABLE_ID_COLUMN_NAME;
    schema->fields.push_back(table_id_field);
    schema->partitionKeyFields.push_back(0);
    // "IndexId"
    sh::dto::SchemaField index_id_field;
    index_id_field.type = sh::dto::FieldType::INT64T;
    index_id_field.name = CatalogConsts::INDEX_ID_COLUMN_NAME;
    schema->fields.push_back(index_id_field);
    schema->partitionKeyFields.push_back(1);
}

std::shared_ptr<sh::dto::Schema> TableInfoHandler::DeriveSKVSchemaFromTableInfo(std::shared_ptr<TableInfo> table) {
    std::shared_ptr<sh::dto::Schema> schema = std::make_shared<sh::dto::Schema>();
    schema->name = table->table_id();
    schema->version = table->schema().version();
    // add two partitionkey fields
    AddDefaultPartitionKeys(schema);
    uint32_t count = 2;
    for (ColumnSchema col_schema : table->schema().columns()) {
        sh::dto::SchemaField field;
        field.type = ToK2Type(col_schema.type()->id());
        field.name = col_schema.name();
        switch (col_schema.sorting_type()) {
            case ColumnSchema::SortingType::kAscending: {
                field.descending = false;
                field.nullLast = false;
            } break;
            case ColumnSchema::SortingType::kDescending: {
                field.descending = true;
                field.nullLast = false;
            } break;
             case ColumnSchema::SortingType::kAscendingNullsLast: {
                field.descending = false;
                field.nullLast = true;
            } break;
            case ColumnSchema::SortingType::kDescendingNullsLast: {
                field.descending = true;
                field.nullLast = true;
            } break;
            default: break;
       }
       schema->fields.push_back(field);
       if (col_schema.is_primary()) {
           schema->partitionKeyFields.push_back(count);
       }
       count++;
    }

    return schema;
}

std::vector<std::shared_ptr<sh::dto::Schema>> TableInfoHandler::DeriveIndexSchemas(std::shared_ptr<TableInfo> table) {
    std::vector<std::shared_ptr<sh::dto::Schema>> response;
    const IndexMap& index_map = table->secondary_indexes();
    for (const auto& pair : index_map) {
        response.push_back(DeriveIndexSchema(pair.second));
    }
    return response;
}

std::shared_ptr<sh::dto::Schema> TableInfoHandler::DeriveIndexSchema(const IndexInfo& index_info) {
    std::shared_ptr<sh::dto::Schema> schema = std::make_shared<sh::dto::Schema>();
    schema->name = index_info.table_id();
    schema->version = index_info.version();
    // add two partitionkey fields: base table id + index table id
    AddDefaultPartitionKeys(schema);
    uint32_t count = 2;
    for (IndexColumn indexcolumn_schema : index_info.columns()) {
        sh::dto::SchemaField field;
        field.name = indexcolumn_schema.column_name;
        field.type = ToK2Type(indexcolumn_schema.type);
        switch (indexcolumn_schema.sorting_type) {
            case ColumnSchema::SortingType::kAscending: {
                field.descending = false;
                field.nullLast = false;
            } break;
            case ColumnSchema::SortingType::kDescending: {
                K2LOG_W(log::catalog, "Descending secondary indexes are not supported, see issue #268");
                return nullptr;
            } break;
            case ColumnSchema::SortingType::kAscendingNullsLast: {
                field.descending = false;
                field.nullLast = true;
            } break;
            case ColumnSchema::SortingType::kDescendingNullsLast: {
                K2LOG_W(log::catalog, "Descending secondary indexes are not supported, see issue #268");
                return nullptr;
            } break;
            default: break;
        }
        schema->fields.push_back(field);
        // use the keys from PG as the partition keys
        if (indexcolumn_schema.is_hash || indexcolumn_schema.is_range) {
            schema->partitionKeyFields.push_back(count);
        }
        count++;
    }

    return schema;
}

sh::dto::SKVRecord TableInfoHandler::DeriveTableMetaRecord(const std::string& collection_name, std::shared_ptr<TableInfo> table) {
    sh::dto::SKVRecordBuilder builder(collection_name, table_meta_SKVSchema_);

    // SchemaTableId
    builder.serializeNext<int64_t>(CatalogConsts::oid_table_meta);    // 4800
    // SchemaIndexId
    builder.serializeNext<int64_t>(0);
    // TableId
    builder.serializeNext<sh::String>(table->table_id());
    // TableName
    builder.serializeNext<sh::String>(table->table_name());
    // TableOid
    builder.serializeNext<int64_t>(table->table_oid());
    // TableUuid
    builder.serializeNext<sh::String>(table->table_uuid());
    // IsSysTable
    builder.serializeNext<bool>(table->is_sys_table());
    // IsShared
    builder.serializeNext<bool>(table->is_shared());
    // IsIndex
    builder.serializeNext<bool>(false);
    // IsUnique (for index)
    builder.serializeNext<bool>(false);
    // BaseTableId
    builder.serializeNull();
    // IndexPermission
    builder.serializeNull();
    // NextColumnId
    builder.serializeNext<int32_t>(table->next_column_id());
    // SchemaVersion
    builder.serializeNext<int32_t>(table->schema().version());
    
    auto record = builder.build();    
    return record;
}

sh::dto::SKVRecord TableInfoHandler::DeriveTableMetaRecordOfIndex(const std::string& collection_name, const IndexInfo& index, bool is_sys_table, int32_t next_column_id) {
    sh::dto::SKVRecordBuilder builder(collection_name, table_meta_SKVSchema_);
    // SchemaTableId
    builder.serializeNext<int64_t>(CatalogConsts::oid_table_meta);  // 4800
    // SchemaIndexId
    builder.serializeNext<int64_t>(0);
    // TableId -- indexId saved in this field
    builder.serializeNext<sh::String>(index.table_id());
    // TableName -- indexName
    builder.serializeNext<sh::String>(index.table_name());
    // TableOid -- index Oid
    builder.serializeNext<int64_t>(index.table_oid());
    // TableUuid -- index uuid
    builder.serializeNext<sh::String>(index.table_uuid());
    // IsSysTable
    builder.serializeNext<bool>(is_sys_table);
    // IsShared
    builder.serializeNext<bool>(index.is_shared());
    // IsIndex
    builder.serializeNext<bool>(true);
    // IsUnique (for index)
    builder.serializeNext<bool>(index.is_unique());
    // BaseTableId
    builder.serializeNext<sh::String>(index.base_table_id());
    // IndexPermission
    builder.serializeNext<int16_t>(index.index_permissions());
    // NextColumnId
    builder.serializeNext<int32_t>(next_column_id);
    // SchemaVersion
    builder.serializeNext<int32_t>(index.version());

    return builder.build();
}

std::vector<sh::dto::SKVRecord> TableInfoHandler::DeriveTableColumnMetaRecords(const std::string& collection_name, std::shared_ptr<TableInfo> table) {
    std::vector<sh::dto::SKVRecord> response;
    for (std::size_t i = 0; i != table->schema().columns().size(); ++i) {
        ColumnSchema col_schema = table->schema().columns()[i];
        int32_t column_id = table->schema().column_ids()[i];
        sh::dto::SKVRecordBuilder builder(collection_name, tablecolumn_meta_SKVSchema_);
        // SchemaTableId
        builder.serializeNext<int64_t>(CatalogConsts::oid_tablecolumn_meta);   // 4801
        // SchemaIndexId
        builder.serializeNext<int64_t>(0);
        // TableId
        builder.serializeNext<sh::String>(table->table_id());
        // ColumnId
        builder.serializeNext<int32_t>(column_id);
        // ColumnName
        builder.serializeNext<sh::String>(col_schema.name());
        // ColumnType
        builder.serializeNext<int16_t>(col_schema.type()->id());
        // IsNullable
        builder.serializeNext<bool>(col_schema.is_nullable());
        // IsPrimary
        builder.serializeNext<bool>(col_schema.is_primary());
        // IsHash
        builder.serializeNext<bool>(col_schema.is_hash());
        // Order
        builder.serializeNext<int32_t>(col_schema.order());
        // SortingType
        builder.serializeNext<int16_t>(col_schema.sorting_type());

        auto record = builder.build();
        response.push_back(std::move(record));
    }
    return response;
}

std::vector<sh::dto::SKVRecord> TableInfoHandler::DeriveIndexColumnMetaRecords(const std::string& collection_name, const IndexInfo& index, const Schema& base_tablecolumn_schema) {
    std::vector<sh::dto::SKVRecord> response;
    for (IndexColumn index_column : index.columns()) {
        sh::dto::SKVRecordBuilder builder(collection_name, indexcolumn_meta_SKVSchema_);
        // SchemaTableId
        builder.serializeNext<int64_t>(CatalogConsts::oid_indexcolumn_meta);  // 4802
        // SchemaIndexId
        builder.serializeNext<int64_t>(0);
        // TableId
        builder.serializeNext<sh::String>(index.table_id());
        // ColumnId
        builder.serializeNext<int32_t>(index_column.column_id);
        // ColumnName
        builder.serializeNext<sh::String>(index_column.column_name);
        // ColumnType, we persist SQL type directly as an integer
        builder.serializeNext<int16_t>(index_column.type);
        // IsNullable
        builder.serializeNext<bool>(index_column.is_nullable);
        // IsHash
        builder.serializeNext<bool>(index_column.is_hash);
        // IsRange
        builder.serializeNext<bool>(index_column.is_range);
        // Order
        builder.serializeNext<int32_t>(index_column.order);
        // SortingType
        builder.serializeNext<int16_t>(index_column.sorting_type);
        // BaseColumnId
        builder.serializeNext<int32_t>(index_column.base_column_id);
        auto record = builder.build();
        response.push_back(std::move(record));
    }
    return response;
}

sh::dto::FieldType TableInfoHandler::ToK2Type(DataType type) {
    sh::dto::FieldType field_type = sh::dto::FieldType::NOT_KNOWN;
    switch (type) {
        case DataType::K2SQL_DATA_TYPE_UINT8: {
            field_type = sh::dto::FieldType::INT64T;
        } break;
        case DataType::K2SQL_DATA_TYPE_INT8: {
            field_type = sh::dto::FieldType::INT64T;
        } break;
        case DataType::K2SQL_DATA_TYPE_UINT16: {
            field_type = sh::dto::FieldType::INT64T;
        } break;
        case DataType::K2SQL_DATA_TYPE_INT16: {
            field_type = sh::dto::FieldType::INT64T;
        } break;
        case DataType::K2SQL_DATA_TYPE_UINT32: {
            field_type = sh::dto::FieldType::INT64T;
        } break;
        case DataType::K2SQL_DATA_TYPE_INT32: {
            field_type = sh::dto::FieldType::INT64T;
        } break;
        case DataType::K2SQL_DATA_TYPE_UINT64: {
            field_type = sh::dto::FieldType::INT64T;
        } break;
        case DataType::K2SQL_DATA_TYPE_INT64: {
            field_type = sh::dto::FieldType::INT64T;
        } break;
        case DataType::K2SQL_DATA_TYPE_STRING: {
            field_type = sh::dto::FieldType::STRING;
        } break;
        case DataType::K2SQL_DATA_TYPE_BOOL: {
            field_type = sh::dto::FieldType::BOOL;
        } break;
        case DataType::K2SQL_DATA_TYPE_FLOAT: {
            field_type = sh::dto::FieldType::FLOAT;
        } break;
        case DataType::K2SQL_DATA_TYPE_DOUBLE: {
            field_type = sh::dto::FieldType::DOUBLE;
        } break;
        case DataType::K2SQL_DATA_TYPE_BINARY: {
            field_type = sh::dto::FieldType::STRING;
        } break;
        case DataType::K2SQL_DATA_TYPE_TIMESTAMP: {
            field_type = sh::dto::FieldType::INT64T;
        } break;
        case DataType::K2SQL_DATA_TYPE_DECIMAL: {
            field_type = sh::dto::FieldType::STRING;
        } break;
        default:
            throw std::invalid_argument("Unsupported type " + type);
    }
    return field_type;
}

Status TableInfoHandler::FetchTableMetaSKVRecord(std::shared_ptr<PgTxnHandler> txnHandler, const std::string& collection_name, const std::string& table_id, sh::dto::SKVRecord& resultSKVRecord) {
    sh::dto::SKVRecordBuilder builder(collection_name, table_meta_SKVSchema_);
    // SchemaTableId
    builder.serializeNext<int64_t>(CatalogConsts::oid_table_meta);    // 4800
    // SchemaIndexId
    builder.serializeNext<int64_t>(0);
    // table_id
    builder.serializeNext<sh::String>(table_id);

    auto recordKey = builder.build();
    K2LOG_D(log::catalog, "Fetching Table meta SKV record for table {}", table_id);
    auto&& [status, value] = k2_adapter_->ReadRecord(txnHandler->GetTxn(), recordKey);
    // TODO: add error handling and retry logic in catalog manager
    if (!status.is2xxOK()) {
        K2LOG_E(log::catalog, "Error fetching entry {} in {} due to {}", table_id, collection_name, status);
        return K2Adapter::K2StatusToK2PgStatus(status);
    }

    resultSKVRecord = std::move(value);
    return Status(); // OK
}

std::vector<sh::dto::SKVRecord> TableInfoHandler::FetchIndexMetaSKVRecords(std::shared_ptr<PgTxnHandler> txnHandler, const std::string& collection_name, const std::string& base_table_id) {
    std::vector<sh::dto::SKVRecord> records;
    std::vector<sh::dto::expression::Value> values;
    std::vector<sh::dto::expression::Expression> exps;
    // find all the indexes for the base table, i.e., by BaseTableId
    values.emplace_back(sh::dto::expression::makeValueReference(CatalogConsts::BASE_TABLE_ID_COLUMN_NAME));
    values.emplace_back(sh::dto::expression::makeValueLiteral<sh::String>(sh::String(base_table_id)));
    sh::dto::expression::Expression filterExpr = sh::dto::expression::makeExpression(sh::dto::expression::Operation::EQ, std::move(values), std::move(exps));
    auto startScanRecord = buildRangeRecord(collection_name, table_meta_SKVSchema_, CatalogConsts::oid_table_meta, 0/*index_oid*/, std::nullopt);
    auto endScanRecord = buildRangeRecord(collection_name, table_meta_SKVSchema_, CatalogConsts::oid_table_meta, 0/*index_oid*/, std::nullopt);

    auto txn = txnHandler->GetTxn();
    auto [status, query] = k2_adapter_->CreateScanRead(txn, collection_name, table_meta_SKVSchema_->name, startScanRecord, endScanRecord, std::move(filterExpr));
    if (!status.is2xxOK()) {
        auto msg = fmt::format("Failed to create scan read for {} in {} due to {}",
        base_table_id, collection_name, status);
        K2LOG_E(log::catalog, "{}", msg);
        throw std::runtime_error(msg);
    }

    bool done = false;
    do {
        K2LOG_D(log::catalog, "Fetching Table meta SKV records for indexes on base table {}", base_table_id);
        auto [status, query_result] = k2_adapter_->ScanRead(txn, query);
        if (!status.is2xxOK()) {
            auto msg = fmt::format("Failed to run scan read for indexes in base table {} in {} due to {}", base_table_id, collection_name, status);
            K2LOG_E(log::catalog, "{}", msg);
            throw std::runtime_error(msg);
        }

        for (sh::dto::SKVRecord::Storage& storage : query_result.records) {
            sh::dto::SKVRecord record(collection_name,  table_meta_SKVSchema_, std::move(storage), true);
            records.push_back(std::move(record));
        }
        // if the query is not done, the query itself is updated with the pagination token for the next call
        done = query_result.done;
    } while (!done);

    return records;
}

std::vector<sh::dto::SKVRecord> TableInfoHandler::FetchTableColumnMetaSKVRecords(std::shared_ptr<PgTxnHandler> txnHandler, const std::string& collection_name, const std::string& table_id) {
    std::vector<sh::dto::expression::Value> values;
    std::vector<sh::dto::expression::Expression> exps;
    // find all the columns for a table by TableId
    values.emplace_back(sh::dto::expression::makeValueReference(sh::String(CatalogConsts::TABLE_ID_COLUMN_NAME)));
    values.emplace_back(sh::dto::expression::makeValueLiteral<sh::String>(sh::String(table_id)));
    sh::dto::expression::Expression filterExpr = sh::dto::expression::makeExpression(sh::dto::expression::Operation::EQ, std::move(values), std::move(exps));
    auto startScanRecord = buildRangeRecord(collection_name, tablecolumn_meta_SKVSchema_, CatalogConsts::oid_tablecolumn_meta, 0/*index_oid*/, std::make_optional(table_id));
    auto endScanRecord = buildRangeRecord(collection_name, tablecolumn_meta_SKVSchema_, CatalogConsts::oid_tablecolumn_meta, 0/*index_oid*/, std::make_optional(table_id));
    auto txn = txnHandler->GetTxn();
    auto [status, query] = k2_adapter_->CreateScanRead(txn, collection_name, tablecolumn_meta_SKVSchema_->name, startScanRecord, endScanRecord, std::move(filterExpr));
    if (!status.is2xxOK()) {
        auto msg = fmt::format("Failed to create scan read for {} in {} due to {}",
        table_id, collection_name, status);
        K2LOG_E(log::catalog, "{}", msg);
        throw std::runtime_error(msg);
    }
    std::vector<sh::dto::SKVRecord> records;
    bool done = false;
    do {
        auto [status, query_result] = k2_adapter_->ScanRead(txn, query);
        if (!status.is2xxOK()) {
            auto msg = fmt::format("Failed to run scan read for {} in {} due to {}",
                table_id, collection_name,status);
            K2LOG_E(log::catalog, "{}", msg);
            throw std::runtime_error(msg);
        }

        for (sh::dto::SKVRecord::Storage& storage : query_result.records) {
            sh::dto::SKVRecord record(collection_name, table_meta_SKVSchema_, std::move(storage), true);
            records.push_back(std::move(record));
        }
        // if the query is not done, the query itself is updated with the pagination token for the next call
        done = query_result.done;
    } while (!done);

    return records;
}

std::vector<sh::dto::SKVRecord> TableInfoHandler::FetchIndexColumnMetaSKVRecords(std::shared_ptr<PgTxnHandler> txnHandler, const std::string& collection_name, const std::string& table_id) {
    std::vector<sh::dto::SKVRecord> records;
    std::vector<sh::dto::expression::Value> values;
    std::vector<sh::dto::expression::Expression> exps;
    // find all the columns for an index table by TableId
    values.emplace_back(sh::dto::expression::makeValueReference(sh::String(CatalogConsts::TABLE_ID_COLUMN_NAME)));
    values.emplace_back(sh::dto::expression::makeValueLiteral<sh::String>(sh::String(table_id)));
    sh::dto::expression::Expression filterExpr = sh::dto::expression::makeExpression(sh::dto::expression::Operation::EQ, std::move(values), std::move(exps));
    auto startScanRecord = buildRangeRecord(collection_name, indexcolumn_meta_SKVSchema_, CatalogConsts::oid_indexcolumn_meta, 0/*index_oid*/, std::make_optional(table_id));
    auto endScanRecord = buildRangeRecord(collection_name, indexcolumn_meta_SKVSchema_, CatalogConsts::oid_indexcolumn_meta, 0/*index_oid*/, std::make_optional(table_id));
    auto txn = txnHandler->GetTxn();
    auto [status, query] = k2_adapter_->CreateScanRead(txn, collection_name, indexcolumn_meta_SKVSchema_->name, startScanRecord, endScanRecord, std::move(filterExpr));
    if (!status.is2xxOK()) {
        auto msg = fmt::format("Failed to create scan read for {} in {} due to {}",
        table_id, collection_name, status);
        K2LOG_E(log::catalog, "{}", msg);
        throw std::runtime_error(msg);
    }

    bool done = false;
    do {
        auto [status, query_result] = k2_adapter_->ScanRead(txn, query);
        if (!status.is2xxOK()) {
            auto msg = fmt::format("Failed to run scan read for {} in {} due to {}",
                table_id, collection_name, status);
            K2LOG_E(log::catalog, "{}", msg);
            throw std::runtime_error(msg);
        }

        for (sh::dto::SKVRecord::Storage& storage : query_result.records) {
            sh::dto::SKVRecord record(collection_name,  table_meta_SKVSchema_, std::move(storage), true);            
            records.push_back(std::move(record));
        }
        done = query_result.done;

    } while (!done);

    return records;
}

std::shared_ptr<TableInfo> TableInfoHandler::BuildTableInfo(const std::string& database_id, const std::string& database_name,
        sh::dto::SKVRecord& table_meta, std::vector<sh::dto::SKVRecord>& table_columns) {
    // deserialize table meta
    // SchemaTableId
    table_meta.deserializeNext<int64_t>();
    // SchemaIndexId
    table_meta.deserializeNext<int64_t>();
    // TableId
    std::string table_id = table_meta.deserializeNext<sh::String>().value();
    // TableName
    std::string table_name = table_meta.deserializeNext<sh::String>().value();
    // TableOid
    uint32_t table_oid = table_meta.deserializeNext<int64_t>().value();
    // TableUuid
    std::string table_uuid = table_meta.deserializeNext<sh::String>().value();
    // IsSysTable
    bool is_sys_table = table_meta.deserializeNext<bool>().value();
    // IsShared
    bool is_shared = table_meta.deserializeNext<bool>().value();
    // IsIndex
    bool is_index = table_meta.deserializeNext<bool>().value();
    if (is_index) {
        throw std::runtime_error("Table " + table_id + " should not be an index");
    }
    // IsUnique
    table_meta.deserializeNext<bool>();
    // BaseTableId
    table_meta.deserializeNext<sh::String>();
    // IndexPermission
    table_meta.deserializeNext<int16_t>();
    // NextColumnId
    int32_t next_column_id = table_meta.deserializeNext<int32_t>().value();
     // SchemaVersion
    uint32_t version = table_meta.deserializeNext<int32_t>().value();

    std::vector<ColumnSchema> cols;
    int key_columns = 0;
    std::vector<ColumnId> ids;
    // deserialize table columns
    for (auto& column : table_columns) {
        // SchemaTableId
        column.deserializeNext<int64_t>();
        // SchemaIndexId
        column.deserializeNext<int64_t>();
        // TableId
        std::string tb_id = column.deserializeNext<sh::String>().value();
        // ColumnId
        int32_t col_id = column.deserializeNext<int32_t>().value();
        // ColumnName
        std::string col_name = column.deserializeNext<sh::String>().value();
        // ColumnType, we persist SQL type directly as an integer
        int16_t col_type = column.deserializeNext<int16_t>().value();
        // IsNullable
        bool is_nullable = column.deserializeNext<bool>().value();
        // IsPrimary
        bool is_primary = column.deserializeNext<bool>().value();
        // IsHash
        bool is_hash = column.deserializeNext<bool>().value();
        // Order
        int32 col_order = column.deserializeNext<int32_t>().value();
        // SortingType
        int16_t sorting_type = column.deserializeNext<int16_t>().value();
        ColumnSchema col_schema(col_name, static_cast<DataType>(col_type), is_nullable, is_primary, is_hash,
                col_order, static_cast<ColumnSchema::SortingType>(sorting_type));
        cols.push_back(std::move(col_schema));
        ids.push_back(col_id);
        if (is_primary) {
            key_columns++;
        }
    }
    Schema table_schema(cols, ids, key_columns);
    table_schema.set_version(version);
    std::shared_ptr<TableInfo> table_info = std::make_shared<TableInfo>(database_id, database_name, table_oid, table_name, table_uuid, table_schema);
    table_info->set_next_column_id(next_column_id);
    table_info->set_is_sys_table(is_sys_table);
    table_info->set_is_shared_table(is_shared);
    return table_info;
}

IndexInfo TableInfoHandler::BuildIndexInfo(std::shared_ptr<PgTxnHandler> txnHandler, const std::string& collection_name, sh::dto::SKVRecord& index_table_meta) {
    // deserialize index's table meta
    // SchemaTableId
    index_table_meta.deserializeNext<int64_t>();
    // SchemaIndexId
    index_table_meta.deserializeNext<int64_t>();
    // TableId
    std::string table_id = index_table_meta.deserializeNext<sh::String>().value();
    // TableName
    std::string table_name = index_table_meta.deserializeNext<sh::String>().value();
    // TableOid
    uint32_t table_oid = index_table_meta.deserializeNext<int64_t>().value();
    // TableUuid
    std::string table_uuid = index_table_meta.deserializeNext<sh::String>().value();
    // IsSysTable
    index_table_meta.deserializeNext<bool>();
    // IsShared
    bool is_shared = index_table_meta.deserializeNext<bool>().value();
    // IsIndex
    bool is_index = index_table_meta.deserializeNext<bool>().value();
    if (!is_index) {
        throw std::runtime_error("Table " + table_id + " should be an index");
    }
    // IsUnique
    bool is_unique = index_table_meta.deserializeNext<bool>().value();
    // BaseTableId
    std::string base_table_id = index_table_meta.deserializeNext<sh::String>().value();
    // IndexPermission
    IndexPermissions index_perm = static_cast<IndexPermissions>(index_table_meta.deserializeNext<int16_t>().value());
    // NextColumnId
    index_table_meta.deserializeNext<int32_t>();
    // SchemaVersion
    uint32_t version = index_table_meta.deserializeNext<int32_t>().value();

    // Fetch index columns
    std::vector<sh::dto::SKVRecord> index_columns = FetchIndexColumnMetaSKVRecords(txnHandler, collection_name, table_id);

    // deserialize index columns
    std::vector<IndexColumn> columns;
    for (auto& column : index_columns) {
        // SchemaTableId
        column.deserializeNext<int64_t>();
        // SchemaIndexId
        column.deserializeNext<int64_t>();
        // TableId
        std::string tb_id = column.deserializeNext<sh::String>().value();
        // ColumnId
        int32_t col_id = column.deserializeNext<int32_t>().value();
        // ColumnName
        std::string col_name = column.deserializeNext<sh::String>().value();
        // ColumnType
        int16_t col_type = column.deserializeNext<int16_t>().value();
        // IsNullable
        bool is_nullable = column.deserializeNext<bool>().value();
        // IsHash
        bool is_hash = column.deserializeNext<bool>().value();
        // IsRange
        bool is_range = column.deserializeNext<bool>().value();
        // Order
        int32_t col_order = column.deserializeNext<int32_t>().value();
        // SortingType
        int16_t sorting_type = column.deserializeNext<int16_t>().value();
        // BaseColumnId
        int32_t base_col_id = column.deserializeNext<int32_t>().value();
        // TODO: add support for expression in index
        IndexColumn index_column(col_id, col_name, static_cast<DataType>(col_type), is_nullable, is_hash, is_range,
                col_order, static_cast<ColumnSchema::SortingType>(sorting_type), base_col_id);
        columns.push_back(std::move(index_column));
    }

    IndexInfo index_info(table_name, table_oid, table_uuid, base_table_id, version, is_unique, is_shared, columns, index_perm);
    return index_info;
}

sh::dto::SKVRecord TableInfoHandler::buildRangeRecord(const std::string& collection_name, std::shared_ptr<sh::dto::Schema> schema, PgOid table_oid, PgOid index_oid, std::optional<std::string> table_id) {
    sh::dto::SKVRecordBuilder builder(collection_name, schema);
    // SchemaTableId
    builder.serializeNext<int64_t>(table_oid);
    // SchemaIndexId
    builder.serializeNext<int64_t>(index_oid);
    if (table_id != std::nullopt) {
        builder.serializeNext<sh::String>(table_id.value());
    }
    return builder.build();
}

} // namespace catalog
} // namespace sql
} // namespace k2pg
