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

#include <string>
#include <vector>

#include "sql_catalog_entity.h"
#include "access/k2/pg_schema.h"

namespace k2pg {
namespace catalog {

namespace sh=skv::http;

// using k2pg::gate::CreateScanReadResult;
using k2pg::PgObjectId;
using k2pg::Status;


struct CopyTableResult {
    std::shared_ptr<TableInfo> tableInfo;
    int num_index = 0;
};


struct CreateIndexTableParams {
    std::string index_name;
    uint32_t table_oid;
    Schema index_schema;
    bool is_unique;
    bool is_shared;
    bool is_not_exist;
    bool skip_index_backfill;
    IndexPermissions index_permissions;
};

struct GetTableTypeInfoResult {
    bool isShared;
    bool isIndex;
};

const std::string skv_schema_name_table_meta =           "K2RESVD_SCHEMA_SQL_TABLE_META";
const std::string skv_schema_name_tablecolumn_meta =     "K2RESVD_SCHEMA_SQL_TABLECOLUMN_META";
const std::string skv_schema_name_indexcolumn_meta =     "K2RESVD_SCHEMA_SQL_INDEXCOLUMN_META";
// hard coded PgOid for these above three meta tables (used for SKV )
// NOTE: these system unused oids are got from script src/includ/catlog/unused_oids. Try staying in 48XX range for future such need in Chogori-SQL.
const PgOid oid_table_meta = 4800;
const PgOid oid_tablecolumn_meta = 4801;
const PgOid oid_indexcolumn_meta = 4802;

class TableInfoHandler {
    public:
    typedef std::shared_ptr<TableInfoHandler> SharedPtr;

    TableInfoHandler();
    ~TableInfoHandler();

    // Design Note: (tables mapping to SKV schema)
    // 1. Any table primary index(must have) is mapped to a SKV schema, and if it have secondary index(s), each one is mapped to its own SKV schema.
    // 2. Following three SKV schemas are for three system meta tables holding all table/index definition(aka. meta), i.e. tablemeta(table identities and basic info), tablecolumnmeta(column def), indexcolumnmeta(index column def)
    // 3. The schema name for meta tables are hardcoded constant, e.g. tablemeta's is CatalogConsts::skv_schema_name_table_meta
    // 4. The schema name for user table/secondary index are the table's TableId(), which is a string presentation of UUID containing tables's pguid (for details, see std::string PgObjectId::GetTableId(const PgOid& table_oid))
    // 5. As of now, before embedded table(s) are supported, all tables are flat in relationship with each other. Thus, all tables(meta or user) have two prefix fields "TableId" and "IndexId" in their SKV schema,
    //    so that all rows in a table and index are clustered together in K2.
    //      For a primary index, the TableId is the PgOid(uint32 but saved as int64_t in K2) of this table, and IndexId is 0
    //      For a secondary index, The TableId is the PgOid of base table(primary index), and IndexId is its own PgOid.
    //      For three system tables which is not defined in PostgreSQL originally, the PgOid of them are taken from unused system Pgoid range 4800-4803 (for detail, see CatalogConsts::oid_table_meta)

    // schema of table information
    sh::dto::Schema skv_schema_table_meta {
        .name = skv_schema_name_table_meta,
        .version = 1,
        .fields = std::vector<sh::dto::SchemaField> {
                {sh::dto::FieldType::INT64T, "SchemaTableId", false, false},        // const PgOid CatalogConsts::oid_table_meta = 4800;
                {sh::dto::FieldType::INT64T, "SchemaIndexId", false, false},        // 0
                {sh::dto::FieldType::STRING, "TableId", false, false},
                {sh::dto::FieldType::STRING, "TableName", false, false},
                {sh::dto::FieldType::INT64T, "TableOid", false, false},
                {sh::dto::FieldType::STRING, "TableUuid", false, false},
                {sh::dto::FieldType::BOOL, "IsSysTable", false, false},
                {sh::dto::FieldType::BOOL, "IsShared", false, false},
                {sh::dto::FieldType::BOOL, "IsIndex", false, false},
                {sh::dto::FieldType::BOOL, "IsUnique", false, false},
                {sh::dto::FieldType::STRING, "BaseTableId", false, false},
                {sh::dto::FieldType::INT16T, "IndexPermission", false, false},
                {sh::dto::FieldType::INT32T, "NextColumnId", false, false},
                {sh::dto::FieldType::INT32T, "SchemaVersion", false, false}},
        .partitionKeyFields = std::vector<uint32_t> { 0, 1, 2},
        .rangeKeyFields = std::vector<uint32_t> {}
    };

    // schema to store table column schema information
    sh::dto::Schema skv_schema_tablecolumn_meta {
        .name = skv_schema_name_tablecolumn_meta,
        .version = 1,
        .fields = std::vector<sh::dto::SchemaField> {
                {sh::dto::FieldType::INT64T, "SchemaTableId", false, false},    // const PgOid CatalogConsts::oid_tablecolumn_meta = 4801;
                {sh::dto::FieldType::INT64T, "SchemaIndexId", false, false},    // 0
                {sh::dto::FieldType::STRING, "TableId", false, false},
                {sh::dto::FieldType::INT32T, "ColumnId", false, false},
                {sh::dto::FieldType::STRING, "ColumnName", false, false},
                {sh::dto::FieldType::INT16T, "ColumnType", false, false},
                {sh::dto::FieldType::BOOL, "IsNullable", false, false},
                {sh::dto::FieldType::BOOL, "IsPrimary", false, false},
                {sh::dto::FieldType::BOOL, "IsHash", false, false},
                {sh::dto::FieldType::INT32T, "Order", false, false},
                {sh::dto::FieldType::INT16T, "SortingType", false, false}},
        .partitionKeyFields = std::vector<uint32_t> { 0 , 1, 2},
        .rangeKeyFields = std::vector<uint32_t> {3}
    };

    // schema to store index column schema information
    sh::dto::Schema skv_schema_indexcolumn_meta {
        .name = skv_schema_name_indexcolumn_meta,
        .version = 1,
        .fields = std::vector<sh::dto::SchemaField> {
                {sh::dto::FieldType::INT64T, "SchemaTableId", false, false},    // const PgOid CatalogConsts::oid_indexcolumn_meta = 4802;
                {sh::dto::FieldType::INT64T, "SchemaIndexId", false, false},    // 0
                {sh::dto::FieldType::STRING, "TableId", false, false},
                {sh::dto::FieldType::INT32T, "ColumnId", false, false},
                {sh::dto::FieldType::STRING, "ColumnName", false, false},
                {sh::dto::FieldType::INT16T, "ColumnType", false, false},
                {sh::dto::FieldType::BOOL, "IsNullable", false, false},
                {sh::dto::FieldType::BOOL, "IsHash", false, false},
                {sh::dto::FieldType::BOOL, "IsRange", false, false},
                {sh::dto::FieldType::INT32T, "Order", false, false},
                {sh::dto::FieldType::INT16T, "SortingType", false, false},
                {sh::dto::FieldType::INT32T, "BaseColumnId", false, false}},
        .partitionKeyFields = std::vector<uint32_t> { 0, 1, 2},
        .rangeKeyFields = std::vector<uint32_t> {3}
    };

    // create above three meta tables for a DB
    sh::Status CreateMetaTables(std::shared_ptr<sh::TxnHandle> txnHandler, const std::string& collection_name);

    // Create or update a user defined table fully, including all its secondary indexes if any.
    sh::Status CreateOrUpdateTable(std::shared_ptr<sh::TxnHandle> txnHandler, const std::string& collection_name, std::shared_ptr<TableInfo> table);

    sh::Response<std::shared_ptr<TableInfo>> GetTable(std::shared_ptr<sh::TxnHandle> txnHandler, const std::string& collection_name, const std::string& database_name, const std::string& table_id);
    sh::Response<std::shared_ptr<TableInfo>> GetTableSchema(std::shared_ptr<sh::TxnHandle> txnHandler, std::shared_ptr<DatabaseInfo> database_info, const std::string& table_id,
        std::shared_ptr<IndexInfo> index_info,
        std::function<std::shared_ptr<DatabaseInfo>(const std::string&)> fnc_db,
        std::function<std::shared_ptr<sh::TxnHandle>()> fnc_tx);

    sh::Response<std::vector<std::shared_ptr<TableInfo>>> ListTables(std::shared_ptr<sh::TxnHandle> txnHandler, const std::string& collection_name, const std::string& database_name, bool isSysTableIncluded);

    sh::Response<std::vector<std::string>> ListTableIds(std::shared_ptr<sh::TxnHandle> txnHandler, const std::string& collection_name, bool isSysTableIncluded);

    // CopyTable (meta and data) fully including secondary indexes, currently only support cross different database.
    sh::Response<CopyTableResult> CopyTable(std::shared_ptr<sh::TxnHandle> target_txnHandler,
            const std::string& target_coll_name,
            const std::string& target_database_name,
            uint32_t target_database_oid,
            std::shared_ptr<sh::TxnHandle> source_txnHandler,
            const std::string& source_coll_name,
            const std::string& source_database_name,
            const std::string& source_table_id);

    sh::Status CreateIndexSKVSchema(std::shared_ptr<sh::TxnHandle> txnHandler, const std::string& collection_name,
        std::shared_ptr<TableInfo> table, const IndexInfo& index_info);

    sh::Status PersistIndexMeta(std::shared_ptr<sh::TxnHandle> txnHandler, const std::string& collection_name, std::shared_ptr<TableInfo> table, const IndexInfo& index_info);

    sh::Status DeleteTableMetadata(std::shared_ptr<sh::TxnHandle> txnHandler, const std::string& collection_name, std::shared_ptr<TableInfo> table);

    sh::Status DeleteTableData(std::shared_ptr<sh::TxnHandle> txnHandler, const std::string& collection_name, std::shared_ptr<TableInfo> table);

    sh::Status DeleteIndexMetadata(std::shared_ptr<sh::TxnHandle> txnHandler, const std::string& collection_name,  const std::string& index_id);

    sh::Status DeleteIndexData(std::shared_ptr<sh::TxnHandle> txnHandler, const std::string& collection_name,  const std::string& index_id);

    sh::Response<std::string> GetBaseTableId(std::shared_ptr<sh::TxnHandle> txnHandler, const std::string& collection_name, const std::string& index_id);

    // check if passed id is that for a table or index, and if it is a shared table/index(just one instance shared by all databases and resides in primary cluster)
    sh::Response<GetTableTypeInfoResult> GetTableTypeInfo(std::shared_ptr<sh::TxnHandle> txnHandler, const std::string& collection_name, const std::string& table_id);

    // create index table (handle create if exists flag)
    sh::Response<std::shared_ptr<IndexInfo>> CreateIndexTable(std::shared_ptr<sh::TxnHandle> txnHandler, std::shared_ptr<DatabaseInfo> database_info, std::shared_ptr<TableInfo> base_table_info, CreateIndexTableParams &index_params);

    private:
    sh::Status CopySKVTable(std::shared_ptr<sh::TxnHandle> target_txnHandler,
            const std::string& target_coll_name,
            const std::string& target_schema_name,
            uint32_t target_schema_version,
            std::shared_ptr<sh::TxnHandle> source_txnHandler,
            const std::string& source_coll_name,
            const std::string& source_schema_name,
            uint32_t source_schema_version,
            PgOid source_table_oid,
            PgOid source_index_oid);

    // A SKV Schema of perticular version is not mutable, thus, we only create a new specified version if that version doesn't exists yet
    sh::Status CreateTableSKVSchema(std::shared_ptr<sh::TxnHandle> txnHandler, const std::string& collection_name, std::shared_ptr<TableInfo> table);

    // Persist (user) table's definition/meta into three sytem meta tables.
    sh::Status PersistTableMeta(std::shared_ptr<sh::TxnHandle> txnHandler, const std::string& collection_name, std::shared_ptr<TableInfo> table);

    std::shared_ptr<sh::dto::Schema> DeriveSKVSchemaFromTableInfo(std::shared_ptr<TableInfo> table);

    std::vector<std::shared_ptr<sh::dto::Schema>> DeriveIndexSchemas(std::shared_ptr<TableInfo> table);

    std::shared_ptr<sh::dto::Schema> DeriveIndexSchema(const IndexInfo& index_info);

    sh::dto::SKVRecord DeriveTableMetaRecord(const std::string& collection_name, std::shared_ptr<TableInfo> table);

    sh::dto::SKVRecord DeriveTableMetaRecordOfIndex(const std::string& collection_name, const IndexInfo& index, bool is_sys_table, int32_t next_column_id);

    std::vector<sh::dto::SKVRecord> DeriveTableColumnMetaRecords(const std::string& collection_name, std::shared_ptr<TableInfo> table);

    std::vector<sh::dto::SKVRecord> DeriveIndexColumnMetaRecords(const std::string& collection_name, const IndexInfo& index, const Schema& base_tablecolumn_schema);

    sh::dto::FieldType ToK2Type(DataType type);

    DataType ToSqlType(sh::dto::FieldType type);

    sh::Status FetchTableMetaSKVRecord(std::shared_ptr<sh::TxnHandle> txnHandler, const std::string& collection_name, const std::string& table_id, sh::dto::SKVRecord& resultSKVRecord);

    // TODO: change following API return Status instead of throw exception

    std::vector<sh::dto::SKVRecord> FetchIndexMetaSKVRecords(std::shared_ptr<sh::TxnHandle> txnHandler, const std::string& collection_name, const std::string& base_table_id);

    std::vector<sh::dto::SKVRecord> FetchTableColumnMetaSKVRecords(std::shared_ptr<sh::TxnHandle> txnHandler, const std::string& collection_name, const std::string& table_id);

    std::vector<sh::dto::SKVRecord> FetchIndexColumnMetaSKVRecords(std::shared_ptr<sh::TxnHandle> txnHandler, const std::string& collection_name, const std::string& table_id);

    std::shared_ptr<TableInfo> BuildTableInfo(const std::string& database_id, const std::string& database_name, sh::dto::SKVRecord& table_meta, std::vector<sh::dto::SKVRecord>& table_columns);

    IndexInfo BuildIndexInfo(std::shared_ptr<sh::TxnHandle> txnHandler, const std::string& collection_name, sh::dto::SKVRecord& index_table_meta);

    IndexInfo BuildIndexInfo(std::shared_ptr<TableInfo> base_table_info, std::string index_name, uint32_t table_oid, std::string index_uuid,
                const Schema& index_schema, bool is_unique, bool is_shared, IndexPermissions index_permissions);

    void AddDefaultPartitionKeys(std::shared_ptr<sh::dto::Schema> schema);

    // Build a range record for a scan, optionally using third param table_id when applicable(e.g. in sys table).
    sh::dto::SKVRecord buildRangeRecord(const std::string& collection_name, std::shared_ptr<sh::dto::Schema> schema, PgOid table_oid, PgOid index_oid, std::optional<std::string> table_id);

    std::shared_ptr<sh::dto::Schema> table_meta_SKVSchema_;
    std::shared_ptr<sh::dto::Schema> tablecolumn_meta_SKVSchema_;
    std::shared_ptr<sh::dto::Schema> indexcolumn_meta_SKVSchema_;
};

} // namespace catalog
} // namespace k2pg
