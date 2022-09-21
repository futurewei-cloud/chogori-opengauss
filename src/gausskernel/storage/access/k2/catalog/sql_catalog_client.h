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
#include <unordered_map>
#include <optional>

#include "sql_catalog_manager.h"

namespace k2pg {
namespace catalog {

typedef class k2pg::Schema PgSchema;
using k2pg::Status;

// TODO: This catalog client layer so far doesn't provide value, consider eliminate it later
class SqlCatalogClient {
public:
    SqlCatalogClient(std::shared_ptr<SqlCatalogManager> catalog_manager) : catalog_manager_(catalog_manager) {
    };

    ~SqlCatalogClient() {};

    Status IsInitDbDone(bool* isDone);

    Status InitPrimaryCluster();

    Status FinishInitDB();

    // Create a new database with the given name.
    Status CreateDatabase(const std::string& database_name,
                                const std::string& database_id,
                                uint32_t database_oid,
                                const std::string& source_database_id,
                                const std::string& creator_role_name,
                                const std::optional<uint32_t>& next_pg_oid = std::nullopt);

    // Delete database with the given name.
    Status DeleteDatabase(const std::string& database_name,
                                const std::string& database_id);

    Status UseDatabase(const std::string& database_name);


    Status CreateTable(const std::string& database_name,
                            const std::string& table_name,
                            const PgObjectId& table_object_id,
                            PgSchema& schema,
                            bool is_pg_catalog_table,
                            bool is_shared_table,
                            bool if_not_exist);

    Status CreateIndexTable(const std::string& database_name,
                            const std::string& table_name,
                            const PgObjectId& table_object_id,
                            const PgObjectId& base_table_object_id,
                            PgSchema& schema,
                            bool is_unique_index,
                            bool skip_index_backfill,
                            bool is_pg_catalog_table,
                            bool is_shared_table,
                            bool if_not_exist);

    // Delete the specified table.
    // Set 'wait' to true if the call must wait for the table to be fully deleted before returning.
    Status DeleteTable(const PgOid database_oid, const PgOid table_oid, bool wait = true);

    Status DeleteIndexTable(const PgOid database_oid, const PgOid table_oid, PgOid *base_table_oid, bool wait = true);

    Status OpenTable(const PgOid database_oid, const PgOid table_oid, std::shared_ptr<TableInfo>* table);

    std::shared_ptr<TableInfo> OpenTable(const PgOid database_oid, const PgOid table_oid);

    // For Postgres: reserve oids for a Postgres database.
    Status ReservePgOids(const PgOid database_oid,
                                uint32_t next_oid, uint32_t count,
                                uint32_t* begin_oid, uint32_t* end_oid);


    Status GetCatalogVersion(uint64_t *pg_catalog_version);

    Status IncrementCatalogVersion();

    // If relation is a base table, then base_table_oid is set to relation_oid
    Status GetBaseTableOID(uint32_t database_oid, uint32_t relation_oid, uint32_t& base_table_oid);

private:
    std::shared_ptr<SqlCatalogManager> catalog_manager_;
};

} // namespace catalog
}  // namespace k2pg
