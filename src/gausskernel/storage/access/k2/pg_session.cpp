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
#include "libintl.h"
#include "postgres.h"

#include "access/k2/pg_session.h"
#include "catalog/sql_catalog_client.h"

#include "utils/errcodes.h"
#include "utils/elog.h"
namespace k2pg {

PgSession::PgSession(
    std::shared_ptr<k2pg::catalog::SqlCatalogClient> catalog_client,
    const std::string& database_name)
    : catalog_client_(catalog_client),
      connected_database_(database_name),
      client_id_("K2PG"),
      stmt_id_(1) {
}

PgSession::~PgSession() {
}

Status PgSession::ConnectDatabase(const std::string& database_name) {
    connected_database_ = database_name;
    return catalog_client_->UseDatabase(database_name);
}

void PgSession::InvalidateTableCache(const PgObjectId& table_obj_id) {
    std::string pg_table_uuid = table_obj_id.GetTableUuid();
    table_cache_.erase(pg_table_uuid);
}

std::shared_ptr<PgTableDesc> PgSession::LoadTable(const PgOid database_oid, const PgOid object_oid) {
    PgObjectId pg_object(database_oid, object_oid);
    return LoadTable(pg_object);
}

std::shared_ptr<PgTableDesc> PgSession::LoadTable(const PgObjectId& table_object_id) {
    std::string t_table_uuid = table_object_id.GetTableUuid();
    std::shared_ptr<TableInfo> table;

    auto cached_table = table_cache_.find(t_table_uuid);
    if (cached_table == table_cache_.end()) {
        Status status = catalog_client_->OpenTable(table_object_id.GetDatabaseOid(), table_object_id.GetObjectOid(), &table);
        if (!status.IsOK()) {
            ereport(ERROR, (errcode(status.pg_code), errmsg("Error loading table with oid %d in database %d",
                table_object_id.GetObjectOid(), table_object_id.GetDatabaseOid())));
        }
        table_cache_[t_table_uuid] = table;
    } else {
        table = cached_table->second;
    }

    std::string t_table_id = table_object_id.GetTableId();
    // check if the t_table_id is for a table or an index
    if (table->table_id().compare(t_table_id) == 0) {
        // a table
        return std::make_shared<PgTableDesc>(table);
    }

    // an index
    const auto itr = table->secondary_indexes().find(t_table_id);
    if (itr == table->secondary_indexes().end()) {
        ereport(ERROR, (errcode(ERRCODE_CASE_NOT_FOUND), errmsg("Error loading index with oid %s in database %s",
            t_table_id.c_str(), table->database_id().c_str())));
    }

    return std::make_shared<PgTableDesc>(itr->second, table->database_id());
}

}  // namespace k2pg
