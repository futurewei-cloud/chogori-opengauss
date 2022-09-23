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
    table_cache_.erase(table_obj_id.GetTableUuid());
}

std::shared_ptr<PgTableDesc> PgSession::LoadTable(const PgOid database_oid, const PgOid object_oid) {
    PgObjectId pg_object(database_oid, object_oid);
    return LoadTable(pg_object);
}

std::shared_ptr<PgTableDesc> PgSession::LoadTable(const PgObjectId& table_object_id) {
    std::string t_table_uuid = table_object_id.GetTableUuid();

    auto cached_table = table_cache_.find(t_table_uuid);
    if (cached_table == table_cache_.end()) {
        std::shared_ptr<TableInfo> table;
        Status status = catalog_client_->OpenTable(table_object_id.GetDatabaseOid(), table_object_id.GetObjectOid(), &table);
        if (!status.IsOK()) {
            ereport(ERROR, (errcode(status.pg_code), errmsg("Error loading table with oid %d in database %d",
                table_object_id.GetObjectOid(), table_object_id.GetDatabaseOid())));
        }

        std::shared_ptr<PgTableDesc> table_desc;
        std::string t_table_id = std::move(table_object_id.GetTableId());
        // check if the t_table_id is for a table or an index
        if (table->table_id().compare(t_table_id) == 0) {
            // a table
            table_desc = std::make_shared<PgTableDesc>(table);
        } else {
            // an index
            const auto itr = table->secondary_indexes().find(t_table_id);
            if (itr == table->secondary_indexes().end()) {
                ereport(ERROR, (errcode(ERRCODE_CASE_NOT_FOUND), errmsg("Error loading index with oid %s in database %s",
                    t_table_id.c_str(), table->database_id().c_str())));
            }
            table_desc = std::make_shared<PgTableDesc>(itr->second, table->database_id());
        }
        // cache it
        table_cache_[t_table_uuid] = table_desc;
        return table_desc;
    } else {
        return cached_table->second;
    }
}

bool operator==(const PgForeignKeyReference& k1, const PgForeignKeyReference& k2) {
  return k1.table_oid == k2.table_oid &&
      k1.k2pgctid == k2.k2pgctid;
}

size_t hash_value(const PgForeignKeyReference& key) {
  size_t hash = 0;
  boost::hash_combine(hash, key.table_oid);
  boost::hash_combine(hash, key.k2pgctid);
  return hash;
}

bool PgSession::ForeignKeyReferenceExists(uint32_t table_oid, std::string&& k2pgctid) {
  PgForeignKeyReference reference{table_oid, std::move(k2pgctid)};
  return fk_reference_cache_.find(reference) != fk_reference_cache_.end();
}

Status PgSession::CacheForeignKeyReference(uint32_t table_oid, std::string&& k2pgctid) {
  PgForeignKeyReference reference{table_oid, std::move(k2pgctid)};
  fk_reference_cache_.emplace(reference);
  return Status::OK;
}

Status PgSession::DeleteForeignKeyReference(uint32_t table_oid, std::string&& k2pgctid) {
  PgForeignKeyReference reference{table_oid, std::move(k2pgctid)};
  fk_reference_cache_.erase(reference);
  return Status::OK;
}

}  // namespace k2pg
