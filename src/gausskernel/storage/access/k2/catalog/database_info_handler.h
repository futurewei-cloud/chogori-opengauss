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

#include "sql_catalog_entity.h"

namespace k2pg {
namespace catalog {

class DatabaseInfoHandler {
public:
    static inline const sh::dto::Schema schema_ {
        .name = "K2RESVD_SCHEMA_SQL_DATABASE_META",
        .version = 1,
        .fields = std::vector<sh::dto::SchemaField> {
                {sh::dto::FieldType::STRING, "DatabaseId", false, false},
                {sh::dto::FieldType::STRING, "DatabaseName", false, false},
                {sh::dto::FieldType::INT64T, "DatabaseOid", false, false},
                {sh::dto::FieldType::INT64T, "NextPgOid", false, false}},
        .partitionKeyFields = std::vector<uint32_t> { 0 },
        .rangeKeyFields = std::vector<uint32_t> {}
    };

    DatabaseInfoHandler();

    ~DatabaseInfoHandler();

    sh::Status InitDatabaseTable();

    sh::Status UpsertDatabase(const DatabaseInfo& database_info);

    sh::Response<DatabaseInfo> GetDatabase(const std::string& database_id);

    sh::Response< std::vector<DatabaseInfo>> ListDatabases();

    sh::Status DeleteDatabase(DatabaseInfo& database_info);

    // TODO(ahsank): add partial update for next_pg_oid once SKV supports partial update

private:
    // Deserialize DatabaseInfo from an skv record
    DatabaseInfo getInfo(sh::dto::SKVRecord& record);
    // Serialize DatabaseInfo to an skv record
    sh::dto::SKVRecord getRecord(const DatabaseInfo& info);

    std::string collection_name_ ="K2RESVD_COLLECTION_SQL_PRIMARY_CLUSTER";
    std::shared_ptr<sh::dto::Schema> schema_ptr_;
};

} // namespace catalog
} // namespace k2pg
