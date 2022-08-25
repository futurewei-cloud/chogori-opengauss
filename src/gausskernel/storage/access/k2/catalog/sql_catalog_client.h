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

#include "sql_catalog_manager.h"

namespace k2pg {
namespace sql {
namespace catalog {

// TODO: This catalog client layer so far doesn't provide value, consider eliminate it later
class SqlCatalogClient {
public:
    SqlCatalogClient(std::shared_ptr<SqlCatalogManager> catalog_manager) : catalog_manager_(catalog_manager) {
    };

    ~SqlCatalogClient() {};

    CHECKED_STATUS IsInitDbDone(bool* isDone);

    CHECKED_STATUS InitPrimaryCluster();

    CHECKED_STATUS FinishInitDB();

    // Create a new database with the given name.
    CHECKED_STATUS CreateDatabase(const std::string& database_name,
                                const std::string& database_id,
                                uint32_t database_oid,
                                const std::string& source_database_id,
                                const std::string& creator_role_name,
                                const std::optional<uint32_t>& next_pg_oid = std::nullopt);

    // Delete database with the given name.
    CHECKED_STATUS DeleteDatabase(const std::string& database_name,
                                const std::string& database_id);

    CHECKED_STATUS UseDatabase(const std::string& database_name);

    CHECKED_STATUS GetCatalogVersion(uint64_t *pg_catalog_version);

    CHECKED_STATUS IncrementCatalogVersion();

private:
    std::shared_ptr<SqlCatalogManager> catalog_manager_;
};

} // namespace catalog
}  // namespace sql
}  // namespace k2pg
