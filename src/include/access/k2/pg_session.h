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
#include <memory>
#include <string>
#include <unordered_map>
#include <atomic>

#include "access/k2/status.h"
#include "access/k2/pg_ids.h"
#include "access/k2/pg_schema.h"
#include "access/k2/pg_tabledesc.h"
namespace k2pg {

namespace catalog {
    // forward definition of SqlCatalogClient to avoid bring in various dependencies
    class SqlCatalogClient;
}

class PgSession {
public:
    // Constructors.
    PgSession(std::shared_ptr<k2pg::catalog::SqlCatalogClient> catalog_client, const std::string& database_name);

    virtual ~PgSession();

    std::shared_ptr<k2pg::catalog::SqlCatalogClient> GetCatalogClient() {
        return catalog_client_;
    }

    Status ConnectDatabase(const std::string& database_name);

    std::string& current_database() {
        return connected_database_;
    };

    const std::string& GetClientId() const {
        return client_id_;
    };

    int64_t GetNextStmtId() {
        // TODO: add more complext stmt id generation logic
        return stmt_id_++;
    };

    void InvalidateCache() {
        table_cache_.clear();
    }

    std::shared_ptr<PgTableDesc> LoadTable(const PgObjectId& table_object_id);

    std::shared_ptr<PgTableDesc> LoadTable(const PgOid database_oid, const PgOid object_oid);

    void InvalidateTableCache(const PgObjectId& table_object_id);

private:
    std::shared_ptr<k2pg::catalog::SqlCatalogClient> catalog_client_;

    // Connected database.
    std::string connected_database_;

    // table cache
    std::unordered_map<std::string, std::shared_ptr<PgTableDesc>> table_cache_;

    std::string client_id_;

    std::atomic<int64_t> stmt_id_;
};

}  // namespace k2pg
