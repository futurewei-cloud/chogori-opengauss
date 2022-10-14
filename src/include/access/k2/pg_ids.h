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
#include <set>

#include <boost/functional/hash/hash.hpp>
#include <boost/uuid/uuid_generators.hpp>

namespace k2pg {

typedef uint32_t PgOid;
static constexpr PgOid kPgInvalidOid = 0;
static constexpr PgOid kPgByteArrayOid = 17;

template <typename E>
constexpr typename std::underlying_type<E>::type to_underlying(E e) {
    return static_cast<typename std::underlying_type<E>::type>(e);
 }

class ObjectIdGenerator {
    public:
    ObjectIdGenerator() {}
    ~ObjectIdGenerator() {}

    std::string Next(bool binary_id = false);

    private:
    boost::uuids::random_generator oid_generator_;
};

// A class to identify a Postgres object by oid and the database oid it belongs to.
class PgObjectId {
    public:
    PgObjectId(const PgOid database_oid, const PgOid object_oid) : database_oid_(database_oid), object_oid_(object_oid) {
    }

    PgObjectId(): database_oid_(kPgInvalidOid), object_oid_(kPgInvalidOid) {
    }

    explicit PgObjectId(const std::string& table_uuid) {
        auto res = GetDatabaseOidByTableUuid(table_uuid);
        if (res != kPgInvalidOid) {
            database_oid_ = res;
        }
        res = GetTableOidByTableUuid(table_uuid);
        if (res != kPgInvalidOid) {
            object_oid_ = res;
        } else {
            // Reset the previously set database_oid.
            database_oid_ = kPgInvalidOid;
        }
    }

    // Get database uuid for a Postgres database.
    std::string GetDatabaseUuid() const;
    static std::string GetDatabaseUuid(const PgOid& database_oid);

    // Get table uuid for a Postgres table.
    std::string GetTableUuid() const;
    static std::string GetTableUuid(const PgOid& database_oid, const PgOid& table_oid);

    // Get table id string from table oid
    std::string GetTableId() const;
    static std::string GetTableId(const PgOid& table_oid);

    // Is the database/table uuid a Postgres database or table uuid?
    static bool IsPgsqlId(const std::string& uuid);

    // Get Postgres database and table oids from a namespace/table uuid.
    uint32_t GetDatabaseOidByUuid(const std::string& namespace_uuid);
    static uint32_t GetTableOidByTableUuid(const std::string& table_uuid);
    static uint32_t GetDatabaseOidByTableUuid(const std::string& table_uuid);
    std::string ToString() const;

    const PgOid GetDatabaseOid() const {
        return database_oid_;
    }

    const PgOid GetObjectOid() const {
        return object_oid_;
    }

    bool IsValid() const {
        return database_oid_ != kPgInvalidOid && object_oid_ != kPgInvalidOid;
    }

    bool operator== (const PgObjectId& other) const {
        return database_oid_ == other.GetDatabaseOid() && object_oid_ == other.GetObjectOid();
    }

    friend std::size_t hash_value(const PgObjectId& id) {
        std::size_t value = 0;
        boost::hash_combine(value, id.GetDatabaseOid());
        boost::hash_combine(value, id.GetObjectOid());
        return value;
    }

    private:
    PgOid database_oid_ = kPgInvalidOid;
    PgOid object_oid_ = kPgInvalidOid;
};

} // namespace k2pg
