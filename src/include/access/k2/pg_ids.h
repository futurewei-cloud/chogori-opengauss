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

// PostgreSQL can represent text strings up to 1 GB minus a four-byte header.
static const int64_t kMaxPostgresTextSizeBytes = 1024ll * 1024 * 1024 - 4;

// type oids defined in pg_type_d.h, which was generated by the build script
static const int32_t BOOL_TYPE_OID = 16;
static const int32_t STRING_TYPE_OID = 19;

// Datatype representation:
// Definition of a datatype is divided into two different sections.
// - K2PgTypeEntity is used to keep static information of a datatype.
// - K2PgTypeAttrs is used to keep customizable information of a datatype.
//
struct PgTypeAttrs {
  // Currently, we only need typmod, but we might need more datatype information in the future.
  // For example, array dimensions might be needed.
  int32_t typmod;
};

// Datatype conversion functions.
typedef void (*K2PgDatumToData)(uint64_t datum, void *ybdata, int64_t *bytes);
typedef uint64_t (*K2PgDatumFromData)(const void *ybdata, int64_t bytes,
                                       const PgTypeAttrs *type_attrs);

struct PgTypeEntity {
  // Postgres type OID.
  int type_oid;

  // K2 SQL type.
  K2SqlDataType k2pg_type;

  // Allow to be used for primary key.
  bool allow_for_primary_key;

  // Datum in-memory fixed size.
  // - Size of in-memory representation for a type. Usually it's sizeof(a_struct).
  //   Example: BIGINT in-memory size === sizeof(int64)
  //            POINT in-memory size === sizeof(struct Point)
  // - Set to (-1) for types of variable in-memory size - VARSIZE_ANY should be used.
  int64_t datum_fixed_size;

  // Converting Postgres datum to K2 SQL expression.
  K2PgDatumToData datum_to_k2pg;

  // Converting K2 SQL values to Postgres in-memory-formatted datum.
  K2PgDatumFromData k2pg_to_datum;
};

} // namespace k2pg