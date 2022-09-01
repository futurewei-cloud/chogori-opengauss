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

#include <string>
#include <sstream>
#include <assert.h>
#include <boost/uuid/nil_generator.hpp>

#include "access/k2/pg_ids.h"
#include "access/k2/status.h"

namespace k2pg {
using boost::uuids::uuid;

static constexpr int kUuidVersion = 3; // Repurpose old name-based UUID v3 to embed Postgres oids.

namespace {
// TODO: get rid of the 16 byid UUID, and use PG dboid and tabld oid directly.
// Layout of Postgres database and table 4-byte oids in a the 16-byte table UUID:
//
// +-----------------------------------------------------------------------------------------------+
// |  0  |  1  |  2  |  3  |  4  |  5  |  6  |  7  |  8  |  9  |  10 |  11 |  12 |  13 |  14 |  15 |
// +-----------------------------------------------------------------------------------------------+
// |        database       |           | vsn |     | var |     |           |        table          |
// |          oid          |           |     |     |     |     |           |         oid           |
// +-----------------------------------------------------------------------------------------------+
static char hex_chars[] = "0123456789abcdef";

void UuidSetDatabaseId(const uint32_t database_oid, uuid* id) {
  id->data[0] = database_oid >> 24 & 0xFF;
  id->data[1] = database_oid >> 16 & 0xFF;
  id->data[2] = database_oid >> 8  & 0xFF;
  id->data[3] = database_oid & 0xFF;
}

void UuidSetTableIds(const uint32_t table_oid, uuid* id) {
  id->data[12] = table_oid >> 24 & 0xFF;
  id->data[13] = table_oid >> 16 & 0xFF;
  id->data[14] = table_oid >> 8  & 0xFF;
  id->data[15] = table_oid & 0xFF;
}

std::string b2a_hex(const char* input, int len) {
  std::string result;
  result.resize(len << 1);
  const unsigned char* b = reinterpret_cast<const unsigned char*>(input);
  for (int i = 0; i < len; i++) {
    result[i * 2 + 0] = hex_chars[b[i] >> 4];
    result[i * 2 + 1] = hex_chars[b[i] & 0xf];
  }
  return result;
}

std::string UuidToString(uuid* id) {
  // Set variant that is stored in octet 7, which is index 8, since indexes count backwards.
  // Variant must be 0b10xxxxxx for RFC 4122 UUID variant 1.
  id->data[8] &= 0xBF;
  id->data[8] |= 0x80;

  // Set version that is stored in octet 9 which is index 6, since indexes count backwards.
  id->data[6] &= 0x0F;
  id->data[6] |= (kUuidVersion << 4);
  return b2a_hex(reinterpret_cast<const char *>(id->data), sizeof(id->data));
}

} // namespace

std::string ObjectIdGenerator::Next(const bool binary_id) {
  boost::uuids::uuid oid = oid_generator_();
  return binary_id ? std::string(reinterpret_cast<const char *>(oid.data), sizeof(oid.data))
                   : b2a_hex(reinterpret_cast<const char *>(oid.data), sizeof(oid.data));
}

std::string PgObjectId::ToString() const {
  std::stringstream ss;
  ss << "(" << database_oid_ << ", " << object_oid_ << ")";
  return ss.str();
}

std::string PgObjectId::GetDatabaseUuid() const {
  return GetDatabaseUuid(database_oid_);
}

std::string PgObjectId::GetDatabaseUuid(const PgOid& database_oid) {
  uuid id = boost::uuids::nil_uuid();
  UuidSetDatabaseId(database_oid, &id);
  return UuidToString(&id);
}

std::string PgObjectId::GetTableUuid(const PgOid& database_oid, const PgOid& table_oid) {
  uuid id = boost::uuids::nil_uuid();
  UuidSetDatabaseId(database_oid, &id);
  UuidSetTableIds(table_oid, &id);
  return UuidToString(&id);
}

std::string PgObjectId::GetTableUuid() const {
  return GetTableUuid(database_oid_, object_oid_);
}

std::string PgObjectId::GetTableId() const {
  return GetTableId(object_oid_);
}

std::string PgObjectId::GetTableId(const PgOid& table_oid) {
// table id is a uuid without database oid information
  return GetTableUuid(kPgInvalidOid, table_oid);
}

bool PgObjectId::IsPgsqlId(const std::string& uuid) {
  if (uuid.size() != 32) return false; // Ignore non-UUID string like "sys.catalog.uuid"
  try {
    size_t pos = 0;
    const int version = std::stoi(uuid.substr(6 * 2, 2), &pos, 16);
    if ((pos == 2) && (version & 0xF0) >> 4 == kUuidVersion) return true;

  } catch(const std::invalid_argument&) {
  } catch(const std::out_of_range&) {
    // TODO: log the actual exceptions
  }

  return false;
}

uint32_t PgObjectId::GetDatabaseOidByUuid(const std::string& database_uuid) {
  assert(IsPgsqlId(database_uuid));
  try {
    size_t pos = 0;
    const uint32_t oid = stoul(database_uuid.substr(0, sizeof(uint32_t) * 2), &pos, 16);
    if (pos == sizeof(uint32_t) * 2) {
      return oid;
    }
  } catch(const std::invalid_argument&) {
  } catch(const std::out_of_range&) {
    // TODO: log the actual exceptions
  }
  return kPgInvalidOid;
}

uint32_t PgObjectId::GetTableOidByTableUuid(const std::string& table_uuid) {
  assert(IsPgsqlId(table_uuid));
  try {
    size_t pos = 0;
    const uint32_t oid = stoul(table_uuid.substr(12 * 2, sizeof(uint32_t) * 2), &pos, 16);
    if (pos == sizeof(uint32_t) * 2) {
      return oid;
    }
  } catch(const std::invalid_argument&) {
  } catch(const std::out_of_range&) {
    // TODO: log the actual exceptions
  }
  return kPgInvalidOid;
}

uint32_t PgObjectId::GetDatabaseOidByTableUuid(const std::string& table_uuid) {
  assert(IsPgsqlId(table_uuid));
  try {
    size_t pos = 0;
    const uint32_t oid = stoul(table_uuid.substr(0, sizeof(uint32_t) * 2), &pos, 16);
    if (pos == sizeof(uint32_t) * 2) {
      return oid;
    }
  } catch(const std::invalid_argument&) {
  } catch(const std::out_of_range&) {
      // TODO: log the actual exceptions
  }
  return kPgInvalidOid;
}

}  // namespace k2pg
