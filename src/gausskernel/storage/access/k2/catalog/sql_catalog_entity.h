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
#include <k2/logging/Log.h>
#include "../session.h"

namespace k2pg {
namespace sql {
namespace catalog {

namespace log {
inline thread_local k2::logging::Logger catalog("k2::pg_catalog");
}

#define CHECKED_STATUS sh::Status

class ClusterInfo {
public:
    // cluster id, could be randomly generated or from a configuration parameter
    std::string cluster_id;

    // Right now, the K2PG logic in PG uses a single global catalog caching version defined in
    //  src/include/pg_k2pg_utils.h
    //
    //  extern uint64_t k2pg_catalog_cache_version;
    //
    // to check if the catalog needs to be refreshed or not. To not break the above caching
    // logic, we need to store the catalog_version as a global variable here.
    //
    // TODO: update both K2PG logic in PG, PG gate APIs, and catalog manager to be more fine-grained to
    // reduce frequency and/or duration of cache refreshes. One good example is to use a separate
    // catalog version for a database, however, we do need to consider the catalog version change
    // for shared system tables in PG if we go this path.
    //
    // Only certain system catalogs (such as pg_database) are shared.
    uint64_t catalog_version;

    // whether initdb, i.e., PG bootstrap procedure to create template DBs, has been done or not
    bool initdb_done = false;
};

// DatabaseInfo is a cluster level system table holding info for each database,
// currently its Name/Oid, and next PgOid(for objects inside this DB).
class DatabaseInfo {
public:
    // encoded id, for example, uuid
    std::string database_id;

    // name
    std::string database_name;

    // object id assigned by PG
    uint32_t database_oid;

    // next PG Oid that is available for object id assignment for this namespace
    uint32_t next_pg_oid;
};

} // namespace catalog
} // namespace sql
} // namespace k2pg
