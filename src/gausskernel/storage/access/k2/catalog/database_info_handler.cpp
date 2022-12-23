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

#include "database_info_handler.h"

namespace k2pg {
namespace catalog {

using namespace sh;

DatabaseInfoHandler::DatabaseInfoHandler() {
    schema_ptr_ = std::make_shared<sh::dto::Schema>(schema_);
}

DatabaseInfoHandler::~DatabaseInfoHandler() {
}

// Verify the database_info corresponding SKVSchema in the PG primary SKVCollection doesn't exist and create it
// Called only once in sql_catalog_manager::InitPrimaryCluster()
sh::Status DatabaseInfoHandler::InitDatabaseTable() {
    // check to make sure the schema doesn't exists
    auto result = TXMgr.getSchema(collection_name_, schema_.name).get();
    auto status = std::get<0>(result);
    if (status.code != 404) {  // expect NotFound
        if (status.is2xxOK()) {
            // K2LOG_ECT(log::catalog, "Unexpected DatabaseInfo SKV schema already exists during init.");
            return sh::Statuses::S500_Internal_Server_Error("Unexpected DatabaseInfo SKV schema already exists during init.");
        }
        // K2LOG_ECT(log::catalog, "Unexpected DatabaseInfo SKV schema read error during init.{}", status);
        return status;
    }

    // create the table schema since it does not exist
    auto createResult = TXMgr.createSchema(collection_name_, schema_).get();
    status = std::get<0>(createResult);
    if (!status.is2xxOK()) {
        // K2LOG_ECT(log::catalog, "Failed to create schema for {} in {}, due to {}", schema_ptr_->name, collection_name_, status);
        return status;
    }

    // K2LOG_DCT(log::catalog, "succeeded schema as {} in {}", schema_ptr_->name, collection_name_);
    return status;
}

DatabaseInfo DatabaseInfoHandler::getInfo(dto::SKVRecord& record) {
    DatabaseInfo info;
    info.database_id = record.deserializeNext<sh::String>().value();
    info.database_name = record.deserializeNext<sh::String>().value();
     // use int64_t to represent uint32_t since since SKV does not support them
    info.database_oid = record.deserializeNext<int64_t>().value();
    info.next_pg_oid = record.deserializeNext<int64_t>().value();
    return info;
}

sh::dto::SKVRecord DatabaseInfoHandler::getRecord(const DatabaseInfo& info) {
    dto::SKVRecordBuilder builder(collection_name_, schema_ptr_);
    builder.serializeNext<sh::String>(info.database_id);
    builder.serializeNext<sh::String>(info.database_name);
    // use int64_t to represent uint32_t since since SKV does not support them
    builder.serializeNext<int64_t>(info.database_oid);
    builder.serializeNext<int64_t>(info.next_pg_oid);
    return builder.build();
}

sh::Status DatabaseInfoHandler::UpsertDatabase(const DatabaseInfo& database_info) {
    dto::SKVRecord record = getRecord(database_info);
    auto [write_status] = TXMgr.write(record, false).get();
    if (!write_status.is2xxOK()) {
        // K2LOG_EWT(log::catalog, "Failed to upsert databaseinfo record {} due to {}", database_info.database_id, write_status);
        return write_status;
    }
    return write_status;
}

sh::Response<DatabaseInfo> DatabaseInfoHandler::GetDatabase(const std::string& database_id) {
    sh::dto::SKVRecordBuilder keyBuilder(collection_name_, schema_ptr_);
    keyBuilder.serializeNext<sh::String>(database_id);
    auto rec = keyBuilder.build();

    auto [read_status, value] = TXMgr.read(rec).get();
    if (!read_status.is2xxOK()) {
        // K2LOG_ERT(log::catalog, "Failed to read SKV record {} {} {} due to {}", collection_name_, schema_ptr_->name, database_id, read_status);
        return sh::Response<DatabaseInfo>(read_status, {});
    }
    DatabaseInfo info = getInfo(value);
    return sh::Response<DatabaseInfo>(read_status, info);
}

sh::Response< std::vector<DatabaseInfo>> DatabaseInfoHandler::ListDatabases() {
    std::vector<DatabaseInfo> infos;

    // For a forward full schema scan in SKV, we need to explictly set the start record
    sh::dto::SKVRecordBuilder startBuilder(collection_name_, schema_ptr_);
    startBuilder.serializeNext<sh::String>("");
    auto startKey = startBuilder.build();
    dto::SKVRecordBuilder endBuilder(collection_name_, schema_ptr_);
    auto endKey = endBuilder.build();

    auto [create_status, query] = TXMgr.createQuery(startKey, endKey).get();
    if (!create_status.is2xxOK()) {
        // K2LOG_ERT(log::catalog, "Failed to create scan read for ListDatabases due to {} in collection {}.", create_status, collection_name_);
        return std::make_tuple(create_status, infos);
    }

    bool done = false;
    do {
        auto [status, result] = TXMgr.query( query).get();

        if (!status.is2xxOK()) {
            // K2LOG_ERT(log::catalog, "Failed to run scan read due to {}", status);
            return std::make_tuple(status, infos);
        }

        for (auto& storage : result.records) {
            dto::SKVRecord record(collection_name_, schema_ptr_, std::move(storage));
            DatabaseInfo info = getInfo(record);
            infos.push_back(std::move(info));
        }
        done = result.done;
    } while (!done);

    return std::make_tuple(sh::Statuses::S200_OK, infos);
}

sh::Status DatabaseInfoHandler::DeleteDatabase(DatabaseInfo& info) {
    sh::dto::SKVRecord record = getRecord(info);
    auto [status]  = TXMgr.write(record, true).get();
    if (!status.is2xxOK()) {
        // K2LOG_ECT(log::catalog, "Failed to delete database ID {} in Collection {}, due to {}",
            // info.database_id, collection_name_, status);
        return status;
    }
    return status;
}

} // namespace catalog
} // namespace k2pg
