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

#include "cluster_info_handler.h"

namespace k2pg {
namespace catalog {

using namespace sh;

ClusterInfoHandler::ClusterInfoHandler() {
    schema_ptr_ = std::make_shared<sh::dto::Schema>(schema_);
}

ClusterInfoHandler::~ClusterInfoHandler() {
}

// Called only once in sql_catalog_manager::InitPrimaryCluster()
sh::Status ClusterInfoHandler::InitClusterInfo(ClusterInfo& cluster_info) {
    auto [status] = TXMgr.createSchema(collection_name_, schema_).get();
    if (!status.is2xxOK()) {
        K2LOG_ECT(log::catalog, "Failed to create schema for {} in {}, due to {}", schema_ptr_->name, collection_name_, status);
        return status;
    }

    status = UpdateClusterInfo(cluster_info);
    return status;
}

sh::Status ClusterInfoHandler::UpdateClusterInfo(ClusterInfo& cluster_info) {
    sh::dto::SKVRecordBuilder builder(collection_name_, schema_ptr_);
    builder.serializeNext<sh::String>(cluster_info.cluster_id);
    // use signed integers for unsigned integers since SKV does not support them
    builder.serializeNext<int64_t>(cluster_info.catalog_version);
    builder.serializeNext<bool>(cluster_info.initdb_done);
    auto record = builder.build();

    auto [status] = TXMgr.write(record, false).get();
    if (!status.is2xxOK()) {
        K2LOG_ECT(log::catalog, "Failed to upsert cluster info record due to {}", status);
        return status;
    }
    return status;
}

sh::Response<ClusterInfo> ClusterInfoHandler::GetClusterInfo(const std::string& cluster_id) {
    ClusterInfo info;
    dto::SKVRecordBuilder keyBuilder(collection_name_, schema_ptr_);
    keyBuilder.serializeNext<sh::String>(cluster_id);
    auto key = keyBuilder.build();

    auto [status, record] = TXMgr.read(key).get();
    if (!status.is2xxOK()) {
        K2LOG_ECT(log::catalog, "Failed to read SKV record due to {}", status);
        return std::make_tuple(status, info);
    }

    info.cluster_id = record.deserializeNext<sh::String>().value();
    // use signed integers for unsigned integers since SKV does not support them
    info.catalog_version = record.deserializeNext<int64_t>().value();
    info.initdb_done = record.deserializeNext<bool>().value();
    return std::make_tuple(status, info);
}
} // namespace sql
} // namespace k2pg
