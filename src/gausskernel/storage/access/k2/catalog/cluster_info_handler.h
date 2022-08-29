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

#include "sql_catalog_entity.h"

namespace k2pg {
namespace catalog {

class ClusterInfoHandler {
public:
    static inline const sh::dto::Schema schema_ {
        .name = "K2RESVD_SCHEMA_SQL_CLUSTER_META",
        .version = 1,
        .fields = std::vector<sh::dto::SchemaField> {
                {sh::dto::FieldType::STRING, "ClusterId", false, false},
                {sh::dto::FieldType::INT64T, "CatalogVersion", false, false},
                {sh::dto::FieldType::BOOL, "InitDbDone", false, false}},
        .partitionKeyFields = std::vector<uint32_t> { 0 },
        .rangeKeyFields = std::vector<uint32_t> {}
    };

    ClusterInfoHandler();
    ~ClusterInfoHandler();

    sh::Status InitClusterInfo(ClusterInfo& cluster_info);

    sh::Status UpdateClusterInfo(ClusterInfo& cluster_info);

    sh::Response<ClusterInfo> GetClusterInfo(const std::string& cluster_id);

private:
    std::string collection_name_ = "K2RESVD_COLLECTION_SQL_PRIMARY_CLUSTER";
    std::shared_ptr<sh::dto::Schema> schema_ptr_;
};

} // namespace catalog
} // namespace k2pg
