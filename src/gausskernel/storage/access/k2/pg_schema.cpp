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

#include <set>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <string>
#include <sstream>
#include <assert.h>

#include "utils/errcodes.h"
#include "utils/elog.h"

#include "access/k2/pg_schema.h"

namespace k2pg {

    std::string ColumnSchema::ToString() const {
        std::stringstream ss;
        ss << name_ << "[" << TypeToString() << "]";
        return ss.str();
    }

    std::string ColumnSchema::TypeToString() const {
        std::stringstream ss;
        ss << (*type_) << ", " << (is_nullable_ ? "NULLABLE" : "NOT NULL") << ", " << (is_primary_ ? "PRIMARY KEY" : "NOT A PRIMARY KEY") << ", "
           << (is_hash_ ? "HASH KEY" : "NOT A HASH KEY") << ", " << order_;
        return ss.str();
    }

    Schema::Schema(const Schema& other) {
        CopyFrom(other);
    }

    Schema& Schema::operator=(const Schema& other) {
        if (&other != this) {
            CopyFrom(other);
        }
        return *this;
    }

    void Schema::CopyFrom(const Schema& other) {
        num_key_columns_ = other.num_key_columns_;
        num_hash_key_columns_ = other.num_hash_key_columns_;
        cols_ = other.cols_;
        col_ids_ = other.col_ids_;
        col_offsets_ = other.col_offsets_;
        id_to_index_ = other.id_to_index_;

        // We can't simply copy name_to_index_ since the GStringPiece keys
        // reference the other Schema's ColumnSchema objects.
        name_to_index_.clear();
        int i = 0;
        for (const ColumnSchema &col : cols_) {
            // The map uses the 'name' string from within the ColumnSchema object.
            name_to_index_[col.name()] = i++;
        }

        has_nullables_ = other.has_nullables_;
    }

    void Schema::swap(Schema& other) {
        std::swap(num_key_columns_, other.num_key_columns_);
        std::swap(num_hash_key_columns_, other.num_hash_key_columns_);
        cols_.swap(other.cols_);
        col_ids_.swap(other.col_ids_);
        col_offsets_.swap(other.col_offsets_);
        name_to_index_.swap(other.name_to_index_);
        id_to_index_.swap(other.id_to_index_);
        std::swap(has_nullables_, other.has_nullables_);
    }

    Status Schema::Reset(const std::vector<ColumnSchema>& cols,
                         const std::vector<ColumnId>& ids,
                         int key_columns) {
        cols_ = cols;
        num_key_columns_ = key_columns;
        num_hash_key_columns_ = 0;

        // Determine whether any column is nullable or static, and count number of hash columns.
        has_nullables_ = false;
        for (const ColumnSchema& col : cols_) {
            if (col.is_key()) {
                num_hash_key_columns_++;
            }
            if (col.is_nullable()) {
                has_nullables_ = true;
            }
        }

        if (key_columns > (int) cols_.size()) {
            Status status {
                .pg_code = ERRCODE_INVALID_SCHEMA_DEFINITION,
                .k2_code = 501,
                .msg = "Bad schema",
                .detail = "More key columns than columns"
            };
            return status;
        }

        if (key_columns < 0) {
            Status status {
                .pg_code = ERRCODE_INVALID_SCHEMA_DEFINITION,
                .k2_code = 501,
                .msg = "Bad schema",
                .detail = "Cannot specify a negative number of key columns"
            };
            return status;
        }

        if (!ids.empty() && ids.size() != cols_.size()) {
            Status status {
                .pg_code = ERRCODE_INVALID_SCHEMA_DEFINITION,
                .k2_code = 501,
                .msg = "Bad schema",
                .detail = "The number of ids does not match with the number of columns"
            };
            return status;
        }

        // Verify that the key columns are not nullable nor static
        for (int i = 0; i < key_columns; ++i) {
            if (cols_[i].is_nullable()) {
                Status status {
                    .pg_code = ERRCODE_INVALID_SCHEMA_DEFINITION,
                    .k2_code = 501,
                    .msg = "Bad schema",
                    .detail = "Nullable key columns are not supported: " + cols_[i].name()
                };
                return status;
            }
        }

        // Calculate the offset of each column in the row format.
        col_offsets_.reserve(cols_.size() + 1);  // Include space for total byte size at the end.
        size_t off = 0;
        size_t i = 0;
        name_to_index_.clear();
        for (const ColumnSchema &col : cols_) {
            // The map uses the 'name' string from within the ColumnSchema object.
            if (!name_to_index_.insert(std::make_pair(col.name(), i++)).second) {
                Status status {
                    .pg_code = ERRCODE_DUPLICATE_COLUMN,
                    .k2_code = 501,
                    .msg = "Bad schema",
                    .detail = "Duplicate column name " + col.name()
                };
                return status;
            }

            col_offsets_.push_back(off);
            // TODO: Port TypeInfo
            // off += col.type_info()->size();
        }

        // Add an extra element on the end for the total
        // byte size
        col_offsets_.push_back(off);

        // Initialize IDs mapping
        col_ids_ = ids;
        id_to_index_.clear();
        max_col_id_ = 0;
        for (size_t i = 0; i < ids.size(); ++i) {
            if (ids[i] > max_col_id_) {
                max_col_id_ = ids[i];
            }
            id_to_index_.insert(std::make_pair(ids[i], i));
        }

        // Ensure clustering columns have a default sorting type of 'ASC' if not specified.
        for (size_t i = num_hash_key_columns_; i < num_key_columns(); i++) {
            ColumnSchema& col = cols_[i];
            if (col.sorting_type() == ColumnSchema::SortingType::kNotSpecified) {
                col.set_sorting_type(ColumnSchema::SortingType::kAscending);
            }
        }

        return Status::OK;
    }

    std::string Schema::ToString() const {
        std::stringstream ss;
        ss << "Schema [\n\t";
         if (has_column_ids()) {
            for (size_t i = 0; i < cols_.size(); ++i) {
                ss << col_ids_[i] << ":" << cols_[i].ToString() << ",\n\t";
            }
        } else {
            for (const ColumnSchema &col : cols_) {
                ss << col.ToString() << ",\n\t";
            }
        }
        ss << "]\n";
        return ss.str();
    }

    ColumnId Schema::ColumnIdByName(const std::string& column_name) const {
        int column_index = find_column(column_name);
        if (column_index != Schema::kColumnNotFound) {
            return ColumnId(column_id(column_index));
        }
        return Schema::kColumnNotFound;
    }

    std::pair<bool, ColumnId> Schema::FindColumnIdByName(const std::string& column_name) const {
        int column_index = find_column(column_name);
        if (column_index == Schema::kColumnNotFound) {
            return std::make_pair<bool, ColumnId>(false, -1);
        }
        return std::make_pair<bool, ColumnId>(true, ColumnId(column_id(column_index)));
    }

    ColumnId Schema::first_column_id() {
        return kFirstColumnId;
    }

    std::vector<ColumnId> IndexInfo::index_key_column_ids() const {
        std::unordered_map<ColumnId, ColumnId> map;
        for (const auto column : columns_) {
            map[column.base_column_id] = column.column_id;
        }
        std::vector<ColumnId> ids;
        ids.reserve(indexed_hash_column_ids_.size() + indexed_range_column_ids_.size());
        for (const auto id : indexed_hash_column_ids_) {
            ids.push_back(map[id]);
        }
        for (const auto id : indexed_range_column_ids_) {
            ids.push_back(map[id]);
        }
        return ids;
    }

    bool IndexInfo::PrimaryKeyColumnsOnly(const Schema& indexed_schema) const {
        for (size_t i = 0; i < hash_column_count_ + range_column_count_; i++) {
            if (!indexed_schema.is_key_column(columns_[i].base_column_id)) {
                return false;
            }
        }
        return true;
    }

    // Check for dependency is used for DDL operations, so it does not need to be fast. As a result,
    // the dependency list does not need to be cached in a member id list for fast access.
    bool IndexInfo::CheckColumnDependency(ColumnId column_id) const {
        for (const IndexColumn &index_col : columns_) {
            // The index data contains IDs of all columns that this index is referencing.
            // Examples:
            // 1. Index by column
            // - INDEX ON tab (a_column)
            // - The ID of "a_column" is included in index data.
            //
            // 2. Index by expression of column:
            // - INDEX ON tab (j_column->>'field')
            // - The ID of "j_column" is included in index data.
            if (index_col.base_column_id == column_id) {
                return true;
            }
        }
        return false;
    }

    int32_t IndexInfo::FindKeyIndex(const std::string& key_expr_name) const {
        for (size_t idx = 0; idx < key_column_count(); idx++) {
            const auto& col = columns_[idx];
            if (!col.column_name.empty() && key_expr_name.find(col.column_name) != key_expr_name.npos) {
                // Return the found key column that is referenced by the expression.
                return (int32_t)idx;
            }
        }

        return -1;
    }

    const IndexInfo* IndexMap::FindIndex(const std::string& index_id) const {
        const auto itr = find(index_id);
        if (itr == end()) {
            elog(ERROR, "Index id %s not found", index_id.c_str());
            return NULL;
        }
        return &itr->second;
    }

    const IndexInfo* TableInfo::FindIndex(const std::string& index_id) const {
        return index_map_.FindIndex(index_id);
    }

    std::shared_ptr<TableInfo> TableInfo::Clone(std::shared_ptr<TableInfo> table_info, std::string database_id,
            std::string database_name, std::string table_uuid, std::string table_name) {
        std::shared_ptr<TableInfo> new_table_info = std::make_shared<TableInfo>(database_id, database_name, table_info->table_oid(), table_name, table_uuid, table_info->schema());
        new_table_info->set_next_column_id(table_info->next_column_id());
        new_table_info->set_is_sys_table(table_info->is_sys_table());
        if (table_info->has_secondary_indexes()) {
            for (std::pair<std::string, IndexInfo> secondary_index : table_info->secondary_indexes()) {
                new_table_info->add_secondary_index(secondary_index.first, secondary_index.second);
            }
        }

        return new_table_info;
    }

}  // namespace k2pg
