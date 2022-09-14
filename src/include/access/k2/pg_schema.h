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
#include <vector>
#include <unordered_map>
#include <assert.h>

#include "access/k2/pg_ids.h"
#include "access/k2/pg_param.h"
#include "access/k2/pg_type.h"
#include "access/k2/pg_expr.h"

namespace k2pg {
    typedef int32_t ColumnId;
    constexpr ColumnId kFirstColumnId = 0;

    enum IndexPermissions {
        INDEX_PERM_DELETE_ONLY = 0,
        INDEX_PERM_WRITE_AND_DELETE = 2,
        INDEX_PERM_DO_BACKFILL = 4,
        // This is the "success" state, where the index is completely usable.
        INDEX_PERM_READ_WRITE_AND_DELETE = 6,
        // Used while removing an index -- either due to backfill failure, or
        // due to a client requested "drop index".
        INDEX_PERM_WRITE_AND_DELETE_WHILE_REMOVING = 8,
        INDEX_PERM_DELETE_ONLY_WHILE_REMOVING = 10,
        INDEX_PERM_INDEX_UNUSED = 12,
        // Used as a sentinel value.
        INDEX_PERM_NOT_USED = 14
    };

    class ColumnSchema {
        public:
        enum SortingType : uint8_t {
            kNotSpecified = 0,
            kAscending,          // ASC, NULLS FIRST
            kDescending,         // DESC, NULLS FIRST
            kAscendingNullsLast, // ASC, NULLS LAST
            kDescendingNullsLast // DESC, NULLS LAST
        };

        ColumnSchema(std::string name,
                const std::shared_ptr<SQLType>& type,
                bool is_nullable,
                bool is_primary,
                bool is_hash,
                int32_t order,
                SortingType sorting_type)
            : name_(std::move(name)),
            type_(type),
            is_nullable_(is_nullable),
            is_primary_(is_primary),
            is_hash_(is_hash),
            order_(order),
            sorting_type_(sorting_type) {
        }

        // convenience constructor for creating columns with simple (non-parametric) data types
        ColumnSchema(std::string name,
                DataType type,
                bool is_nullable,
                bool is_primary,
                bool is_hash,
                int32_t order,
                SortingType sorting_type)
            : ColumnSchema(name, SQLType::Create(type), is_nullable, is_primary, is_hash, order, sorting_type) {
        }

        const std::shared_ptr<SQLType>& type() const {
            return type_;
        }

        void set_type(const std::shared_ptr<SQLType>& type) {
            type_ = type;
        }

        bool is_nullable() const {
            return is_nullable_;
        }

        bool is_key() const {
            return is_hash_ || is_primary_;
        }

        bool is_hash() const {
            return is_hash_;
        }

        bool is_primary() const {
            return is_primary_;
        }

        int32_t order() const {
            return order_;
        }

        SortingType sorting_type() const {
            return sorting_type_;
        }

        void set_sorting_type(SortingType sorting_type) {
            sorting_type_ = sorting_type;
        }

        const std::string &name() const {
            return name_;
        }

        // Return a string identifying this column, including its
        // name.
        std::string ToString() const;

        // Same as above, but only including the type information.
        // For example, "STRING NOT NULL".
        std::string TypeToString() const;

        bool EqualsType(const ColumnSchema &other) const {
            return is_nullable_ == other.is_nullable_ &&
                   is_primary_ == other.is_primary_ &&
                   is_hash_ == other.is_hash_ &&
                   sorting_type_ == other.sorting_type_ &&
                   type_ == other.type();
        }

        bool Equals(const ColumnSchema &other) const {
            return EqualsType(other) && this->name_ == other.name_;
        }

        private:
        void set_name(const std::string& name) {
            name_ = name;
        }

        std::string name_;
        std::shared_ptr<SQLType> type_;
        bool is_nullable_;
        bool is_primary_;
        bool is_hash_;
        int32_t order_;
        SortingType sorting_type_;
    };

    class Schema {
        public:

        static const int kColumnNotFound = -1;

        Schema()
        : num_key_columns_(0),
            num_hash_key_columns_(0),
            has_nullables_(false) {
        }

        Schema(const Schema& other);

        void swap(Schema& other); // NOLINT(build/include_what_you_use)

        void CopyFrom(const Schema& other);
        
        // Construct a schema with the given information.
        //
        // NOTE: if the schema is user-provided, it's better to construct an
        // empty schema and then use Reset(...)  so that errors can be
        // caught. If an invalid schema is passed to this constructor, an
        // assertion will be fired!
        Schema(const std::vector<ColumnSchema>& cols,
                int key_columns) {
            Reset(cols, key_columns);
        }

        // Construct a schema with the given information.
        //
        // NOTE: if the schema is user-provided, it's better to construct an
        // empty schema and then use Reset(...)  so that errors can be
        // caught. If an invalid schema is passed to this constructor, an
        // assertion will be fired!
        Schema(const std::vector<ColumnSchema>& cols,
                const std::vector<ColumnId>& ids,
                int key_columns) {
            Reset(cols, ids, key_columns);
        }

        // Reset this Schema object to the given schema.
        // If this fails, the Schema object is left in an inconsistent
        // state and may not be used.
        Status Reset(const std::vector<ColumnSchema>& cols, int key_columns) {
            std::vector <ColumnId> ids;
            return Reset(cols, ids, key_columns);
        }

        // Reset this Schema object to the given schema.
        // If this fails, the Schema object is left in an inconsistent
        // state and may not be used.
        Status Reset(const std::vector<ColumnSchema>& cols,
            const std::vector<ColumnId>& ids,
            int key_columns);


        Schema& operator=(const Schema& other);
        // Return the number of columns in this schema
        size_t num_columns() const {
            return cols_.size();
        }

        // Return the length of the key prefix in this schema.
        size_t num_key_columns() const {
            return num_key_columns_;
        }

        // Number of hash key columns.
        size_t num_hash_key_columns() const {
            return num_hash_key_columns_;
        }

        // Number of range key columns.
        size_t num_range_key_columns() const {
            return num_key_columns_ - num_hash_key_columns_;
        }

        // Return the ColumnSchema corresponding to the given column index.
        inline const ColumnSchema &column(size_t idx) const {
            assert(idx <cols_.size());
            return cols_[idx];
        }

        // Return the column ID corresponding to the given column index
        ColumnId column_id(size_t idx) const {
            assert(has_column_ids());
            assert(idx < cols_.size());
            return col_ids_[idx];
        }

        // Return true if the schema contains an ID mapping for its columns.
        // In the case of an empty schema, this is false.
        bool has_column_ids() const {
            return !col_ids_.empty();
        }

        const std::vector<ColumnSchema>& columns() const {
            return cols_;
        }

        const std::vector<ColumnId>& column_ids() const {
            return col_ids_;
        }

        const std::vector<std::string> column_names() const {
            std::vector<std::string> column_names;
            for (const auto& col : cols_) {
                column_names.push_back(col.name());
            }
            return column_names;
        }

        // Return the column index corresponding to the given column,
        // or kColumnNotFound if the column is not in this schema.
        int find_column(const std::string col_name) const {
            auto iter = name_to_index_.find(col_name);
            if (iter == name_to_index_.end()) {
                return kColumnNotFound;
            } else {
                return (*iter).second;
            }
        }

        ColumnId ColumnIdByName(const std::string& name) const;
        
        std::pair<bool, ColumnId> FindColumnIdByName(const std::string& col_name) const;

        // Returns true if the schema contains nullable columns
        bool has_nullables() const {
            return has_nullables_;
        }

        // Returns true if the specified column (by index) is a key
        bool is_key_column(size_t idx) const {
            return idx < num_key_columns_;
        }

        // Returns true if the specified column (by column id) is a key
        bool is_key_column(ColumnId column_id) const {
            return is_key_column(find_column_by_id(column_id));
        }

        // Returns true if the specified column (by name) is a key
        bool is_key_column(const std::string col_name) const {
            return is_key_column(find_column(col_name));
        }

        // Returns true if the specified column (by index) is a hash key
        bool is_hash_key_column(size_t idx) const {
            return idx < num_hash_key_columns_;
        }

        // Returns true if the specified column (by column id) is a hash key
        bool is_hash_key_column(ColumnId column_id) const {
            return is_hash_key_column(find_column_by_id(column_id));
        }

        // Returns true if the specified column (by name) is a hash key
        bool is_hash_key_column(const std::string col_name) const {
            return is_hash_key_column(find_column(col_name));
        }

        // Returns true if the specified column (by index) is a range column
        bool is_range_column(size_t idx) const {
            return is_key_column(idx) && !is_hash_key_column(idx);
        }

        // Returns true if the specified column (by column id) is a range column
        bool is_range_column(ColumnId column_id) const {
            return is_range_column(find_column_by_id(column_id));
        }

        // Returns true if the specified column (by name) is a range column
        bool is_range_column(const std::string col_name) const {
            return is_range_column(find_column(col_name));
        }

        // Return true if this Schema is initialized and valid.
        bool initialized() const {
            return !col_offsets_.empty();
        }

        // Returns the highest column id in this Schema.
        ColumnId max_col_id() const {
            return max_col_id_;
        }

        // Stringify this Schema. This is not particularly efficient,
        // so should only be used when necessary for output.
        std::string ToString() const;

        // Return true if the schemas have exactly the same set of columns
        // and respective types, and the same table properties.
        bool Equals(const Schema &other) const {
            if (this == &other) return true;
            if (this->num_key_columns_ != other.num_key_columns_) return false;
            if (this->cols_.size() != other.cols_.size()) return false;

            for (size_t i = 0; i < other.cols_.size(); i++) {
                if (!this->cols_[i].Equals(other.cols_[i])) return false;
            }

            return true;
        }

        // Returns the column index given the column ID.
        // If no such column exists, returns kColumnNotFound.
        int find_column_by_id(ColumnId id) const {
            assert(cols_.empty() || has_column_ids());
            auto iter = id_to_index_.find(id);
            if (iter == id_to_index_.end()) {
               return kColumnNotFound;
            }
            return (*iter).second;
        }

        static ColumnId first_column_id();

        uint32_t version() const {
            return version_;
        }

        void set_version(uint32_t version) {
            version_ = version;
        }

        private:
        std::vector<ColumnSchema> cols_;
        size_t num_key_columns_;
        size_t num_hash_key_columns_;
        ColumnId max_col_id_;
        std::vector<ColumnId> col_ids_;
        std::vector<size_t> col_offsets_;

        std::unordered_map<std::string, size_t> name_to_index_;
        std::unordered_map<int, int> id_to_index_;

        // Cached indicator whether any columns are nullable.
        bool has_nullables_;

        // initialie schema version to zero
        uint32_t version_ = 0;
    };

    struct IndexColumn {
        ColumnId column_id;             // Column id in the index table.
        std::string column_name;        // Column name in the index table - colexpr.MangledName().
        DataType type;                  // data type
        bool is_nullable;               // can be null or not
        bool is_hash;                   // is hash key
        bool is_range;                  // is range key
        int32_t order;                  // attr_num
        ColumnSchema::SortingType sorting_type;       // sort type
        ColumnId base_column_id;      // Corresponding column id in base table.
        std::shared_ptr<PgExpr> colexpr = nullptr;    // Index expression.

        explicit IndexColumn(ColumnId in_column_id, std::string in_column_name,
                DataType in_type, bool in_is_nullable, bool in_is_hash, bool in_is_range,
                int32_t in_order, ColumnSchema::SortingType in_sorting_type,
                ColumnId in_base_column_id, std::shared_ptr<PgExpr> in_colexpr)
                : column_id(in_column_id), column_name(std::move(in_column_name)),
                    type(in_type), is_nullable(in_is_nullable), is_hash(in_is_hash),
                    is_range(in_is_range), order(in_order), sorting_type(in_sorting_type),
                    base_column_id(in_base_column_id), colexpr(in_colexpr) {
        }

        explicit IndexColumn(ColumnId in_column_id, std::string in_column_name,
                DataType in_type, bool in_is_nullable, bool in_is_hash, bool in_is_range,
                int32_t in_order, ColumnSchema::SortingType in_sorting_type,
                ColumnId in_base_column_id)
                : column_id(in_column_id), column_name(std::move(in_column_name)),
                    type(in_type), is_nullable(in_is_nullable), is_hash(in_is_hash),
                    is_range(in_is_range), order(in_order), sorting_type(in_sorting_type),
                    base_column_id(in_base_column_id) {
        }
    };

    class IndexInfo {
        public:
        explicit IndexInfo(std::string table_name, uint32_t table_oid, std::string table_uuid,
                std::string base_table_id, uint32_t schema_version, bool is_unique,
                bool is_shared, std::vector<IndexColumn> columns, size_t hash_column_count,
                size_t range_column_count, std::vector<ColumnId> indexed_hash_column_ids,
                std::vector<ColumnId> indexed_range_column_ids, IndexPermissions index_permissions,
                bool use_mangled_column_name)
                : table_name_(table_name),
                table_oid_(table_oid),
                table_id_(PgObjectId::GetTableId(table_oid)),
                table_uuid_(table_uuid),
                base_table_id_(base_table_id),
                schema_version_(schema_version),
                is_unique_(is_unique),
                is_shared_(is_shared),
                columns_(std::move(columns)),
                hash_column_count_(hash_column_count),
                range_column_count_(range_column_count),
                indexed_hash_column_ids_(std::move(indexed_hash_column_ids)),
                indexed_range_column_ids_(std::move(indexed_range_column_ids)),
                index_permissions_(index_permissions) {
        }

        explicit IndexInfo(std::string table_name,
                uint32_t table_oid,
                std::string table_uuid,
                std::string base_table_id,
                uint32_t schema_version,
                bool is_unique,
                bool is_shared,
                std::vector<IndexColumn> columns,
                IndexPermissions index_permissions)
                : table_name_(table_name),
                    table_oid_(table_oid),
                    table_id_(PgObjectId::GetTableId(table_oid)),
                    table_uuid_(table_uuid),
                    base_table_id_(base_table_id),
                    schema_version_(schema_version),
                    is_unique_(is_unique),
                    is_shared_(is_shared),
                    columns_(std::move(columns)),
                    index_permissions_(index_permissions) {
                    for (auto& column : columns_) {
                        if (column.is_hash) {
                            hash_column_count_++;
                        } else if (column.is_range) {
                            range_column_count_++;
                        }
                    }
        }

        const std::string& table_id() const {
            return table_id_;
        }

        const std::string& table_name() const {
            return table_name_;
        }

        const uint32_t table_oid() const {
            return table_oid_;
        }

        const std::string& table_uuid() const {
            return table_uuid_;
        }

        const std::string& base_table_id() const {
            return base_table_id_;
        }

        const uint32_t base_table_oid() const {
            return PgObjectId::GetTableOidByTableUuid(base_table_id_);
        }

        bool is_unique() const {
            return is_unique_;
        }

        bool is_shared() const {
            return is_shared_;
        }

        const uint32_t version() const {
            return schema_version_;
        }

        const std::vector<IndexColumn>& columns() const {
            return columns_;
        }

        const IndexColumn& column(const size_t idx) const {
            return columns_[idx];
        }

        size_t num_columns() const {
            return columns_.size();
        }

        size_t hash_column_count() const {
            return hash_column_count_;
        }

        size_t range_column_count() const {
            return range_column_count_;
        }

        size_t key_column_count() const {
            return hash_column_count_ + range_column_count_;
        }

        const std::vector<ColumnId>& indexed_hash_column_ids() const {
            return indexed_hash_column_ids_;
        }

        const std::vector<ColumnId>& indexed_range_column_ids() const {
            return indexed_range_column_ids_;
        }

        const IndexPermissions index_permissions() const {
            return index_permissions_;
        }

        // Return column ids that are primary key columns of the base table.
        std::vector<ColumnId> index_key_column_ids() const;

        // Check if this index is dependent on the given column.
        bool CheckColumnDependency(ColumnId column_id) const;

        // Index primary key columns of the base table only?
        bool PrimaryKeyColumnsOnly(const Schema& indexed_schema) const;

        // Are read operations allowed to use the index?  During CREATE INDEX, reads are not allowed until
        // the index backfill is successfully completed.
        bool HasReadPermission() const {
            return index_permissions_ == INDEX_PERM_READ_WRITE_AND_DELETE;
        }

        // Should write operations to the index update the index table?  This includes INSERT and UPDATE.
        bool HasWritePermission() const {
            return index_permissions_ >= INDEX_PERM_WRITE_AND_DELETE &&
                    index_permissions_ <= INDEX_PERM_WRITE_AND_DELETE_WHILE_REMOVING;
        }

        // Should delete operations to the index update the index table?  This includes DELETE and UPDATE.
        bool HasDeletePermission() const {
            return index_permissions_ >= INDEX_PERM_DELETE_ONLY &&
                    index_permissions_ <= INDEX_PERM_DELETE_ONLY_WHILE_REMOVING;
        }

        // Is the index being backfilled?
        bool IsBackfilling() const {
            return index_permissions_ == INDEX_PERM_DO_BACKFILL;
        }

        int32_t FindKeyIndex(const std::string& key_name) const;

        private:
        const std::string table_name_;      // Index table name.
        const uint32_t table_oid_;
        const std::string table_id_;            // Index table id.
        const std::string table_uuid_;
        const std::string base_table_id_;    // Base table id.
        const uint32_t schema_version_ = 0; // Index table's schema version.
        const bool is_unique_ = false;      // Whether this is a unique index.
        const bool is_shared_ = false;      // whether this is a shared index
        const std::vector<IndexColumn> columns_; // Index columns.
        size_t hash_column_count_ = 0;     // Number of hash columns in the index.
        size_t range_column_count_ = 0;    // Number of range columns in the index.
        const std::vector<ColumnId> indexed_hash_column_ids_;  // Hash column ids in the base table.
        const std::vector<ColumnId> indexed_range_column_ids_; // Range column ids in the base table.
        const IndexPermissions index_permissions_ = INDEX_PERM_READ_WRITE_AND_DELETE;
    };

    class IndexMap : public std::unordered_map<std::string, IndexInfo> {
        public:
        IndexMap() {}

        const IndexInfo* FindIndex(const std::string& index_id) const;
    };

    class TableInfo {
        public:

        typedef std::shared_ptr<TableInfo> SharedPtr;

        TableInfo(std::string database_id, std::string database_name, uint32_t table_oid, std::string table_name, std::string table_uuid, Schema schema) :
            database_id_(database_id), database_name_(database_name), table_oid_(table_oid), table_id_(PgObjectId::GetTableId(table_oid)), table_name_(table_name),
            table_uuid_(table_uuid), schema_(std::move(schema)) {
        }

        const std::string& database_id() const {
            return database_id_;
        }

        const std::string& database_name() const {
            return database_name_;
        }

        const std::string& table_id() const {
            return table_id_;
        }

        const std::string& table_name() const {
            return table_name_;
        }

        void set_table_oid(uint32_t table_oid) {
            table_oid_ = table_oid;
        }

        uint32_t table_oid() {
            return table_oid_;
        }

        const std::string table_uuid() {
            return table_uuid_;
        }

        void set_next_column_id(int32_t next_column_id) {
            next_column_id_ = next_column_id;
        }

        int32_t next_column_id() {
            return next_column_id_;
        }

        const Schema& schema() const {
            return schema_;
        }

        const bool has_secondary_indexes() {
            return !index_map_.empty();
        }

         // Return the number of columns in this table
        size_t num_columns() const {
            return schema_.num_columns();
        }

        // Return the length of the key prefix in this table.
        size_t num_key_columns() const {
            return schema_.num_key_columns();
        }

        // Number of hash key columns.
        size_t num_hash_key_columns() const {
            return schema_.num_hash_key_columns();
        }

        // Number of range key columns.
        size_t num_range_key_columns() const {
            return schema_.num_range_key_columns();
        }

        void add_secondary_index(const std::string& index_id, const IndexInfo& index_info) {
            index_map_.emplace(index_id, index_info);
        }

        const IndexMap& secondary_indexes() {
            return index_map_;
        }

        void drop_index(const std::string& index_id) {
            index_map_.erase(index_id);
        }

       const IndexInfo* FindIndex(const std::string& index_id) const;

        void set_is_sys_table(bool is_sys_table) {
            is_sys_table_ = is_sys_table;
        }

        bool is_sys_table() {
            return is_sys_table_;
        }

        void set_is_shared_table(bool is_shared_table) {
            is_shared_table_ = is_shared_table;
        }

        bool is_shared() {
            return is_shared_table_;
        }

        static std::shared_ptr<TableInfo> Clone(std::shared_ptr<TableInfo> table_info, std::string database_id,
            std::string database_name, std::string table_uuid, std::string table_name);

        private:
        std::string database_id_;
        std::string database_name_; // Can be empty, that means the database has not been set yet.
        // PG internal object id
        uint32_t table_oid_;
        std::string table_id_;
        std::string table_name_;
        // cache key and it is unique cross databases
        std::string table_uuid_;
        Schema schema_;
        IndexMap index_map_;
        int32_t next_column_id_ = 0;
        bool is_sys_table_ = false;
        bool is_shared_table_ = false;
    };

}  // namespace k2pg
