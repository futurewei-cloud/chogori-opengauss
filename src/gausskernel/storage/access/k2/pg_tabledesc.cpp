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

#include "access/k2/catalog/k2_catalog_defaults.h"
#include "access/k2/pg_tabledesc.h"

#include "catalog/pg_type.h"
#include "utils/elog.h"


namespace k2pg {

using k2pg::catalog::CatalogConsts;

    void PgColumn::Init(PgSystemAttrNum attr_num)
    {
      switch (attr_num)
      {
        case PgSystemAttrNum::kSelfItemPointer:
        case PgSystemAttrNum::kObjectId:
        case PgSystemAttrNum::kMinTransactionId:
        case PgSystemAttrNum::kMinCommandId:
        case PgSystemAttrNum::kMaxTransactionId:
        case PgSystemAttrNum::kMaxCommandId:
        case PgSystemAttrNum::kTableOid:
        case PgSystemAttrNum::kPgRowId:
        case PgSystemAttrNum::kPgIdxBaseTupleId:
        case PgSystemAttrNum::kPgUniqueIdxKeySuffix:
            break;

        case PgSystemAttrNum::kPgTupleId:
        {
            int idx = static_cast<int>(PgSystemAttrNum::kPgTupleId);
            desc_.Init(idx,
                    idx,
                    "k2pgctid",
                    BYTEAOID,
                    -1,
                    false,
                    false,
                    idx,
                    ColumnSchema::SortingType::kNotSpecified);
            return;
        }
      }
      elog(ERROR, "Invalid attribute number for hidden column");
    }

    bool PgColumn::is_virtual_column()
    {
        // Currently only k2pgctid is a virtual column.
        return attr_num() == static_cast<int>(PgSystemAttrNum::kPgTupleId);
    }

    PgTableDesc::PgTableDesc(std::shared_ptr<TableInfo> pg_table) : is_index_(false),
        database_id_(pg_table->database_id()), table_id_(pg_table->table_id()), base_table_oid_(pg_table->table_oid()), index_oid_(0),
        schema_version_(pg_table->schema().version()),
        key_column_num_(pg_table->schema().num_key_columns())
    {
        // create PgTableDesc from a table
        const auto& schema = pg_table->schema();
        size_t num_columns = schema.num_columns();
        columns_.resize(num_columns);
        for (size_t idx = 0; idx < num_columns; idx++) {
            // Find the column descriptor.
            const auto& col = schema.column(idx);

            // create map by attr_num instead of the default id
            ColumnDesc *desc = columns_[idx].desc();
            desc->Init(idx,
                    schema.column_id(idx),
                    col.name(),
                    col.type_oid(),
                    col.attr_size(),
                    col.is_attr_byvalue(),
                    idx < schema.num_key_columns(),
                    col.order() /* attr_num */,
                    col.sorting_type());
            attr_num_map_[col.order()] = idx;
        }

        // Create virtual columns.
        column_k2pgctid_.Init(PgSystemAttrNum::kPgTupleId);
        collection_name_ = CatalogConsts::physical_collection(database_id_, pg_table->is_shared());
    }

    PgTableDesc::PgTableDesc(const IndexInfo& index_info, const std::string& database_id) : is_index_(true),
        database_id_(database_id), table_id_(index_info.table_id()), base_table_oid_(index_info.base_table_oid()), index_oid_(index_info.table_oid()),
        schema_version_(index_info.version()),
        key_column_num_(index_info.key_column_count())
    {
        // create PgTableDesc from an index
        size_t num_columns = index_info.num_columns();
        columns_.resize(num_columns);
        for (size_t idx = 0; idx < num_columns; idx++) {
            // Find the column descriptor.
            const auto& col = index_info.column(idx);

            // create map by attr_num instead of the default id
            ColumnDesc *desc = columns_[idx].desc();
            desc->Init(idx,
                    col.column_id,
                    col.column_name,
                    col.type_oid,
                    col.attr_size,
                    col.attr_byvalue,
                    col.is_range,
                    col.order /* attr_num */,
                    col.sorting_type);
            attr_num_map_[col.order] = idx;
        }

        // Create virtual columns.
        column_k2pgctid_.Init(PgSystemAttrNum::kPgTupleId);
        collection_name_ = CatalogConsts::physical_collection(database_id_, index_info.is_shared());
    }

    PgColumn * PgTableDesc::FindColumn(int attr_num)
    {
        // Find virtual columns.
        if (attr_num == static_cast<int>(PgSystemAttrNum::kPgTupleId)) {
            return &column_k2pgctid_;
        }

        // Find physical column.
        const auto itr = attr_num_map_.find(attr_num);
        if (itr != attr_num_map_.end()) {
            return &columns_[itr->second];
        }

        // Code calling FindColumn may not know if the table has a RowId, so that is not an error,
        // for other attributes not finding it is an error..
        if (attr_num != (int)PgSystemAttrNum::kPgRowId) {
            elog(ERROR, "Invalid column number %d", attr_num);
        }
        return NULL;
    }

    Status PgTableDesc::GetColumnInfo(int16_t attr_number, bool *is_primary) const
    {
        const auto itr = attr_num_map_.find(attr_number);
        if (itr != attr_num_map_.end()) {
            const ColumnDesc* desc = columns_[itr->second].desc();
            *is_primary = desc->is_primary();
        } else {
            *is_primary = false;
        }
        return Status::OK;
    }

}  // namespace k2pg
