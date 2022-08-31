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

#include "access/k2/pg_ids.h"
#include "access/k2/pg_schema.h"

namespace k2pg {

// Structure to hold the values of hidden columns when passing tuple from K2 SQL to PG.
struct PgSysColumns {
    // Postgres system columns.
    uint32_t oid;
    uint32_t tableoid;
    uint32_t xmin;
    uint32_t cmin;
    uint32_t xmax;
    uint32_t cmax;
    uint64_t ctid;

    // K2 Sql system columns.
    uint8_t *k2pgctid;
    uint8_t *k2pgbasectid;
};

// In addition to regular columns, K2PG support for postgres also have virtual columns.
// Virtual columns are just expression that is evaluated by K2 PG.
enum class PgSystemAttrNum : int {
    // Postgres system columns.
    kSelfItemPointer      = -1, // ctid.
    kObjectId             = -2, // oid.
    kMinTransactionId     = -3, // xmin
    kMinCommandId         = -4, // cmin
    kMaxTransactionId     = -5, // xmax
    kMaxCommandId         = -6, // cmax
    kTableOid             = -7, // tableoid

    // K2PG system columns.
    kPgTupleId            = -8, // k2pgctid: virtual column representing K2 record key.
                                // K2 PG analogue of Postgres's SelfItemPointer/ctid column.

    // The following attribute numbers are stored persistently in the table schema. For this reason,
    // they are chosen to avoid potential conflict with Postgres' own sys attributes now and future.
    kPgRowId              = -100, // k2pgrowid: auto-generated key-column for tables without pkey.
    kPgIdxBaseTupleId     = -101, // k2pgidxbasectid: for indexes, the k2pgctid of base table row.
    kPgUniqueIdxKeySuffix = -102, // k2pguniqueidxkeysuffix: extra key column for unique indexes, used
                                  // to ensure SQL semantics for null (null != null) in K2 PG
                                  // (where null == null). For each index row will be set to:
                                  //  - the base table ctid when one or more indexed cols are null
                                  //  - to null otherwise (all indexed cols are non-null).
};

// This class can be used to describe any reference of a column.
class ColumnDesc {
 public:
  typedef std::shared_ptr<ColumnDesc> SharedPtr;

  ColumnDesc() : sql_type_(SQLType::Create(DataType::K2SQL_DATA_TYPE_UNKNOWN_DATA)) {
  }

  void Init(int index,
            int id,
            std::string name,
            bool is_hash,
            bool is_primary,
            int32_t attr_num,
            const std::shared_ptr<SQLType>& sql_type,
            ColumnSchema::SortingType sorting_type) {
    index_ = index,
    id_ = id;
    name_ = name;
    is_hash_ = is_hash;
    is_primary_ = is_primary;
    attr_num_ = attr_num;
    sql_type_ = sql_type;
    sorting_type_ = sorting_type;
  }

  bool IsInitialized() const {
    return (index_ >= 0);
  }

  int index() const {
    return index_;
  }

  int id() const {
    return id_;
  }

  const std::string& name() const {
    return name_;
  }

  bool is_hash() const {
    return is_hash_;
  }

  bool is_primary() const {
    return is_primary_;
  }

  int32_t attr_num() const {
    return attr_num_;
  }

  std::shared_ptr<SQLType> sql_type() const {
    return sql_type_;
  }

  ColumnSchema::SortingType sorting_type() const {
    return sorting_type_;
  }

 private:
  int index_ = -1;
  int id_ = -1;
  std::string name_;
  bool is_hash_ = false;
  bool is_primary_ = false;
  int32_t attr_num_ = -1;
  std::shared_ptr<SQLType> sql_type_;
  ColumnSchema::SortingType sorting_type_ = ColumnSchema::SortingType::kNotSpecified;
};

class PgColumn {
 public:
  // Constructor & Destructor.
  PgColumn() {
  }

  virtual ~PgColumn() {
  }

  // Initialize hidden columns.
  void Init(PgSystemAttrNum attr_num);

  ColumnDesc *desc() {
    return &desc_;
  }

  const ColumnDesc *desc() const {
    return &desc_;
  }

  const std::string& attr_name() const {
    return desc_.name();
  }

  bool is_primary() const {
    return desc_.is_primary();
  }

  int32_t attr_num() const {
    return desc_.attr_num();
  }

  int index() const {
    return desc_.index();
  }

  int id() const {
    return desc_.id();
  }

  bool is_system_column() {
    return attr_num() < 0;
  }

  bool is_virtual_column();

  private:
  ColumnDesc desc_;
};

// Desc of a table or an index.
// This class can be used to describe any reference of a column.
class PgTableDesc {
 public:
  explicit PgTableDesc(std::shared_ptr<TableInfo> pg_table);

  explicit PgTableDesc(const IndexInfo& index_info, const std::string& database_id);

  const std::string& database_id() const {
    return database_id_;
  }

  const std::string& collection_name() const {
    return collection_name_;
  }

  // if is_index_, this is id of the index, otherwise, it is id of this table.
  const std::string& table_id() {
    return table_id_;
  }

  // if is_index_, this is oid of the base table, otherwise, it is oid of this table.
  const PgOid base_table_oid() {
    return base_table_oid_;
  }

  // if is_index_, this is oid of the index, otherwiese 0
  const PgOid index_oid() {
    return index_oid_;
  }

  static int ToPgAttrNum(const std::string &attr_name, int attr_num);

  std::vector<PgColumn>& columns() {
    return columns_;
  }

  const size_t num_hash_key_columns() const {
    return hash_column_num_;
  }

  const size_t num_key_columns() const {
    return key_column_num_;
  }

  const size_t num_columns() const {
    return columns_.size();
  }

  // Find the column given the postgres attr number.
  PgColumn * FindColumn(int attr_num);

  Status GetColumnInfo(int16_t attr_number, bool *is_primary, bool *is_hash) const;

  int GetPartitionCount() const {
    // TODO:  Assume 1 partition for now until we add logic to expose k2 storage partition counts
    return 1;
  }

  uint32_t SchemaVersion() const {
    return schema_version_;
  };

  bool is_index() {
    return is_index_;
  }

  private:
  bool is_index_;
  std::string database_id_;
  std::string table_id_;  // if is_index_, this is id of the index, otherwise, it is id of this table.
  PgOid base_table_oid_;  // if is_index_, this is oid of the base table, otherwise, it is oid of this table.
  PgOid index_oid_;       // if is_index_, this is oid of the index, otherwiese 0
  uint32_t schema_version_;
  size_t hash_column_num_;
  size_t key_column_num_;
  std::vector<PgColumn> columns_;
  std::unordered_map<int, size_t> attr_num_map_; // Attr number to column index map.

  // Hidden columns.
  PgColumn column_k2pgctid_;

  // k2 collection name
  std::string collection_name_;
};

}  // namespace k2pg