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
#include <unordered_map>

#include "utils/elog.h"
#include "utils/errcodes.h"

#include "access/k2/data_type.h"
#include "access/k2/pg_param.h"
#include "access/k2/pg_expr.h"

namespace k2pg {

void SqlValue::Clear() {
    null_value_ = true;
}

SqlValue::SqlValue(const PgTypeEntity* type_entity, uint64_t datum, bool is_null) {
  null_value_ = is_null;

 switch (type_entity->k2pg_type) {
    case K2SQL_DATA_TYPE_INT8:
      type_ = ValueType::INT;
      if (!is_null) {
        int8_t value;
        type_entity->datum_to_k2pg(datum, &value, nullptr);
        data_.int_val_ = value;
      }
      break;

    case K2SQL_DATA_TYPE_INT16:
      type_ = ValueType::INT;
      if (!is_null) {
        int16_t value;
        type_entity->datum_to_k2pg(datum, &value, nullptr);
        data_.int_val_ = value;
      }
      break;

    case K2SQL_DATA_TYPE_INT32:
      type_ = ValueType::INT;
      if (!is_null) {
        int32_t value;
        type_entity->datum_to_k2pg(datum, &value, nullptr);
        data_.int_val_ = value;
      }
      break;

    case K2SQL_DATA_TYPE_INT64:
      type_ = ValueType::INT;
      if (!is_null) {
        int64_t value;
        type_entity->datum_to_k2pg(datum, &value, nullptr);
        data_.int_val_ = value;
      }
      break;

    case K2SQL_DATA_TYPE_UINT32:
      type_ = ValueType::INT;
      if (!is_null) {
        uint32_t value;
        type_entity->datum_to_k2pg(datum, &value, nullptr);
        data_.int_val_ = value;
      }
      break;

    case K2SQL_DATA_TYPE_UINT8:
      type_ = ValueType::INT;
      if (!is_null) {
        uint8_t value;
        type_entity->datum_to_k2pg(datum, &value, nullptr);
        data_.int_val_ = value;
      }
      break;

    case K2SQL_DATA_TYPE_UINT16:
      type_ = ValueType::INT;
      if (!is_null) {
        uint16_t value;
        type_entity->datum_to_k2pg(datum, &value, nullptr);
        data_.int_val_ = value;
      }
      break;

    case K2SQL_DATA_TYPE_UINT64:
      type_ = ValueType::INT;
      if (!is_null) {
        uint64_t value;
        type_entity->datum_to_k2pg(datum, &value, nullptr);
        data_.int_val_ = value;
      }
      break;

    case K2SQL_DATA_TYPE_STRING:
      type_ = ValueType::SLICE;
      if (!is_null) {
        char *value;
        int64_t bytes = type_entity->datum_fixed_size;
        type_entity->datum_to_k2pg(datum, &value, &bytes);
        data_.slice_val_ = std::string(value, bytes);
      }
      break;

    case K2SQL_DATA_TYPE_BOOL:
      type_ = ValueType::BOOL;
      if (!is_null) {
        bool value;
        type_entity->datum_to_k2pg(datum, &value, nullptr);
        data_.bool_val_ = value;
      }
      break;

    case K2SQL_DATA_TYPE_FLOAT:
      type_ = ValueType::FLOAT;
      if (!is_null) {
        float value;
        type_entity->datum_to_k2pg(datum, &value, nullptr);
        data_.float_val_ = value;
      }
      break;

    case K2SQL_DATA_TYPE_DOUBLE:
      type_ = ValueType::DOUBLE;
      if (!is_null) {
        double value;
        type_entity->datum_to_k2pg(datum, &value, nullptr);
        data_.double_val_ = value;
      }
      break;

    case K2SQL_DATA_TYPE_BINARY:
      type_ = ValueType::SLICE;
      if (!is_null) {
        uint8_t *value;
        int64_t bytes = type_entity->datum_fixed_size;
        type_entity->datum_to_k2pg(datum, &value, &bytes);
        data_.slice_val_ = std::string((char*)value, bytes);
      }
      break;

    case K2SQL_DATA_TYPE_TIMESTAMP:
      type_ = ValueType::INT;
      if (!is_null) {
        int64_t value;
        type_entity->datum_to_k2pg(datum, &value, nullptr);
        data_.int_val_ = value;
      }
      break;

    case K2SQL_DATA_TYPE_DECIMAL:
      type_ = ValueType::SLICE;
      if (!is_null) {
        char* plaintext;
        type_entity->datum_to_k2pg(datum, &plaintext, nullptr);
        // TODO: add decimal type handling
  //      k2pg::Decimal k2pg_decimal(plaintext);
  //      data_.slice_val_ = k2pg_decimal.EncodeToComparable();
      }
      break;

    case K2SQL_DATA_TYPE_LIST:
    case K2SQL_DATA_TYPE_MAP:
    case K2SQL_DATA_TYPE_SET:
    case K2SQL_DATA_TYPE_DATE: // Not used for PG storage
    case K2SQL_DATA_TYPE_TIME: // Not used for PG storage
    default:
      elog(FATAL, "Internal error: unsupported type %d", type_entity->k2pg_type);
  }
}

void SqlValue::set_bool_value(bool value, bool is_null) {
    type_ = ValueType::BOOL;
    if(is_null) {
        Clear();
    } else {
        data_.bool_val_ = value;
        null_value_ = false;
    }
}

void SqlValue::set_int8_value(int8_t value, bool is_null) {
    type_ = ValueType::INT;
    if(is_null) {
        Clear();
    } else {
        data_.int_val_ = value;
        null_value_ = false;
    }
}

void SqlValue::set_int16_value(int16_t value, bool is_null) {
    type_ = ValueType::INT;
    if(is_null) {
        Clear();
    } else {
        data_.int_val_ = value;
        null_value_ = false;
    }
}

void SqlValue::set_int32_value(int32_t value, bool is_null) {
    type_ = ValueType::INT;
    if(is_null) {
        Clear();
    } else {
        data_.int_val_ = value;
        null_value_ = false;
    }
}

void SqlValue::set_int64_value(int64_t value, bool is_null) {
    type_ = ValueType::INT;
    if(is_null) {
        Clear();
    } else {
        data_.int_val_ = value;
        null_value_ = false;
    }
}

void SqlValue::set_float_value(float value, bool is_null) {
    type_ = ValueType::FLOAT;
    if(is_null) {
        Clear();
    } else {
        data_.float_val_ = value;
        null_value_ = false;
    }
}

void SqlValue::set_double_value(double value, bool is_null) {
    type_ = ValueType::DOUBLE;
    if(is_null) {
        Clear();
    } else {
        data_.double_val_ = value;
        null_value_ = false;
    }
}

void SqlValue::set_string_value(const char *value, bool is_null) {
    type_ = ValueType::SLICE;
    if(is_null) {
        Clear();
    } else {
        data_.slice_val_ = std::string(value);
        null_value_ = false;
    }
}

void SqlValue::set_binary_value(const char *value, size_t bytes, bool is_null) {
    type_ = ValueType::SLICE;
    if(is_null) {
        Clear();
    } else {
        data_.slice_val_ = std::string(value, bytes);
        null_value_ = false;
    }
}

//--------------------------------------------------------------------------------------------------
// Mapping Postgres operator names to K2 PG gate opcodes.
// When constructing expresions, Postgres layer will pass the operator name.
const std::unordered_map<std::string, PgExpr::Opcode> kOperatorNames = {
  { "!", PgExpr::Opcode::PG_EXPR_NOT },
  { "not", PgExpr::Opcode::PG_EXPR_NOT },
  { "=", PgExpr::Opcode::PG_EXPR_EQ },
  { "<>", PgExpr::Opcode::PG_EXPR_NE },
  { "!=", PgExpr::Opcode::PG_EXPR_NE },
  { ">", PgExpr::Opcode::PG_EXPR_GT },
  { ">=", PgExpr::Opcode::PG_EXPR_GE },
  { "<", PgExpr::Opcode::PG_EXPR_LT },
  { "<=", PgExpr::Opcode::PG_EXPR_LE },

  { "and", PgExpr::Opcode::PG_EXPR_AND },
  { "or", PgExpr::Opcode::PG_EXPR_OR },
  { "in", PgExpr::Opcode::PG_EXPR_IN },
  { "between", PgExpr::Opcode::PG_EXPR_BETWEEN },

  { "avg", PgExpr::Opcode::PG_EXPR_AVG },
  { "sum", PgExpr::Opcode::PG_EXPR_SUM },
  { "count", PgExpr::Opcode::PG_EXPR_COUNT },
  { "max", PgExpr::Opcode::PG_EXPR_MAX },
  { "min", PgExpr::Opcode::PG_EXPR_MIN },
  { "eval_expr_call", PgExpr::Opcode::PG_EXPR_EVAL_EXPR_CALL }
};

PgExpr::PgExpr(Opcode opcode, const PgTypeEntity *type_entity)
    : opcode_(opcode), type_entity_(type_entity) , type_attrs_({0}) {
  assert(type_entity_ != NULL);
  assert(type_entity_->k2pg_type != K2SQL_DATA_TYPE_NOT_SUPPORTED &&
         type_entity_->k2pg_type != K2SQL_DATA_TYPE_UNKNOWN_DATA &&
         type_entity_->k2pg_type != K2SQL_DATA_TYPE_NULL_VALUE_TYPE);
  assert(type_entity_->datum_to_k2pg != NULL);
  assert(type_entity_->k2pg_to_datum != NULL);
}

PgExpr::PgExpr(Opcode opcode, const PgTypeEntity *type_entity, const PgTypeAttrs *type_attrs)
    : opcode_(opcode), type_entity_(type_entity), type_attrs_(*type_attrs) {
  assert(type_entity_ != NULL);
  assert(type_entity_->k2pg_type != K2SQL_DATA_TYPE_NOT_SUPPORTED &&
         type_entity_->k2pg_type != K2SQL_DATA_TYPE_UNKNOWN_DATA &&
         type_entity_->k2pg_type != K2SQL_DATA_TYPE_NULL_VALUE_TYPE);
  assert(type_entity_->datum_to_k2pg != NULL);
  assert(type_entity_->k2pg_to_datum != NULL);
}

PgExpr::PgExpr(const char *opname, const PgTypeEntity *type_entity)
    : PgExpr(NameToOpcode(opname), type_entity) {
}

PgExpr::~PgExpr() {
}

Status PgExpr::CheckOperatorName(const char *name) {
  auto iter = kOperatorNames.find(name);
  if (iter == kOperatorNames.end()) {
    Status status {
      .pg_code = ERRCODE_INVALID_NAME,
      .k2_code = 404,
      .msg = "Wrong operator name",
      .detail = name
    };
    return status;
  }

  return Status::OK;
}

PgExpr::Opcode PgExpr::NameToOpcode(const char *name) {
  auto iter = kOperatorNames.find(name);
  assert(iter != kOperatorNames.end());
  return iter->second;
}

PgConstant::PgConstant(const PgTypeEntity *type_entity, uint64_t datum, bool is_null,
    PgExpr::Opcode opcode)
    : PgExpr(opcode, type_entity), value_(type_entity, datum, is_null) {
}

PgConstant::PgConstant(const PgTypeEntity *type_entity, SqlValue value) : PgExpr(PgExpr::Opcode::PG_EXPR_CONSTANT, type_entity), value_(value) {

}

PgConstant::~PgConstant() {
}

void PgConstant::UpdateConstant(int8_t value, bool is_null) {
    value_.set_int8_value(value, is_null);
}

void PgConstant::UpdateConstant(int16_t value, bool is_null) {
    value_.set_int16_value(value, is_null);
}

void PgConstant::UpdateConstant(int32_t value, bool is_null) {
    value_.set_int32_value(value, is_null);
}

void PgConstant::UpdateConstant(int64_t value, bool is_null) {
    value_.set_int64_value(value, is_null);
}

void PgConstant::UpdateConstant(float value, bool is_null) {
    value_.set_float_value(value, is_null);
}

void PgConstant::UpdateConstant(double value, bool is_null) {
    value_.set_double_value(value, is_null);
}

void PgConstant::UpdateConstant(const char *value, bool is_null) {
    value_.set_string_value(value, is_null);
}

void PgConstant::UpdateConstant(const char *value, size_t bytes, bool is_null) {
    value_.set_binary_value(value, bytes, is_null);
}

PgColumnRef::PgColumnRef(int attr_num,
                         const PgTypeEntity *type_entity,
                         const PgTypeAttrs *type_attrs)
    : PgExpr(PgExpr::Opcode::PG_EXPR_COLREF, type_entity, type_attrs), attr_num_(attr_num) {
}

PgColumnRef::~PgColumnRef() {
}

bool PgColumnRef::is_k2pgbasetid() const {
  return attr_num_ == static_cast<int>(PgSystemAttrNum::kPgIdxBaseTupleId);
}

PgOperator::PgOperator(const char *opname, const PgTypeEntity *type_entity)
  : PgExpr(opname, type_entity), opname_(opname) {
}

PgOperator::~PgOperator() {
}

void PgOperator::AppendArg(PgExpr *arg) {
  args_.push_back(arg);
}

}  // namespace k2pg
