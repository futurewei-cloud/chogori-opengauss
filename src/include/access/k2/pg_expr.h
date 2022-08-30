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

namespace k2pg {

struct Data {
    union {
        bool bool_val_;
        int64_t int_val_;
        float float_val_;
        double double_val_;
    };
    std::string slice_val_;
};

class SqlValue {
public:
  enum ValueType {
      BOOL,
      INT,
      FLOAT,
      DOUBLE,
      SLICE,
      UNKNOWN
  };

  SqlValue(bool b) {
      type_ = ValueType::BOOL;
      data_.bool_val_ = b;
      null_value_ = false;
  }

  SqlValue(int64_t v) {
      type_ = ValueType::INT;
      data_.int_val_ = v;
      null_value_ = false;
  }

  SqlValue(float f) {
      type_ = ValueType::FLOAT;
      data_.float_val_ = f;
      null_value_ = false;
  }

  SqlValue(double d) {
      type_ = ValueType::DOUBLE;
      data_.double_val_ = d;
      null_value_ = false;
  }

  SqlValue(k2pg::Slice s) {
      type_ = ValueType::SLICE;
      data_.slice_val_ = std::string(s.cdata(), s.size());
      null_value_ = false;
  }

  SqlValue(std::string s) {
      type_ = ValueType::SLICE;
      data_.slice_val_ = std::move(s);
      null_value_ = false;
  }

  SqlValue(const K2PgTypeEntity* type_entity, uint64_t datum, bool is_null);

  SqlValue(const SqlValue& val) = default;

  bool IsBoolean() {
      return type_ == ValueType::BOOL;
  }

  bool IsInteger() {
      return type_ == ValueType::INT;
  }

  bool IsMaxInteger() {
      if (!IsInteger()) {
          return false;
      }

      // null values are not handled here since SQL has its own way to handle nulls
      assert(!IsNull());
      return data_.int_val_ == std::numeric_limits<int64_t>::max();
  }

  // get a value that is higher than the current one
  SqlValue UpperBound() {
    // null values are not handled here since SQL has its own way to handle nulls
    assert(!IsNull());
    switch (type_) {
        case ValueType::INT: {
            return SqlValue(data_.int_val_ + 1);
        } break;
        default: {
            throw std::invalid_argument("Unsupported data type: " + type_);
        } break;
    }

    throw std::invalid_argument("Unsupported data type: " + type_);
  }

  int Compare(const SqlValue& val) {
    // null values are not considered here since their comparison is based on column sorting type
    assert((!IsNull()) && (!val.IsNull()));
    // types must be the same for comparison
    assert(type_ == val.type_);

    switch (type_) {
        case ValueType::BOOL: {
            if (data_.bool_val_ == val.data_.bool_val_) {
                return 0;
            } else if (data_.bool_val_ < val.data_.bool_val_) {
                return -1;
            } else {
                return 1;
            }
        } break;
        case ValueType::INT: {
            if (data_.int_val_ == val.data_.int_val_) {
                return 0;
            } else if (data_.int_val_ < val.data_.int_val_) {
                return -1;
            } else {
                return 1;
            }
        } break;
        case ValueType::FLOAT: {
            if (data_.float_val_ == val.data_.float_val_) {
                return 0;
            } else if (data_.float_val_ < val.data_.float_val_) {
                return -1;
            } else {
                return 1;
            }
        } break;
        case ValueType::DOUBLE: {
            if (data_.double_val_ == val.data_.double_val_) {
                return 0;
            } else if (data_.double_val_ < val.data_.double_val_) {
                return -1;
            } else {
                return 1;
            }
        } break;
        case ValueType::SLICE: {
            return data_.slice_val_.compare(val.data_.slice_val_);
        } break;
        default:
            throw std::invalid_argument("Unknown data type");
        break;
    }

    throw std::invalid_argument("Unknown data type");
  }

  friend std::ostream& operator<<(std::ostream& os, const SqlValue& sql_value) {
    os << "{type: " << sql_value.type_ << ", isNull: " << sql_value.null_value_ << ", value: ";
    if (sql_value.null_value_) {
        os << "NULL";
    } else {
        switch (sql_value.type_) {
            case ValueType::BOOL: {
                os << sql_value.data_.bool_val_;
            } break;
            case ValueType::INT: {
                os << sql_value.data_.int_val_;
            } break;
            case ValueType::FLOAT: {
                os << sql_value.data_.float_val_;
            } break;
            case ValueType::DOUBLE: {
                os << sql_value.data_.double_val_;
            } break;
            case ValueType::SLICE: {
                os << k2::HexCodec::encode(sql_value.data_.slice_val_);
            } break;
            default: {
                os << "Unknown";
            } break;
        }
    }
    os << "}";
    return os;
  }

  bool IsNull() const {
      return null_value_;
  }

  bool isBinaryValue() const {
      return type_ == ValueType::SLICE;
  }

  void set_bool_value(bool value, bool is_null);
  void set_int8_value(int8_t value, bool is_null);
  void set_int16_value(int16_t value, bool is_null);
  void set_int32_value(int32_t value, bool is_null);
  void set_int64_value(int64_t value, bool is_null);
  void set_float_value(float value, bool is_null);
  void set_double_value(double value, bool is_null);
  void set_string_value(const char *value, bool is_null);
  void set_binary_value(const char *value, size_t bytes, bool is_null);

  ValueType type_ = ValueType::UNKNOWN;
  Data data_;

  private:
  void Clear();

  bool null_value_ = true;
};

class PgExpr {
    public:
    enum class Opcode {
        PG_EXPR_CONSTANT,
        PG_EXPR_COLREF,

        // The logical expression for defining the conditions when we support WHERE clause.
        PG_EXPR_NOT,
        PG_EXPR_EQ,
        PG_EXPR_NE,
        PG_EXPR_GE,
        PG_EXPR_GT,
        PG_EXPR_LE,
        PG_EXPR_LT,

        // exists
        PG_EXPR_EXISTS,

        // Logic operators that take two or more operands.
        PG_EXPR_AND,
        PG_EXPR_OR,
        PG_EXPR_IN,
        PG_EXPR_BETWEEN,

        // Aggregate functions.
        PG_EXPR_AVG,
        PG_EXPR_SUM,
        PG_EXPR_COUNT,
        PG_EXPR_MAX,
        PG_EXPR_MIN,

        // built-in functions
        PG_EXPR_EVAL_EXPR_CALL,
    };

    friend std::ostream& operator<<(std::ostream& os, const Opcode& opcode) {
        switch(opcode) {
            case Opcode::PG_EXPR_CONSTANT: return os << "PG_EXPR_CONSTANT";
            case Opcode::PG_EXPR_COLREF: return os << "PG_EXPR_COLREF";
            case Opcode::PG_EXPR_NOT: return os << "PG_EXPR_NOT";
            case Opcode::PG_EXPR_EQ: return os << "PG_EXPR_EQ";
            case Opcode::PG_EXPR_NE: return os << "PG_EXPR_NE";
            case Opcode::PG_EXPR_GE: return os << "PG_EXPR_GE";
            case Opcode::PG_EXPR_GT: return os << "PG_EXPR_GT";
            case Opcode::PG_EXPR_LE: return os << "PG_EXPR_LE";
            case Opcode::PG_EXPR_LT: return os << "PG_EXPR_LT";
            case Opcode::PG_EXPR_EXISTS: return os << "PG_EXPR_EXISTS";
            case Opcode::PG_EXPR_AND: return os << "PG_EXPR_AND";
            case Opcode::PG_EXPR_OR: return os << "PG_EXPR_OR";
            case Opcode::PG_EXPR_IN: return os << "PG_EXPR_IN";
            case Opcode::PG_EXPR_BETWEEN: return os << "PG_EXPR_BETWEEN";
            case Opcode::PG_EXPR_AVG: return os << "PG_EXPR_AVG";
            case Opcode::PG_EXPR_SUM: return os << "PG_EXPR_SUM";
            case Opcode::PG_EXPR_COUNT: return os << "PG_EXPR_COUNT";
            case Opcode::PG_EXPR_MAX: return os << "PG_EXPR_MAX";
            case Opcode::PG_EXPR_MIN: return os << "PG_EXPR_MIN";
            case Opcode::PG_EXPR_EVAL_EXPR_CALL: return os << "PG_EXPR_EVAL_EXPR_CALL";
            default: return os << "UNKNOWN";
        }
    }

    typedef std::shared_ptr<PgExpr> SharedPtr;

    explicit PgExpr(Opcode opcode, const K2PgTypeEntity *type_entity);

    explicit PgExpr(Opcode opcode, const K2PgTypeEntity *type_entity, const PgTypeAttrs *type_attrs);

    explicit PgExpr(const char *opname, const K2PgTypeEntity *type_entity);

    virtual ~PgExpr();

    Opcode opcode() const {
        return opcode_;
    }

    bool is_constant() const {
        return opcode_ == Opcode::PG_EXPR_CONSTANT;
    }

    bool is_colref() const {
        return opcode_ == Opcode::PG_EXPR_COLREF;
    }

    bool is_aggregate() const {
        // Only return true for pushdown supported aggregates.
        return (opcode_ == Opcode::PG_EXPR_SUM ||
                opcode_ == Opcode::PG_EXPR_COUNT ||
                opcode_ == Opcode::PG_EXPR_MAX ||
                opcode_ == Opcode::PG_EXPR_MIN);
    }

    bool is_logic_expr() const {
        return (opcode_ == Opcode::PG_EXPR_NOT ||
                opcode_ == Opcode::PG_EXPR_EQ ||
                opcode_ == Opcode::PG_EXPR_NE ||
                opcode_ == Opcode::PG_EXPR_GE ||
                opcode_ == Opcode::PG_EXPR_GT ||
                opcode_ == Opcode::PG_EXPR_LE ||
                opcode_ == Opcode::PG_EXPR_LT);
    }

    virtual bool is_k2pgbasetid() const {
        return false;
    }

    const PgTypeEntity *type_entity() const {
        return type_entity_;
    }

    const PgTypeAttrs *type_attrs() const {
        return &type_attrs_;
    }

    // Find opcode.
    static CHECKED_STATUS CheckOperatorName(const char *name);
    static Opcode NameToOpcode(const char *name);

    friend std::ostream& operator<<(std::ostream& os, const PgExpr& expr) {
      os << "(PgExpr: Opcode: " << expr.opcode_ << ", type: " << expr.type_entity_->k2pg_type << ")";
      return os;
    }

    protected:
    Opcode opcode_;
    const PgTypeEntity *type_entity_;
    const PgTypeAttrs type_attrs_;
};

class PgConstant : public PgExpr {
 public:
  // Public types.
  typedef std::shared_ptr<PgConstant> SharedPtr;
  // Constructor.
  explicit PgConstant(const K2PgTypeEntity *type_entity, uint64_t datum, bool is_null,
      PgExpr::Opcode opcode = PgExpr::Opcode::PG_EXPR_CONSTANT);

  explicit PgConstant(const K2PgTypeEntity *type_entity, SqlValue value);

  // Destructor.
  virtual ~PgConstant();

  // Update numeric.
  void UpdateConstant(int8_t value, bool is_null);
  void UpdateConstant(int16_t value, bool is_null);
  void UpdateConstant(int32_t value, bool is_null);
  void UpdateConstant(int64_t value, bool is_null);
  void UpdateConstant(float value, bool is_null);
  void UpdateConstant(double value, bool is_null);

  // Update text.
  void UpdateConstant(const char *value, bool is_null);
  void UpdateConstant(const char *value, size_t bytes, bool is_null);

  SqlValue* getValue() {
      return &value_;
  }

  friend std::ostream& operator<<(std::ostream& os, const PgConstant& expr) {
      os << "(PgConst: " << expr.value_ << ")";
      return os;
  }

  private:
  SqlValue value_;
};

class PgColumnRef : public PgExpr {
 public:
  // Public types.
  typedef std::shared_ptr<PgColumnRef> SharedPtr;
  explicit PgColumnRef(int attr_num,
                       const PgTypeEntity *type_entity,
                       const PgTypeAttrs *type_attrs);
  virtual ~PgColumnRef();

  void set_attr_name(const std::string& name) {
    attr_name_ = name;
  }

  const std::string& attr_name() const {
      return attr_name_;
  }

  int attr_num() const {
    return attr_num_;
  }

  bool is_k2pgbasetid() const override;

  friend std::ostream& operator<<(std::ostream& os, const PgColumnRef& expr) {
      os << "(PgColumnRef: attr_name: " << expr.attr_name_ << ", attr_num: " << expr.attr_num_ << ")";
      return os;
  }

 private:
  int attr_num_;
  std::string attr_name_;
};

class PgOperator : public PgExpr {
 public:
  // Public types.
  typedef std::shared_ptr<PgOperator> SharedPtr;

  // Constructor.
  explicit PgOperator(const char *name, const K2PgTypeEntity *type_entity);
  virtual ~PgOperator();

  // Append arguments.
  void AppendArg(PgExpr *arg);

  const std::vector<PgExpr*> & getArgs() const {
      return args_;
  }

  friend std::ostream& operator<<(std::ostream& os, const PgOperator& expr) {
      os << "(PgOperator: opcode: " << expr.opcode_ << ", opname: " << expr.opname_ << ", args_num: " << expr.args_.size() << ", args:[";
      for (PgExpr* arg : expr.args_) {
        os << (*arg) << ",";
      }
      os << "])";
      return os;
  }

  private:
  const std::string opname_;
  std::vector<PgExpr*> args_;
};

}  // namespace k2pg
