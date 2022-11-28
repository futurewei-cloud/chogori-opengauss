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

#include <skvhttp/dto/Expression.h>
#include <skvhttp/dto/SKVRecord.h>

#include "postgres.h"
#include "access/k2/pg_gate_api.h"
#include "access/k2/k2pg_aux.h"
#include "access/k2/k2_types.h"
#include "access/sysattr.h"
#include "catalog/pg_type.h"
#include "utils/numeric.h"

#include "access/k2/k2_util.h"
#include "access/k2/storage.h"
#include "session.h"

namespace k2pg {
namespace gate {

// Helper class to manager palloc'ed objects. All managed objects are freed on destruction.
// The intent is that "release" is called after any possible error/exception path which releases
// owneship so the objects can be passed to upper PG layers
class PallocManager {
public:
    PallocManager() = default;

    ~PallocManager() {
        for (char* object : objects) {
            pfree(object);
        }
    }

    char* alloc(size_t size) {
        char* p = (char*)palloc0(size);
        objects.push_back(p);
        return p;
    }

    void release() {
        objects.clear();
    }

private:
    std::vector<char*> objects;
};

static void populateSysColumnFromSKVRecord(skv::http::dto::SKVRecord& record, int attr_num, K2PgSysColumns* syscols, PallocManager& allocManager) {
    PgSystemAttrNum attr_enum = (PgSystemAttrNum) attr_num;
    switch (attr_enum) {
        case PgSystemAttrNum::kObjectId:
        case PgSystemAttrNum::kMinTransactionId:
        case PgSystemAttrNum::kMinCommandId:
        case PgSystemAttrNum::kMaxTransactionId:
        case PgSystemAttrNum::kMaxCommandId:
        case PgSystemAttrNum::kTableOid: {
            std::optional<int64_t> value = record.deserializeNext<int64_t>();
            if (!value.has_value()) {
                throw std::runtime_error("System attribute in skvrecord was null");
                break;
            }
            switch (attr_enum) {
                case PgSystemAttrNum::kObjectId:
                    syscols->oid = (uint32_t)*value;
                    break;
                case PgSystemAttrNum::kMinTransactionId:
                    syscols->xmin = (uint32_t)*value;
                    break;
                case PgSystemAttrNum::kMinCommandId:
                    syscols->cmin = (uint32_t)*value;
                    break;
                case PgSystemAttrNum::kMaxTransactionId:
                    syscols->xmax = (uint32_t)*value;
                    break;
                case PgSystemAttrNum::kMaxCommandId:
                    syscols->cmax = (uint32_t)*value;
                    break;
                case PgSystemAttrNum::kTableOid:
                    syscols->tableoid = (uint32_t)*value;
                    break;
                default:
                    throw std::runtime_error("Unexpected system attribute id");
            }
        }
            break;
        case PgSystemAttrNum::kSelfItemPointer: {
            std::optional<int64_t> value = record.deserializeNext<int64_t>();
            if (!value.has_value()) {
                throw std::runtime_error("System attribute in skvrecord was null");
                break;
            }
            syscols->ctid = *value;
        }
            break;
        case PgSystemAttrNum::kPgIdxBaseTupleId: {
            std::optional<std::string> value = record.deserializeNext<std::string>();
            if (!value.has_value()) {
                throw std::runtime_error("System attribute in skvrecord was null");
                break;
            }

            char *datum = allocManager.alloc(value->size() + VARHDRSZ);
            SET_VARSIZE(datum, value->size() + VARHDRSZ);
            memcpy(VARDATA(datum), value->data(), value->size());
            syscols->k2pgbasectid = (uint8_t*)PointerGetDatum(datum);
        }
            break;
        case PgSystemAttrNum::kPgRowId: {
            // PG does not want to read rowid back
            std::optional<std::string> value = record.deserializeNext<std::string>();
            break;
        }
        default:
            throw std::runtime_error("Unexpected system attribute id");
    }
}

K2PgStatus populateDatumsFromSKVRecord(skv::http::dto::SKVRecord& record, std::shared_ptr<k2pg::PgTableDesc> pg_table,
                                       int nattrs, Datum* values, bool* isnulls, K2PgSysColumns* syscols) {
    // Initialize output
    for (int i=0; i < nattrs; ++i) {
        values[i] = 0;
        isnulls[i] = true;
    }

    // Setup some helper data structures
    PallocManager allocManager{};
    std::unordered_map<uint32_t, int> offset_to_attr;
    std::unordered_map<uint32_t, Oid> offset_to_oid;
    for (const auto& column : pg_table->columns()) {
        // we have two extra fields, i.e., table_id and index_id, in skv key
        offset_to_attr[column.index() + K2_FIELD_OFFSET] = column.attr_num();
        offset_to_oid[column.index() + K2_FIELD_OFFSET] = column.type_oid();
    }

    // Iterate through the SKV record's fields
    try {
    uint32_t offset = K2_FIELD_OFFSET;
    record.seekField(K2_FIELD_OFFSET);
    for (; offset < record.schema->fields.size(); ++offset) {
        // If attribute number is < 0, then it is a system column
        if (offset_to_attr[offset] < 0) {
            populateSysColumnFromSKVRecord(record, offset_to_attr[offset], syscols, allocManager);
            continue;
        }

        Oid id = offset_to_oid[offset];
        int datum_offset = offset_to_attr[offset] - 1;

        // Otherwise field is a normal user column
        if (isStringType(id)) {
            std::optional<std::string> value = record.deserializeNext<std::string>();
            if (value.has_value()) {
                char* datum = allocManager.alloc(value->size() + VARHDRSZ);
                memcpy(VARDATA(datum), value->data(), value->size());
                SET_VARSIZE(datum, value->size() + VARHDRSZ);

                values[datum_offset] = PointerGetDatum(datum);
                isnulls[datum_offset] = false;
            }
        }
        else if (id == NAMEOID) {
            // NAMEOID is a special case that is dynamically allocated, but it is fixed size so it doesn't have a header
             std::optional<std::string> value = record.deserializeNext<std::string>();
            if (value.has_value()) {
                if (value->size() > NAMEDATALEN) {
                    throw std::runtime_error("SKV value is too large for NAMEOID type");
                }
                char* datum = allocManager.alloc(NAMEDATALEN);
                memcpy(datum, value->data(), value->size());

                values[datum_offset] = CStringGetDatum(datum);
                isnulls[datum_offset] = false;
            }
        }
        else if (id == BOOLOID) {
            std::optional<bool> value = record.deserializeNext<bool>();
            if (value.has_value()) {
                values[datum_offset] = (Datum)(*value);
                isnulls[datum_offset] = false;
            }
        }
        else if (is1ByteIntType(id) || is2ByteIntType(id)) {
            std::optional<int16_t> value = record.deserializeNext<int16_t>();
            if (value.has_value()) {
                values[datum_offset] = (Datum)(*value);
                isnulls[datum_offset] = false;
            }
        }
        else if (is4ByteIntType(id)) {
            std::optional<int32_t> value = record.deserializeNext<int32_t>();
            if (value.has_value()) {
                values[datum_offset] = (Datum)(*value);
                isnulls[datum_offset] = false;
            }
        }
        else if (is8ByteIntType(id)) {
            std::optional<int64_t> value = record.deserializeNext<int64_t>();
            if (value.has_value()) {
                values[datum_offset] = (Datum)(*value);
                isnulls[datum_offset] = false;
            }
        }
        else if (isUnsignedPromotedType(id)) {
            std::optional<int64_t> value = record.deserializeNext<int64_t>();
            if (value.has_value()) {
                values[datum_offset] = (Datum)(uint32_t)(*value);
                isnulls[datum_offset] = false;
            }
        }
        else if (id == FLOAT4OID) {
            std::optional<float> value = record.deserializeNext<float>();
            if (value.has_value()) {
                values[datum_offset] = (Datum)(*value);
                isnulls[datum_offset] = false;
            }
        }
        else if (id == FLOAT8OID) {
            std::optional<double> value = record.deserializeNext<double>();
            if (value.has_value()) {
                values[datum_offset] = (Datum)(*value);
                isnulls[datum_offset] = false;
            }
        } else {
            std::optional<std::string> value = record.deserializeNext<std::string>();
            if (value.has_value()) {
                char* datum = allocManager.alloc(value->size());
                memcpy(datum, value->data(), value->size());

                values[datum_offset] = PointerGetDatum(datum);
                isnulls[datum_offset] = false;
            }
        }
    }
    } // try
    catch (const std::exception& err) {
        K2PgStatus status {
            .pg_code = ERRCODE_INTERNAL_ERROR,
            .k2_code = 0,
            .msg = "Deserialization error when converting skvrecord to datums",
            .detail = err.what()
        };

        return status;
    }

    // Last step is the k2pgctid system column, which is virtual and not stored in the record so it is constructed here
    skv::http::dto::SKVRecord keyRecord = record.getSKVKeyRecord();
    skv::http::MPackWriter _writer;
    skv::http::Binary serializedStorage;
    _writer.write(keyRecord.getStorage());
    bool flushResult = _writer.flush(serializedStorage);
    if (!flushResult) {
        K2PgStatus err {
            .pg_code = ERRCODE_INTERNAL_ERROR,
            .k2_code = 0,
            .msg = "Serialization error in _writer flush",
            .detail = ""
        };
        return err;
    }

    // This comes from cstring_to_text_with_len which is used to create a proper datum
    // that is prepended with the data length. Doing it by hand here to avoid the extra copy
    char *datum = allocManager.alloc(serializedStorage.size() + VARHDRSZ);
    SET_VARSIZE(datum, serializedStorage.size() + VARHDRSZ);
    memcpy(VARDATA(datum), serializedStorage.data(), serializedStorage.size());
    syscols->k2pgctid = (uint8_t*)PointerGetDatum(datum);

    allocManager.release();
    return K2PgStatus::OK;
}

skv::http::dto::SKVRecord makePrimaryKeyFromSecondary(skv::http::dto::SKVRecord& secondary, std::shared_ptr<k2pg::PgTableDesc> secondaryTable,
                                                      std::shared_ptr<skv::http::dto::Schema> primarySchema) {
    // Find the skv offset that matches the basetupleid attribute
    uint32_t offset = 0;
    bool found = false;
    for (const auto& column : secondaryTable->columns()) {
        if (column.attr_num() == (int)PgSystemAttrNum::kPgIdxBaseTupleId) {
            offset = column.index() + K2_FIELD_OFFSET;
            found = true;
        }
    }
    if (!found) {
        throw std::runtime_error("kPgIdxBaseTupleId not found for secondary index table");
    }

    secondary.seekField(offset);
    std::optional<std::string> baseidStr = secondary.deserializeNext<std::string>();
    if (!baseidStr.has_value()) {
        throw std::runtime_error("kPgIdxBaseTupleId is null in skv record");
    }

    skv::http::Binary binary(baseidStr->data(), baseidStr->size(), [] () {});
    skv::http::MPackReader reader(binary);
    skv::http::dto::SKVRecord::Storage storage;
    bool success = reader.read(storage);
    if (!success) {
        throw std::runtime_error("Failed to deserialize SKVRecord storage in makePrimaryKeyFromSecondary");
    }
    return skv::http::dto::SKVRecord(secondary.collectionName, primarySchema, std::move(storage), true).getSKVKeyRecord();
}

// Checks if the value children structure of an expression match what is expected by BuildRangeRecords and throws if not
static void ValidateExprChildren(const skv::http::dto::expression::Expression& expr) {
    auto& child_args = expr.valueChildren;
    // the first arg for the child should column reference
    if (child_args.size() < 2) {
        throw std::invalid_argument("Size of valueChildren is < 2");
    }
    if (!child_args[0].isReference()) {
        throw std::invalid_argument("First argument should be column reference");
    }
    if (child_args[1].isReference()) {
        throw std::invalid_argument("Second argument should be constant value");
    }
}

static void AppendValueToRecord(skv::http::dto::expression::Value& value, skv::http::dto::SKVRecordBuilder& builder) {
    skv::http::dto::applyTyped(value, [&value, &builder] (const auto& afr) mutable {
    typename std::remove_reference_t<decltype(afr)>::ValueT constant;
    skv::http::MPackReader reader(value.literal);
    if (reader.read(constant)) {
        K2LOG_D(k2log::k2pg, "read: {}", constant);
        builder.serializeNext(constant);
    }
    else {
        throw std::runtime_error("Unable to deserialize value literal");
    }
    });
}

// Start and end builders must be passed with the metadata fields already serialized (e.g. table and index ID)
void BuildRangeRecords(skv::http::dto::expression::Expression& range_conds, std::vector<skv::http::dto::expression::Expression>& leftover_exprs,
                              skv::http::dto::SKVRecordBuilder& start, skv::http::dto::SKVRecordBuilder& end, bool& isRangeScan) {
    using namespace skv::http::dto::expression;
    using namespace skv::http;

    std::shared_ptr<skv::http::dto::Schema> schema = start.getSchema();

    if (range_conds.op == Operation::UNKNOWN) {
        K2LOG_D(k2log::k2pg, "range_conds UNKNOWN");
        return;
    }

    if (range_conds.op != Operation::AND) {
        std::string msg = "Only AND top-level condition is supported in range expression";
        K2LOG_ERT(k2log::k2pg, "{}", msg);
        //return STATUS(InvalidCommand, msg);
        throw std::invalid_argument(msg);
    }

    if (range_conds.expressionChildren.size() == 0) {
        K2LOG_D(k2log::k2pg, "Child conditions are empty");
        return;
    }

    // use multimap since the same column might have multiple conditions
    std::multimap<std::string, Expression> children_by_col_name;
    for (auto& pg_expr : range_conds.expressionChildren) {
        ValidateExprChildren(pg_expr);
        // the children should be PgOperators for the top "and" condition
        auto& child_args = pg_expr.valueChildren;
        children_by_col_name.insert(std::make_pair(child_args[0].fieldName, pg_expr));
    }

    std::vector<Expression> sorted_args;
    std::vector<dto::SchemaField> fields = schema->fields;
    std::unordered_map<std::string, int> field_map;
    for (size_t i = K2_FIELD_OFFSET; i < fields.size(); i++) {
        field_map[fields[i].name] = i;
        auto range = children_by_col_name.equal_range(fields[i].name);
        for (auto it = range.first; it != range.second; ++it) {
            sorted_args.push_back(it->second);
        }
    }

    int start_idx = K2_FIELD_OFFSET - 1;
    bool didBranch = false;
    isRangeScan = false;
    for (auto& pg_expr : sorted_args) {
        if (didBranch) {
            // there was a branch in the processing of previous condition and we cannot continue.
            // Ideally, this shouldn't happen if the query parser did its job well.
            // This is not an error, and so we can still process the request. PG would down-filter the result set after
            K2LOG_D(k2log::k2pg, "Condition branched at previous key field. Use the condition as filter condition");
            leftover_exprs.push_back(pg_expr);
            continue; // keep going so that we log all skipped expressions;
        }
        auto& args = pg_expr.valueChildren;
        skv::http::dto::expression::Value& col_ref = args[0];
        skv::http::dto::expression::Value& val = args[1];
        int cur_idx = field_map[col_ref.fieldName];

        switch(pg_expr.op) {
            case Operation::EQ: {
                if (cur_idx - start_idx == 0 || cur_idx - start_idx == 1) {
                    start_idx = cur_idx;

                    K2LOG_D(k2log::k2pg, "Appending to start");
                    AppendValueToRecord(val, start);
                    K2LOG_D(k2log::k2pg, "Appending to end");
                    AppendValueToRecord(val, end);
                } else {
                    didBranch = true;
                    K2LOG_D(k2log::k2pg, "Appending to leftover EQ else cur: {}, start: {}", cur_idx, start_idx);
                    leftover_exprs.emplace_back(pg_expr);
                }
            } break;
            case Operation::GTE:
            case Operation::GT: {
                if (cur_idx - start_idx == 0 || cur_idx - start_idx == 1) {
                    start_idx = cur_idx;
                    K2LOG_D(k2log::k2pg, "Appending to start");
                    AppendValueToRecord(val, start);
                    isRangeScan = true;
                } else {
                    didBranch = true;
                }
                // always push the comparison operator to K2 as discussed
                K2LOG_D(k2log::k2pg, "Appending to leftover GT");
                leftover_exprs.emplace_back(pg_expr);
            } break;
            case Operation::LT: {
                if (cur_idx - start_idx == 0 || cur_idx - start_idx == 1) {
                    start_idx = cur_idx;
                    K2LOG_D(k2log::k2pg, "Appending to end");
                    AppendValueToRecord(val, end);
                    isRangeScan = true;
                } else {
                    didBranch = true;
                }
                // always push the comparison operator to K2 as discussed
                K2LOG_D(k2log::k2pg, "Appending to leftover LT");
                leftover_exprs.emplace_back(pg_expr);
            } break;
            case Operation::LTE: {
                /*
                if (cur_idx - start_idx == 0 || cur_idx - start_idx == 1) {
                    start_idx = cur_idx;
                    if (val->getValue()->IsMaxInteger()) {
                        // do not set the range if the value is maximum
                        didBranch = true;
                    } else {
                        K2Adapter::SerializeValueToSKVRecord(val->getValue()->UpperBound(), end);
                    }
                    } else { */
                // Not pushing LTE to range record because end record is exclusive
                didBranch = true;
                isRangeScan = true;
                K2LOG_D(k2log::k2pg, "Appending to leftover LTE");
                leftover_exprs.emplace_back(pg_expr);
            } break;
            default: {
                //const char* msg = "Expression Condition must be one of [BETWEEN, EQ, GE, LE]";
                //K2LOG_WCT(log::k2Adapter, "{}", msg);
                didBranch = true;
                isRangeScan = true;
                K2LOG_D(k2log::k2pg, "Appending to leftover default");
                leftover_exprs.emplace_back(pg_expr);
            } break;
        }
    }
}

skv::http::dto::expression::Value serializePGConstToValue(const K2PgConstant& constant) {
    using namespace skv::http::dto::expression;
    // Three different types of constants to handle:
    // 1: String-like types that we can push down operations into K2.
    // 2: Integer/float types that fit in a K2 equivalent.
    // 3: Arbitrary binary types that we store the datum contents directly including header

    if (constant.is_null) {
        // TODO
        return skv::http::dto::expression::Value();
    }
    else if (isStringType(constant.type_id)) {
        // Borrowed from MOT. This handles stripping the datum header for toasted or non-toasted data
        UntoastedDatum data = UntoastedDatum(constant.datum);
        size_t size = VARSIZE(data.untoasted);  // includes header len VARHDRSZ
        char* src = VARDATA(data.untoasted);
        return makeValueLiteral<std::string>(std::string(src, size - VARHDRSZ));
    }
    else if (constant.type_id == BOOLOID) {
        bool byte = (bool)(((uintptr_t)(constant.datum)) & 0x000000ff);
        return makeValueLiteral<bool>(std::move(byte));
    }
    else if (constant.type_id == NAMEOID) {
        // NAMEOID is dynamically allocated but fixed length
        char* bytes = DatumGetCString(constant.datum);
        return makeValueLiteral<std::string>(std::string(bytes));
    }
    else if (is1ByteIntType(constant.type_id)) {
        int8_t byte = (int8_t)(((uintptr_t)(constant.datum)) & 0x000000ff);
        return makeValueLiteral<int16_t>(std::move(byte));
    }
    else if (is2ByteIntType(constant.type_id)) {
        int16_t byte2 = (int16_t)(((uintptr_t)(constant.datum)) & 0x0000ffff);
        return makeValueLiteral<int16_t>(std::move(byte2));
    }
    else if (is4ByteIntType(constant.type_id)) {
        int32_t byte4 = (int32_t)(((uintptr_t)(constant.datum)) & 0xffffffff);
        return makeValueLiteral<int32_t>(std::move(byte4));
    }
    else if (isUnsignedPromotedType(constant.type_id)) {
        int64_t byte8 = (int64_t)(((uintptr_t)(constant.datum)) & 0xffffffff);
        return makeValueLiteral<int64_t>(std::move(byte8));
    }
    else if (is8ByteIntType(constant.type_id)) {
        int64_t byte8 = (int64_t)constant.datum;
        return makeValueLiteral<int64_t>(std::move(byte8));
    }
    else if (constant.type_id == FLOAT4OID) {
        uint32_t fbytes = (uint32_t)(((uintptr_t)(constant.datum)) & 0xffffffff);
        // We don't want to convert, we want to treat the bytes directly as the float's bytes
        float fval = *(float*)&fbytes;
        return makeValueLiteral<float>(std::move(fval));
    }
    else if (constant.type_id == FLOAT8OID) {
        // We don't want to convert, we want to treat the bytes directly as the double's bytes
        double dval = *(double*)&(constant.datum);
        return makeValueLiteral<double>(std::move(dval));
    } else {
        // Anything else we treat as opaque bytes and include the datum header
        UntoastedDatum data = UntoastedDatum(constant.datum);
        size_t size = VARSIZE(data.untoasted);  // includes header len VARHDRSZ
        return makeValueLiteral<std::string>(std::string((char*)data.untoasted, size));
    }
}

skv::http::dto::expression::Expression buildScanExpr(K2PgScanHandle* scan, const K2PgConstraintDef& constraint, const std::unordered_map<int, uint32_t>& attr_to_offset) {
    using namespace skv::http::dto;
    expression::Expression opr_expr{};

    // Check for types we support for filter pushdown
    if (!isPushdownType(constraint.constants[0].type_id)) {
        return opr_expr;
    }

    switch(constraint.constraint) {
        case K2PgConstraintType::K2PG_CONSTRAINT_EQ: //  equal =
            opr_expr.op = expression::Operation::EQ;
            break;
        case K2PgConstraintType::K2PG_CONSTRAINT_LT:
            opr_expr.op = expression::Operation::LT;
            break;
        case K2PgConstraintType::K2PG_CONSTRAINT_LTE:
            opr_expr.op = expression::Operation::LTE;
            break;
        case K2PgConstraintType::K2PG_CONSTRAINT_GT:
            opr_expr.op = expression::Operation::GT;
            break;
        case K2PgConstraintType::K2PG_CONSTRAINT_GTE:
            opr_expr.op = expression::Operation::GTE;
            break;
        default:
            K2LOG_WCT(k2log::k2pg, "Ignoring scan constraint of type: {}", constraint.constraint);
            return opr_expr;
    }

    auto it = attr_to_offset.find(constraint.attr_num);
    if (it == attr_to_offset.end()) {
        K2LOG_WCT(k2log::k2pg, "Attr_num not found in map for buildScanExpr: {}, constants size {}", constraint.attr_num, constraint.constants.size());
        return expression::Expression();
    }
    uint32_t offset = it->second;

    std::shared_ptr<skv::http::dto::Schema> schema = scan->secondarySchema ? scan->secondarySchema : scan->primarySchema;
    expression::Value col_ref = expression::makeValueReference(schema->fields[offset].name);
    // TODO null, etc
    expression::Value constant = serializePGConstToValue(constraint.constants[0]);
    opr_expr.valueChildren.push_back(std::move(col_ref));
    opr_expr.valueChildren.push_back(std::move(constant));

    return opr_expr;
}

K2PgStatus getSKVBuilder(K2PgOid database_oid, K2PgOid table_oid,
                         std::unique_ptr<skv::http::dto::SKVRecordBuilder>& builder) {
    std::shared_ptr<k2pg::PgTableDesc> pg_table = k2pg::pg_session->LoadTable(database_oid, table_oid);
    const std::string& collectionName = pg_table->collection_name();
    const std::string& schemaName = pg_table->schema_name();

    auto [status, schema] = k2pg::TXMgr.getSchema(collectionName, schemaName).get();
    if (!status.is2xxOK()) {
        return k2pg::K2StatusToK2PgStatus(std::move(status));
    }

    builder = std::make_unique<skv::http::dto::SKVRecordBuilder>(collectionName, schema);
    return K2PgStatus::OK;
}

// May throw if there is a schema mismatch bug
void serializePGConstToK2SKV(skv::http::dto::SKVRecordBuilder& builder, K2PgConstant constant) {
    // Three different types of constants to handle:
    // 1: String-like types that we can push down operations into K2.
    // 2: Integer/float types that fit in a K2 equivalent.
    // 3: Arbitrary binary types that we store the datum contents directly including header

    if (constant.is_null) {
        builder.serializeNull();
        return;
    }
    else if (isStringType(constant.type_id)) {
        // Borrowed from MOT. This handles stripping the datum header for toasted or non-toasted data
        k2pg::UntoastedDatum data = k2pg::UntoastedDatum(constant.datum);
        size_t size = VARSIZE(data.untoasted);  // includes header len VARHDRSZ
        char* src = VARDATA(data.untoasted);
        builder.serializeNext<std::string>(std::string(src, size - VARHDRSZ));
    }
    else if (constant.type_id == BOOLOID) {
        bool byte = (bool)(((uintptr_t)(constant.datum)) & 0x000000ff);
        builder.serializeNext<bool>(byte);
    }
    else if (constant.type_id == NAMEOID) {
        // NAMEOID is dynamically allocated but fixed length
        char* bytes = DatumGetCString(constant.datum);
        return builder.serializeNext<std::string>(std::string(bytes));
    }
    else if (is1ByteIntType(constant.type_id)) {
        int8_t byte = (int8_t)(((uintptr_t)(constant.datum)) & 0x000000ff);
        builder.serializeNext<int16_t>(byte);
    }
    else if (is2ByteIntType(constant.type_id)) {
        int16_t byte2 = (int16_t)(((uintptr_t)(constant.datum)) & 0x0000ffff);
        builder.serializeNext<int16_t>(byte2);
    }
    else if (is4ByteIntType(constant.type_id)) {
        int32_t byte4 = (int32_t)(((uintptr_t)(constant.datum)) & 0xffffffff);
        builder.serializeNext<int32_t>(byte4);
    }
    else if (isUnsignedPromotedType(constant.type_id)) {
        int64_t uint = (int64_t)(((uintptr_t)(constant.datum)) & 0xffffffff);
        builder.serializeNext<int64_t>(uint);
    }
    else if (is8ByteIntType(constant.type_id)) {
        int64_t byte8 = (int64_t)constant.datum;
        builder.serializeNext<int64_t>(byte8);
    }
    else if (constant.type_id == FLOAT4OID) {
        uint32_t fbytes = (uint32_t)(((uintptr_t)(constant.datum)) & 0xffffffff);
        // We don't want to convert, we want to treat the bytes directly as the float's bytes
        float fval = *(float*)&fbytes;
        builder.serializeNext<float>(fval);
    }
    else if (constant.type_id == FLOAT8OID) {
        // We don't want to convert, we want to treat the bytes directly as the double's bytes
        double dval = *(double*)&(constant.datum);
        builder.serializeNext<double>(dval);
    } else {
        // Anything else we treat as opaque bytes and include the datum header
        k2pg::UntoastedDatum data = k2pg::UntoastedDatum(constant.datum);
        size_t size = VARSIZE(data.untoasted);  // includes header len VARHDRSZ
        builder.serializeNext<std::string>(std::string((char*)data.untoasted, size));
    }
}

skv::http::dto::SKVRecord tupleIDDatumToSKVRecord(Datum tuple_id, std::string collection, std::shared_ptr<skv::http::dto::Schema> schema) {
    k2pg::UntoastedDatum data = k2pg::UntoastedDatum(tuple_id);
    size_t size = VARSIZE(data.untoasted) - VARHDRSZ;
    char* src = VARDATA(data.untoasted);
    // No-op deleter. Data is owned by PG heap and we will not access it outside of this function
    skv::http::Binary binary(src, size, [] () {});
    skv::http::MPackReader reader(binary);
    skv::http::dto::SKVRecord::Storage storage;
    reader.read(storage);
    return skv::http::dto::SKVRecord(collection, schema, std::move(storage), true);
}

K2PgStatus makeSKVBuilderWithKeysSerialized(K2PgOid database_oid, K2PgOid table_oid,
                                           const std::vector<K2PgAttributeDef>& columns,
                                           std::unique_ptr<skv::http::dto::SKVRecordBuilder>& builder) {
    skv::http::dto::SKVRecord record;
    bool use_tupleID = false;

    // Check if we have the virtual tupleID column and if so deserialize the datum into a SKVRecord
    for (auto& attribute : columns) {
        if (attribute.attr_num == K2PgTupleIdAttributeNumber && !attribute.value.is_null) {
            std::unique_ptr<skv::http::dto::SKVRecordBuilder> builder;
            K2PgStatus status = getSKVBuilder(database_oid, table_oid, builder);
            if (status.pg_code != ERRCODE_SUCCESSFUL_COMPLETION) {
                return status;
            }

            if (attribute.value.datum != 0) {
                record = tupleIDDatumToSKVRecord(attribute.value.datum, builder->getCollectionName(), builder->getSchema());
                use_tupleID = true;
            } else {
                K2LOG_WCT(k2log::k2pg, "TupeId is NULL in makeSKVBuilderWithKeys for schema {}", *(builder->getSchema()));
            }
        }
    }

    // Get a SKVBuilder
    K2PgStatus status = getSKVBuilder(database_oid, table_oid, builder);
    if (status.pg_code != ERRCODE_SUCCESSFUL_COMPLETION) {
        return status;
    }

    // If we have a tupleID just need to copy fields from the record to a builder and we are done
    if (use_tupleID) {
        K2PgStatus status = serializeKeysFromSKVRecord(record, *builder);
        if (status.pg_code != ERRCODE_SUCCESSFUL_COMPLETION) {
            return status;
        }

        return K2PgStatus::OK;
    }


    // If we get here it is because we did not have a tupleID, so prepare to serialize keys from the provided columns

    // Get attribute to SKV offset mapping
    std::shared_ptr<k2pg::PgTableDesc> pg_table = k2pg::pg_session->LoadTable(database_oid, table_oid);
    std::unordered_map<int, K2PgConstant> attr_map;
    for (const auto& column : columns) {
        k2pg::PgColumn *pg_column = pg_table->FindColumn(column.attr_num);
        if (pg_column == NULL) {
            K2PgStatus status {
                .pg_code = ERRCODE_INTERNAL_ERROR,
                .k2_code = 404,
                .msg = "Cannot find column with attr_num",
                .detail = "Load table failed"
            };
            return status;
        }
        // we have two extra fields, i.e., table_id and index_id, in skv key
        attr_map[pg_column->index() + 2] = column.value;
    }

    // Determine table id and index id to use as first two fields in SKV record.
    // The passed in table id may or may not be a secondary index
    uint32_t base_table_oid = pg_table->base_table_oid();
    uint32_t index_id = base_table_oid == table_oid ? 0 : table_oid;

    // Last, serialize key fields into the builder
    try {
        builder->serializeNext<int64_t>((int64_t)base_table_oid);
        builder->serializeNext<int64_t>((int64_t)index_id);

        for (size_t i = K2_FIELD_OFFSET; i < builder->getSchema()->partitionKeyFields.size(); ++i) {
            auto it = attr_map.find(i);
            if (it == attr_map.end()) {
                builder->serializeNull();
            } else {
                serializePGConstToK2SKV(*builder, it->second);
            }
        }
    }
    catch (const std::exception& err) {
        K2PgStatus status {
            .pg_code = ERRCODE_INTERNAL_ERROR,
            .k2_code = 0,
            .msg = "Serialization error in serializePgAttributesToSKV",
            .detail = err.what()
        };

        return status;
    }

    return K2PgStatus::OK;
}

K2PgStatus serializeKeysFromSKVRecord(skv::http::dto::SKVRecord& source, skv::http::dto::SKVRecordBuilder& builder) {
    try {
        source.seekField(0);
        std::optional<int64_t> table_id = source.deserializeNext<int64_t>();
        builder.serializeNext<int64_t>(*table_id);
        std::optional<int64_t> index_id = source.deserializeNext<int64_t>();
        builder.serializeNext<int64_t>(*index_id);

        for (size_t i=K2_FIELD_OFFSET; i < source.schema->partitionKeyFields.size(); ++i) {
            source.visitNextField([&builder] (const auto& field, auto&& value) mutable {
                using T = typename std::remove_reference_t<decltype(value)>::value_type;
                if (!value.has_value()) {
                    builder.serializeNull();
                } else {
                    builder.serializeNext<T>(*value);
                }
            });
        }
    }

    catch (const std::exception& err) {
        K2PgStatus status {
            .pg_code = ERRCODE_INTERNAL_ERROR,
            .k2_code = 0,
            .msg = "Serialization error in serializeKeysFromSKVRecord",
            .detail = err.what()
        };

        return status;
    }

    return K2PgStatus::OK;
}

K2PgStatus serializePgAttributesToSKV(skv::http::dto::SKVRecordBuilder& builder, uint32_t table_id, uint32_t index_id,
                                      const std::vector<K2PgAttributeDef>& attrs, const std::unordered_map<int, uint32_t>& attr_num_to_index) {
    std::unordered_map<int, K2PgConstant> attr_map;
    for (size_t i=0; i < attrs.size(); ++i) {
        auto it = attr_num_to_index.find(attrs[i].attr_num);
        if (it != attr_num_to_index.end()) {
            attr_map[it->second] = attrs[i].value;
        }
    }

    try {
        builder.serializeNext<int64_t>((int64_t)table_id);
        builder.serializeNext<int64_t>((int64_t)index_id);

        for (size_t i = K2_FIELD_OFFSET; i < builder.getSchema()->fields.size(); ++i) {
            auto it = attr_map.find(i);
            if (it == attr_map.end()) {
                builder.serializeNull();
            } else {
                serializePGConstToK2SKV(builder, it->second);
            }
        }
    }
    catch (const std::exception& err) {
        K2PgStatus status {
            .pg_code = ERRCODE_INTERNAL_ERROR,
            .k2_code = 0,
            .msg = "Serialization error in serializePgAttributesToSKV",
            .detail = err.what()
        };

        return status;
    }

    return K2PgStatus::OK;
}

K2PgStatus makeSKVRecordFromK2PgAttributes(K2PgOid database_oid, K2PgOid table_oid,
                                           const std::vector<K2PgAttributeDef>& columns,
                                           skv::http::dto::SKVRecord& record,
                                           std::shared_ptr<k2pg::PgTableDesc> pg_table) {
    std::unordered_map<int, uint32_t> attr_to_offset;
    uint32_t base_table_oid = pg_table->base_table_oid();

    for (const auto& column : columns) {
        k2pg::PgColumn *pg_column = pg_table->FindColumn(column.attr_num);
        if (pg_column == NULL) {
            K2PgStatus status {
                .pg_code = ERRCODE_INTERNAL_ERROR,
                .k2_code = 404,
                .msg = "Cannot find column with attr_num",
                .detail = "Load table failed"
            };
            return status;
        }
        // we have two extra fields, i.e., table_id and index_id, in skv key
        attr_to_offset[column.attr_num] = pg_column->index() + 2;

        // If we have a basetupleid (ie this is a secondary index insert), verify that the basetupleid is a valid SKVRecord
        if (column.attr_num == (int)PgSystemAttrNum::kPgIdxBaseTupleId) {
            try {
                std::unique_ptr<skv::http::dto::SKVRecordBuilder> base_builder;
                K2PgStatus bstatus = getSKVBuilder(database_oid, base_table_oid, base_builder);

                std::vector<K2PgAttributeDef> base_attr = {column};
                base_attr[0].attr_num = (int)K2PgTupleIdAttributeNumber;
                K2PgStatus base_status = makeSKVBuilderWithKeysSerialized(database_oid, base_table_oid,
                                           base_attr,
                                           base_builder);
                if (base_status.pg_code != ERRCODE_SUCCESSFUL_COMPLETION) {
                    K2LOG_WCT(k2log::k2pg, "Bad basetupleid, pg status: {}, k2 status: {}, msg: {}", base_status.pg_code, base_status.k2_code, base_status.msg);
                    return base_status;
                }
            }
            catch (...) {
                K2PgStatus status {
                    .pg_code = ERRCODE_INTERNAL_ERROR,
                    .k2_code = 500,
                    .msg = "Bad basetupleid",
                    .detail = "Bad basetupleid"
                };
                return status;
            }
            K2LOG_D(k2log::k2pg, "Verified basetupleid");
        }
    }

    uint32_t index_id = base_table_oid == table_oid ? 0 : table_oid;

    std::unique_ptr<skv::http::dto::SKVRecordBuilder> builder;
    K2PgStatus status = getSKVBuilder(database_oid, table_oid, builder);
    if (status.pg_code != ERRCODE_SUCCESSFUL_COMPLETION) {
        return status;
    }

    status = serializePgAttributesToSKV(*builder, base_table_oid, index_id, columns, attr_to_offset);
    if (status.pg_code != ERRCODE_SUCCESSFUL_COMPLETION) {
        return status;
    }

    record = builder->build();
    return K2PgStatus::OK;
}


} // k2pg ns
} // gate ns
