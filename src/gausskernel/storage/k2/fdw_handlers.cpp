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

#include "libintl.h"
#include "postgres.h"
#include "funcapi.h"
#include "access/reloptions.h"
#include "access/transam.h"

#include <cstdint>
#include <iostream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <skvhttp/dto/SKVRecord.h>
#include <skvhttp/client/SKVClient.h>
#include <skvhttp/dto/Expression.h>

#include "commands/dbcommands.h"
#include "catalog/pg_foreign_table.h"
#include "commands/copy.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/vacuum.h"
#include "commands/tablecmds.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "nodes/nodes.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "utils/memutils.h"
#include "utils/numeric.h"
#include "utils/rel.h"
#include "utils/date.h"
#include "utils/syscache.h"
#include "utils/partitionkey.h"
#include "catalog/heap.h"
#include "optimizer/var.h"
#include "optimizer/clauses.h"
#include "optimizer/pathnode.h"
#include "optimizer/subselect.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_database.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "utils/lsyscache.h"

#include "error_reporting.h"
#include "parse.h"

namespace k2fdw {

class PgStatement;

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

// Structure to hold parameters for preparing query plan.
//
// Index-related parameters are used to describe different types of scan.
//   - Sequential scan: Index parameter is not used.
//     { index_oid, index_only_scan, use_secondary_index } = { kInvalidOid, false, false }
//   - IndexScan:
//     { index_oid, index_only_scan, use_secondary_index } = { IndexOid, false, true }
//   - IndexOnlyScan:
//     { index_oid, index_only_scan, use_secondary_index } = { IndexOid, true, true }
//   - PrimaryIndexScan: This is a special case as K2 SQL doesn't have a separated
//     primary-index database object from table object.
//       index_oid = TableOid
//       index_only_scan = true if ROWID is wanted. Otherwise, regular rowset is wanted.
//       use_secondary_index = false
//
struct PgPrepareParameters {
    Oid index_oid;
    bool index_only_scan;
    bool use_secondary_index;
};

// Structure to hold the execution-control parameters.
struct PgExecParameters {
    // TODO(neil) Move forward_scan flag here.
    // Scan parameters.
    // bool is_forward_scan;

    // LIMIT parameters for executing DML read.
    // - limit_count is the value of SELECT ... LIMIT
    // - limit_offset is value of SELECT ... OFFSET
    // - limit_use_default: Although count and offset are pushed down to K2 platform from Postgres,
    //   they are not always being used to identify the number of rows to be read from K2 platform.
    //   Full-scan is needed when further operations on the rows are not done by K2 platform.
    //
    //   Examples:
    //   o All rows must be sent to Postgres code layer
    //     for filtering before LIMIT is applied.
    //   o ORDER BY clause is not processed by K2 platform. Similarly all rows must be fetched and sent
    //     to Postgres code layer.
    int64_t limit_count;
    uint64_t limit_offset;
    bool limit_use_default;
    // For now we only support one rowmark.
    int rowmark = -1;
    uint64_t read_time = 0;
    char *partition_key = NULL;
};

struct K2FdwPlanState
{
    /* Bitmap of attribute (column) numbers that we need to fetch from K2. */
    Bitmapset *target_attrs;
    /*
    * Restriction clauses, divided into safe and unsafe to pushdown subsets.
    */
    List      *remote_conds;
    List      *local_conds;
};

/*
 * FDW-specific information for ForeignScanState.fdw_state.
 */

struct K2FdwExecState
{
    /* The handle for the internal K2PG Select statement. */
    PgStatement*   handle;
    ResourceOwner   stmt_owner;

    Relation index;

    List *remote_exprs;

    /* Oid of the table being scanned */
    Oid tableOid;

    /* Kept query-plan control to pass it to PgGate during preparation */
    PgPrepareParameters prepare_params;

    PgExecParameters *exec_params; /* execution control parameters for K2 PG */
    bool is_exec_done; /* Each statement should be executed exactly one time */
    skv::http::dto::QueryRequest query;
};

struct K2FdwScanPlanData
{
    /* The relation where to read data from */
    Relation target_relation;

    /* Set of key columns whose values will be used for scanning. */
    Bitmapset *sk_cols;

    // ParamListInfo structures are used to pass parameters into the executor for parameterized plans
    ParamListInfo paramLI;

    /* Description and attnums of the columns to bind */
    TupleDesc bind_desc;

    std::shared_ptr<skv::http::dto::Schema> schema;
    skv::http::dto::expression::Expression range_conds;
    skv::http::dto::expression::Expression where_conds;
};

using skv::http::String;
// Foreign Oid -> Schema
static std::unordered_map<uint64_t, std::shared_ptr<skv::http::dto::Schema>> schemaCache;
static skv::http::Client *client;
static bool initialized=false;

std::shared_ptr<skv::http::dto::Schema> getSchema(Oid foid) {
    auto it = schemaCache.find(foid);
    std::cout << "trying to get schema for foid: " << foid << std::endl;
    if (it == schemaCache.end()) {
        String schemaName = std::to_string(foid);

        auto&&[status, schemaResp] = client->getSchema("postgres", schemaName).get();
        if (!status.is2xxOK()) {
            return nullptr;
        }

        schemaCache[foid] = schemaResp;
        it = schemaCache.find(foid);
    }

    return it->second;
}

/*
 * Get k2Sql-specific table metadata and load it into the scan_plan.
 * Currently only the hash and primary key info.
 */
static void LoadTableInfo(Oid foid, K2FdwScanPlanData* scan_plan)
{
    //Oid            dboid          = K2PgGetDatabaseOid(relation);
    //Oid            relid          = relation->foreignOid;

    scan_plan->schema = getSchema(foid);

    /*
    K2PgTableDesc k2pg_table_desc = NULL;

    // TODO catalog integration
    HandleK2PgStatus(PgGate_GetTableDesc(dboid, relid, &k2pg_table_desc));

    scan_plan->nkeys = 0;
    scan_plan->nNonKeys = 0;
    // number of attributes in the relation tuple
    for (AttrNumber attnum = 1; attnum <= relation->rd_att->natts; attnum++)
    {
        K2CheckPrimaryKeyAttribute(scan_plan, k2pg_table_desc, attnum);
    }
    // we generate OIDs for rows of relation
    if (relation->rd_rel->relhasoids)
    {
        K2CheckPrimaryKeyAttribute(scan_plan, k2pg_table_desc, ObjectIdAttributeNumber);
    }
    */
}

static constexpr uint32_t K2_FIELD_OFFSET = 2;
skv::http::dto::FieldType PGTypidToK2Type(Oid) {
    return skv::http::dto::FieldType::INT32T;
}

skv::http::dto::expression::Expression build_expr(K2FdwScanPlanData* scan_plan, FDWOprCond *opr_cond) {
    using namespace skv::http::dto;
    expression::Expression opr_expr{};

    // Check for types we support for filter pushdown
    // We pushdown: basic scalar types (int, float, bool),
    // text and string types, and all PG internal types that map to K2 scalar types
    const FieldType ref_type = PGTypidToK2Type(opr_cond->ref->attr_typid);
    switch (opr_cond->ref->attr_typid) {
        case CHAROID:
        case NAMEOID:
        case TEXTOID:
        case VARCHAROID:
        case CSTRINGOID:
            break;
        default:
            if (ref_type == FieldType::STRING) {
                return opr_expr;
            }
            break;
    }

    // FDWOprCond separate column refs and constant literals without keeping the
    // structure of the expression, so we need to switch the direction of the comparison if the
    // column reference was not first in the original expression.
    // TODO LTEQ GTEQ
    switch(get_oprrest(opr_cond->opno)) {
        case F_EQSEL: //  equal =
            opr_expr.op = expression::Operation::EQ;
            break;
        case F_SCALARLTSEL: // Less than <
            opr_expr.op = opr_cond->column_ref_first ? expression::Operation::LTE : expression::Operation::GTE;
            break;
            //case F_SCALARLESEL: // Less Equal <=
            //opr_expr.op = opr_cond->column_ref_first ? expression::Operation::LTE : expression::Operation::GTE;
            //break;
        case F_SCALARGTSEL: // Greater than >
            opr_expr.op = opr_cond->column_ref_first ? expression::Operation::GTE : expression::Operation::LTE;
            break;
            //case F_SCALARGESEL: // Greater Euqal >=
            //opr_expr.op = opr_cond->column_ref_first ? expression::Operation::GTE : expression::Operation::LTE;
            //break;
        default:
            elog(DEBUG4, "FDW: unsupported OpExpr type: %d", opr_cond->opno);
            return opr_expr;
    }

    expression::Value col_ref = expression::makeValueReference(scan_plan->schema->fields[K2_FIELD_OFFSET + opr_cond->ref->attr_num - 1].name);
    // TODO k2 types, null?
    int32_t k2val = (int32_t)(opr_cond->val->value & 0xffffffff);
    expression::Value constant = expression::makeValueLiteral<int32_t>(std::move(k2val));
    opr_expr.valueChildren.push_back(std::move(col_ref));
    opr_expr.valueChildren.push_back(std::move(constant));

    return opr_expr;
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
        std::cout << constant << std::endl;
        builder.serializeNext(constant);
    }
    else {
        throw std::runtime_error("Unable to deserialize value literal");
    }
    });
}

// Start and end builders must be passed with the metadata fields already serialized (e.g. table and index ID)
static void BuildRangeRecords(skv::http::dto::expression::Expression& range_conds, std::vector<skv::http::dto::expression::Expression>& leftover_exprs,
                              skv::http::dto::SKVRecordBuilder& start, skv::http::dto::SKVRecordBuilder& end, std::shared_ptr<skv::http::dto::Schema> schema) {
    using namespace skv::http::dto::expression;
    using namespace skv::http;
    if (range_conds.op == Operation::UNKNOWN) {
        std::cout << "range_conds UNKNOWN" << std::endl;
        return;
    }

    if (range_conds.op != Operation::AND) {
        std::cout << "range_conds not AND" << std::endl;
        std::string msg = "Only AND top-level condition is supported in range expression";
        //K2LOG_E(log::k2Adapter, "{}", msg);
        //return STATUS(InvalidCommand, msg);
        throw std::invalid_argument(msg);
    }

    if (range_conds.expressionChildren.size() == 0) {
        std::cout << "range_conds 0 children" << std::endl;
        //K2LOG_D(log::k2Adapter, "Child conditions are empty");
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
    for (auto& pg_expr : sorted_args) {
        if (didBranch) {
            // there was a branch in the processing of previous condition and we cannot continue.
            // Ideally, this shouldn't happen if the query parser did its job well.
            // This is not an error, and so we can still process the request. PG would down-filter the result set after
            //K2LOG_D(log::k2Adapter, "Condition branched at previous key field. Use the condition as filter condition");
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
                    std::cout << "Appending to start: ";
                    AppendValueToRecord(val, start);
                    std::cout << "Appending to end: ";
                    AppendValueToRecord(val, end);
                } else {
                    didBranch = true;
                    std::cout << "Appending to leftover EQ else cur: " << cur_idx << " start: " << start_idx << std::endl;
                    leftover_exprs.emplace_back(pg_expr);
                }
            } break;
            case Operation::GTE:
            case Operation::GT: {
                if (cur_idx - start_idx == 0 || cur_idx - start_idx == 1) {
                    start_idx = cur_idx;
                    std::cout << "Appending to start: ";
                    AppendValueToRecord(val, start);
                } else {
                    didBranch = true;
                }
                // always push the comparison operator to K2 as discussed
                std::cout << "Appending to leftover GT" << std::endl;
                leftover_exprs.emplace_back(pg_expr);
            } break;
            case Operation::LT: {
                if (cur_idx - start_idx == 0 || cur_idx - start_idx == 1) {
                    start_idx = cur_idx;
                    std::cout << "Appending to end: ";
                    AppendValueToRecord(val, end);
                } else {
                    didBranch = true;
                }
                // always push the comparison operator to K2 as discussed
                std::cout << "Appending to leftover LT" << std::endl;
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
                std::cout << "Appending to leftover LTE" << std::endl;
                leftover_exprs.emplace_back(pg_expr);
            } break;
            default: {
                //const char* msg = "Expression Condition must be one of [BETWEEN, EQ, GE, LE]";
                //K2LOG_W(log::k2Adapter, "{}", msg);
                didBranch = true;
                std::cout << "Appending to leftover default" << std::endl;
                leftover_exprs.emplace_back(pg_expr);
            } break;
        }
    }
}

static void BindScanKeys(Relation relation,
                            K2FdwExecState *fdw_state,
                            K2FdwScanPlanData* scan_plan) {
    if (list_length(fdw_state->remote_exprs) == 0) {
        elog(DEBUG4, "FDW: No remote exprs to bind keys for relation: %d", relation->rd_id);
        return;
    }

    foreign_expr_cxt context;
    context.opr_conds = NIL;

    parse_conditions(fdw_state->remote_exprs, scan_plan->paramLI, &context);
    elog(DEBUG4, "FDW: found %d opr_conds from %d remote exprs for relation: %d", list_length(context.opr_conds), list_length(fdw_state->remote_exprs), relation->rd_id);
    if (list_length(context.opr_conds) == 0) {
        elog(DEBUG4, "FDW: No Opr conditions are found to bind keys for relation: %d", relation->rd_id);
        return;
    }

    using namespace skv::http::dto::expression;

    int nKeys = scan_plan->schema->partitionKeyFields.size() + scan_plan->schema->rangeKeyFields.size() - K2_FIELD_OFFSET;
    // Top level should be an "AND" node
    Expression range_conds{};
    range_conds.op = Operation::AND;

    /* Bind the scan keys */
    for (int i = 0; i < nKeys; i++) {
        int attrNum = i + 1;
        // check if the key is in the Opr conditions
        List *opr_conds = findOprCondition(context, attrNum);
        if (opr_conds != NIL) {
            elog(DEBUG4, "FDW: binding key with attr_num %d for relation: %d", attrNum, relation->rd_id);
            ListCell *lc = NULL;
            foreach (lc, opr_conds) {
                FDWOprCond *opr_cond = (FDWOprCond *) lfirst(lc);
                Expression arg = build_expr(scan_plan, opr_cond);
                if (arg.op != Operation::UNKNOWN) {
                    // use primary keys as range condition
                    range_conds.expressionChildren.push_back(std::move(arg));
                }
            }
        }
    }

    //HandleK2PgStatusWithOwner(PgGate_DmlBindRangeConds(fdw_state->handle, range_conds),
    //                                                    fdw_state->handle,
    //                                                    fdw_state->stmt_owner);

    Expression where_conds{};
    // Top level should be an "AND" node
    where_conds.op = Operation::AND;
    int nNonKeys = scan_plan->schema->fields.size() - nKeys - K2_FIELD_OFFSET;
    // bind non-keys
    for (int i = nKeys; i < nNonKeys + nNonKeys; i++)
    {
        int attrNum = i + 1;
        // check if the column is in the Opr conditions
        List *opr_conds = findOprCondition(context, attrNum);
        if (opr_conds != NIL) {
            elog(DEBUG4, "FDW: binding key with attr_num %d for relation: %d", attrNum, relation->rd_id);
            ListCell *lc = NULL;
            foreach (lc, opr_conds) {
                FDWOprCond *opr_cond = (FDWOprCond *) lfirst(lc);
                Expression arg = build_expr(scan_plan, opr_cond);
                if (arg.op != Operation::UNKNOWN) {
                    where_conds.expressionChildren.push_back(std::move(arg));
                }
            }
        }
    }

    //HandleK2PgStatusWithOwner(PgGate_DmlBindWhereConds(fdw_state->handle, where_conds),
    //                                                    fdw_state->handle,
    //                                                    fdw_state->stmt_owner);
    scan_plan->range_conds = std::move(range_conds);
    scan_plan->where_conds = std::move(where_conds);
}

void
k2BeginForeignScan(ForeignScanState *node, int eflags)
{
    std::cout << "BeginForeignScan" << std::endl;
    Relation    relation     = node->ss.ss_currentRelation;
    ForeignScan *foreignScan = (ForeignScan *) node->ss.ps.plan;

    K2FdwExecState *k2pg_state = NULL;

    /* Do nothing in EXPLAIN (no ANALYZE) case.  node->fdw_state stays NULL. */
    if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
        return;

    /* Allocate and initialize K2PG scan state. */
    k2pg_state = (K2FdwExecState *) palloc0(sizeof(K2FdwExecState));

    node->fdw_state = (void *) k2pg_state;
    // TODO
    // EState      *estate      = node->ss.ps.state;
    //HandleK2PgStatus(PgGate_NewSelect(K2PgGetDatabaseOid(relation),
    //               RelationGetRelid(relation),
    //               NULL /* prepare_params */,
    //               &k2pg_state->handle));
    //ResourceOwnerEnlargeK2PgStmts(CurrentResourceOwner);
    //ResourceOwnerRememberK2PgStmt(CurrentResourceOwner, k2pg_state->handle);
    //k2pg_state->stmt_owner = CurrentResourceOwner;
    //k2pg_state->exec_params = &estate->k2pg_exec_params;
    k2pg_state->remote_exprs = foreignScan->fdw_exprs;
    elog(DEBUG4, "FDW: foreign_scan for relation %d, fdw_exprs: %d", relation->rd_id, list_length(foreignScan->fdw_exprs));

    //k2pg_state->exec_params->rowmark = -1;
    //ListCell   *l;
    //foreach(l, estate->es_rowMarks) {
    //    ExecRowMark *erm = (ExecRowMark *) lfirst(l);
        // Do not propogate non-row-locking row marks.
    //    if (erm->markType != ROW_MARK_REFERENCE &&
    //        erm->markType != ROW_MARK_COPY)
    //        k2pg_state->exec_params->rowmark = erm->markType;
    //    break;
    //}

    k2pg_state->is_exec_done = false;

    /* Set the current syscatalog version (will check that we are up to date) */
    // TODO
    //HandleK2PgStatusWithOwner(PgGate_SetCatalogCacheVersion(k2pg_state->handle,
    //                                                    k2pg_catalog_cache_version),
    //                                                    k2pg_state->handle,
    //                                                    k2pg_state->stmt_owner);
    std::cout << "BeginForeignScan done" << std::endl;
}

/*
 * k2IterateForeignScan
 *        Read next record from the data file and store it into the
 *        ScanTupleSlot as a virtual tuple
 */
TupleTableSlot *
k2IterateForeignScan(ForeignScanState *node)
{
    std::cout << "IterateForeignScan" << std::endl;
    TupleTableSlot *slot= nullptr;
    K2FdwExecState *k2pg_state = (K2FdwExecState *) node->fdw_state;
    Relation relation = node->ss.ss_currentRelation;

    auto&& [status, txnHandle] = client->beginTxn(skv::http::dto::TxnOptions()).get();

    /* Execute the select statement one time. */
    if (!k2pg_state->is_exec_done) {
        K2FdwScanPlanData scan_plan{};

        scan_plan.target_relation = relation;
        scan_plan.paramLI = node->ss.ps.state->es_param_list_info;
        LoadTableInfo(RelationGetRelid(relation), &scan_plan);
        scan_plan.bind_desc = RelationGetDescr(relation);
        BindScanKeys(relation, k2pg_state, &scan_plan);
        std::cout << "IterateForeignScan BindScanKeys done" << std::endl;

        skv::http::dto::SKVRecordBuilder startBuild("postgres", scan_plan.schema);
        startBuild.serializeNext<String>(scan_plan.schema->name);
        startBuild.serializeNext<int32_t>(0);
        skv::http::dto::SKVRecordBuilder endBuild("postgres", scan_plan.schema);
        endBuild.serializeNext<String>(scan_plan.schema->name);
        endBuild.serializeNext<int32_t>(0);
        std::vector<skv::http::dto::expression::Expression> leftovers;
        BuildRangeRecords(scan_plan.range_conds, leftovers, startBuild, endBuild, scan_plan.schema);
        skv::http::dto::SKVRecord startScanRecord = startBuild.build();
        skv::http::dto::SKVRecord endScanRecord = endBuild.build();
        std::cout << "IterateForeignScan BuildRangeRecords done" << std::endl;
        for (skv::http::dto::expression::Expression& expr : leftovers) {
            scan_plan.where_conds.expressionChildren.push_back(std::move(expr));
        }
        if (scan_plan.where_conds.expressionChildren.size() == 0) {
            scan_plan.where_conds = skv::http::dto::expression::Expression{};
        }
        auto&&[status, query] = txnHandle.createQuery(startScanRecord, endScanRecord, std::move(scan_plan.where_conds)).get();
        std::cout << "IterateForeignScan createQuery done" << std::endl;
        if (!status.is2xxOK()) {
            std::cout << "error on createQuery: " << status << std::endl;
            return slot;
            // err
        }

        k2pg_state->query = std::move(query);
        //k2SetupScanTargets(node);
        //HandleK2PgStatusWithOwner(PgGate_ExecSelect(k2pg_state->handle, k2pg_state->exec_params),
        //                        k2pg_state->handle,
        //                        k2pg_state->stmt_owner);
        k2pg_state->is_exec_done = true;
    }

    /* Clear tuple slot before starting */
    slot = node->ss.ss_ScanTupleSlot;
    ExecClearTuple(slot);

    std::cout << "IterateForeignScan tuple prep done" << std::endl;

    auto&& [statusQ, result] = txnHandle.query(k2pg_state->query).get();
    if (!statusQ.is2xxOK()) {
        std::cout << "query error: " << statusQ.message << std::endl;
        return slot;
    }
    std::cout << "query done result size: " << result.records.size() << std::endl;
    /* Fetch one row. */
    //bool           has_data   = false;
    //TupleDesc       tupdesc = slot->tts_tupleDescriptor;
    //Datum           *values = slot->tts_values;
    //bool            *isnull = slot->tts_isnull;
    //PgSysColumns syscols;
    //HandleK2PgStatusWithOwner(PgGate_DmlFetch(k2pg_state->handle,
    //                                      tupdesc->natts,
    //                                      (uint64_t *) values,
    //                                      isnull,
    //                                      &syscols,
    //                                      &has_data),
    //                        k2pg_state->handle,
    //                        k2pg_state->stmt_owner);

    /* If we have result(s) update the tuple slot. */
    if (result.records.size())
    {
        // TODO
        //if (node->k2pg_fdw_aggs == NIL)
        //{
        skv::http::dto::SKVRecord record("postgres", getSchema(RelationGetRelid(relation)), std::move(result.records[0]), true);
        record.seekField(K2_FIELD_OFFSET);
        std::optional<int32_t> value = record.deserializeNext<int32_t>();
        std::cout << "K2FDW: scanned a record with val: " << *value << std::endl;
        std::optional<int32_t> value2 = record.deserializeNext<int32_t>();
        std::cout << "K2FDW: scanned a record with val2: " << *value2 << std::endl;
        std::cout << "IterateForeignScan deserialize done" << std::endl;

        //HeapTuple tuple = heap_form_tuple(tupdesc, values, isnull);
        slot->tts_isnull[0] = false;
        slot->tts_values[0] = NumericGetDatum(*value);
        slot->tts_isnull[1] = false;
        slot->tts_values[1] = NumericGetDatum(*value2);

        //if (syscols.oid != InvalidOid)
        //{
        //    HeapTupleSetOid(tuple, syscols.oid);
        //}

        ExecStoreVirtualTuple(slot);
        // TODO xmin xmax like MOT
        //slot = ExecStoreTuple(tuple, slot, InvalidBuffer, false);
        // TODO
        /* Setup special columns in the slot */
        //slot->tts_k2pgctid = PointerGetDatum(syscols.k2pgctid);
        //}
        //else
        //{
        /*
            * Aggregate results stored in virtual slot (no tuple). Set the
            * number of valid values and mark as non-empty.
            */
        //slot->tts_nvalid = tupdesc->natts;
        //slot->tts_isempty = false;
        //}
    }

    return slot;
}

/*
 * k2GetForeignRelSize
 *      Obtain relation size estimates for a foreign table
 */
void
k2GetForeignRelSize(PlannerInfo *root,
                RelOptInfo *baserel,
                Oid foreigntableid)
{
    K2FdwPlanState      *fdw_plan = NULL;

    fdw_plan = (K2FdwPlanState *) palloc0(sizeof(K2FdwPlanState));

    /* Set the estimate for the total number of rows (tuples) in this table. */
    baserel->tuples = 1000;

    /*
    * Initialize the estimate for the number of rows returned by this query.
    * This does not yet take into account the restriction clauses, but it will
    * be updated later by camIndexCostEstimate once it inspects the clauses.
    */
    baserel->rows = baserel->tuples;

    baserel->fdw_private = (void *) fdw_plan;
    fdw_plan->remote_conds = NIL;
    fdw_plan->local_conds = NIL;

    ListCell   *lc;
    std::cout << "K2FDW: k2GetForeignRelSizebase restrictinfos: " << list_length(baserel->baserestrictinfo) << std::endl;
    std::cerr << "K2FDW: k2GetForeignRelSizebase restrictinfos: " << list_length(baserel->baserestrictinfo) << std::endl;

    foreach(lc, baserel->baserestrictinfo) {
        RestrictInfo *ri = lfirst_node(RestrictInfo, lc);
        elog(DEBUG4, "FDW: classing baserestrictinfo: %s", nodeToString(ri));
        if (is_foreign_expr(root, baserel, ri->clause))
            fdw_plan->remote_conds = lappend(fdw_plan->remote_conds, ri);
        else
            fdw_plan->local_conds = lappend(fdw_plan->local_conds, ri);
    }
    std::cout << "K2FDW: classified remote_conds: " << list_length(fdw_plan->remote_conds) << std::endl;
    std::cerr << "K2FDW: classified remote_conds: " << list_length(fdw_plan->remote_conds) << std::endl;

    /*
    * Test any indexes of rel for applicability also.
    */

    //check_index_predicates(root, baserel);
    check_partial_indexes(root, baserel);
}

/*
 * k2GetForeignPaths
 *        Create possible access paths for a scan on the foreign table, which is
 *      the full table scan plus available index paths (including the  primary key
 *      scan path if any).
 */
void
k2GetForeignPaths(PlannerInfo *root,
                   RelOptInfo *baserel,
                   Oid foreigntableid) {
    /* Create a ForeignPath node and it as the scan path */
   add_path(root, baserel,
             (Path *) create_foreignscan_path(root,
                                              baserel,
                                              0.001, // From MOT
                                              0.0, // TODO cost
                                              NIL,  /* no pathkeys */
                                              NULL, /* no outer rel either */
                                              NULL, /* no extra plan */
                                              0  /* no options yet */ ));

    /* Add primary key and secondary index paths also */
    create_index_paths(root, baserel);
}

/*
 * k2GetForeignPlan
 *        Create a ForeignScan plan node for scanning the foreign table
 */
ForeignScan *
k2GetForeignPlan(PlannerInfo *root,
                  RelOptInfo *baserel,
                  Oid foreigntableid,
                  ForeignPath *best_path,
                  List *tlist,
                  List *scan_clauses) {
    K2FdwPlanState *fdw_plan_state = (K2FdwPlanState *) baserel->fdw_private;
    Index          scan_relid;
    ListCell       *lc;
    List       *local_exprs = NIL;
    List       *remote_exprs = NIL;

    elog(DEBUG4, "FDW: fdw_private %d remote_conds and %d local_conds for foreign relation %d",
            list_length(fdw_plan_state->remote_conds), list_length(fdw_plan_state->local_conds), foreigntableid);

    if (IS_SIMPLE_REL(baserel))
    {
        scan_relid     = baserel->relid;
        /*
        * Separate the restrictionClauses into those that can be executed remotely
        * and those that can't.  baserestrictinfo clauses that were previously
        * determined to be safe or unsafe are shown in fpinfo->remote_conds and
        * fpinfo->local_conds.  Anything else in the restrictionClauses list will
        * be a join clause, which we have to check for remote-safety.
        */
        elog(DEBUG4, "FDW: GetForeignPlan with %d scan_clauses for simple relation %d", list_length(scan_clauses), scan_relid);
        foreach(lc, scan_clauses)
        {
            RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);
            elog(DEBUG4, "FDW: classifying scan_clause: %s", nodeToString(rinfo));

            /* Ignore pseudoconstants, they are dealt with elsewhere */
            if (rinfo->pseudoconstant)
                continue;

            if (list_member_ptr(fdw_plan_state->remote_conds, rinfo))
                remote_exprs = lappend(remote_exprs, rinfo->clause);
            else if (list_member_ptr(fdw_plan_state->local_conds, rinfo))
                local_exprs = lappend(local_exprs, rinfo->clause);
            else if (is_foreign_expr(root, baserel, rinfo->clause))
                remote_exprs = lappend(remote_exprs, rinfo->clause);
            else
                local_exprs = lappend(local_exprs, rinfo->clause);
        }
        elog(DEBUG4, "FDW: classified %d scan_clauses for relation %d: remote_exprs: %d, local_exprs: %d",
                list_length(scan_clauses), scan_relid, list_length(remote_exprs), list_length(local_exprs));
    }
    else
    {
        /*
         * Join relation or upper relation - set scan_relid to 0.
         */
        scan_relid = 0;
        /*
         * For a join rel, baserestrictinfo is NIL and we are not considering
         * parameterization right now, so there should be no scan_clauses for
         * a joinrel or an upper rel either.
         */
        Assert(!scan_clauses);

        /*
         * Instead we get the conditions to apply from the fdw_private
         * structure.
         */
        remote_exprs = extract_actual_clauses(fdw_plan_state->remote_conds, false);
        local_exprs = extract_actual_clauses(fdw_plan_state->local_conds, false);
    }

    scan_clauses = extract_actual_clauses(scan_clauses, false);

    /* Get the target columns that need to be retrieved from K2 platform */
    // TODO taken from MOT, does it work?
    pull_varattnos((Node*)baserel->reltargetlist, baserel->relid, &fdw_plan_state->target_attrs);
    //pull_varattnos(scan_clauses, baserel->relid, &fdw_plan_state->target_attrs);

    /* Set scan targets. */
    List *target_attrs = NULL;
    bool wholerow = false;
    for (AttrNumber attnum = baserel->min_attr; attnum <= baserel->max_attr; attnum++)
    {
        int bms_idx = attnum - baserel->min_attr + 1;
        if (wholerow || bms_is_member(bms_idx, fdw_plan_state->target_attrs))
        {
            switch (attnum)
            {
                case InvalidAttrNumber:
                    /*
                     * Postgres repurposes InvalidAttrNumber to represent the "wholerow"
                     * junk attribute.
                     */
                    wholerow = true;
                    break;
                    // TODO
                    //case SelfItemPointerAttributeNumber:
                    //case MinTransactionIdAttributeNumber:
                    //case MinCommandIdAttributeNumber:
                    //case MaxTransactionIdAttributeNumber:
                    //case MaxCommandIdAttributeNumber:
                    //ereport(ERROR,
                    //        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg(
                    //                "System column with id %d is not supported yet",
                    //                attnum)));
                    //break;
                    //case TableOidAttributeNumber:
                    /* Nothing to do in K2PG: Postgres will handle this. */
                    //break;
                    //case ObjectIdAttributeNumber:
                default: /* Regular column: attrNum > 0*/
                {
                    TargetEntry *target = makeNode(TargetEntry);
                    target->resno = attnum;
                    target_attrs = lappend(target_attrs, target);
                }
            }
        }
    }

    /* Create the ForeignScan node */
    return make_foreignscan(tlist,  /* target list */
                            scan_clauses,  /* ideally we should use local_exprs here, still use the whole list in case the FDW cannot process some remote exprs*/
                            scan_relid,
                            remote_exprs,    /* expressions K2 may evaluate */
                            target_attrs);  /* fdw_private data for K2 */
                //nullptr,
                //nullptr,
                            //nullptr);
}

static void createCollection() {
    skv::http::dto::CollectionMetadata meta{};
    meta.name = "postgres";
    meta.hashScheme = skv::http::dto::HashScheme::Range;
    meta.storageDriver = skv::http::dto::StorageDriver::K23SI;

    auto reqFut = client->createCollection(std::move(meta), std::vector<String>{""});
    std::cout << "getting createCollection resp!" << std::endl;
    auto&&[status] = reqFut.get();
    if (!status.is2xxOK()) {
        ereport(ERROR,
            (errmodule(MOD_K2),
            errcode(ERRCODE_UNDEFINED_DATABASE),
            errmsg("Could not create K2 collection")));
    }
}

void k2CreateTable(CreateForeignTableStmt* stmt) {
    std::cout << "Start create table" << std::endl;

    if (!initialized) {
        client = new skv::http::Client("172.17.0.1");
        createCollection();
        initialized = true;
    }

    std::cout << "trying to get db name" << std::endl;
    char* dbname = NULL;
    bool failure = false;
    dbname = get_database_name(u_sess->proc_cxt.MyDatabaseId);
    if (dbname == nullptr) {
        ereport(ERROR,
            (errmodule(MOD_K2),
            errcode(ERRCODE_UNDEFINED_DATABASE),
            errmsg("database with OID %u does not exist", u_sess->proc_cxt.MyDatabaseId)));
        return;
    }
    std::cout << "got db name" << std::endl;
    //dbname will be k2 collection name

    skv::http::dto::Schema schema;
    schema.name = std::to_string(stmt->base.relation->foreignOid);
    std::cout << "got schema name" << std::endl;

    schema.version = 0;
    schema.fields.resize(list_length(stmt->base.tableElts) + 2);
    schema.fields[0].name = "__K2_TABLE_NAME__";
    schema.fields[0].type = skv::http::dto::FieldType::STRING;
    // TODO null ordering?
    schema.fields[1].name = "__K2_INDEX_ID__";
    schema.fields[1].type = skv::http::dto::FieldType::INT32T;

    std::cout << "start column iteration" << std::endl;
    ListCell* cell = nullptr;
    int fieldIdx = 2;
    foreach(cell, stmt->base.tableElts) {
        ColumnDef* colDef = (ColumnDef*)lfirst(cell);
        if (colDef == nullptr || colDef->typname == nullptr) {
            ereport(ERROR,
                (errmodule(MOD_K2),
                errcode(ERRCODE_INVALID_COLUMN_DEFINITION),
                errmsg("Column definition is not complete"),
                errdetail("target table is a foreign table")));
            failure = true;
            break;
        }

        // TODO type oid to k2 type table
        int32_t colLen;
        Type tup;
        Form_pg_type typeDesc;
        Oid typoid;
        tup = typenameType(nullptr, colDef->typname, &colLen);
        typeDesc = ((Form_pg_type)GETSTRUCT(tup));
        typoid = HeapTupleGetOid(tup);
        if (typoid == INT4OID) {
            schema.fields[fieldIdx].name = colDef->colname;
            schema.fields[fieldIdx].type = skv::http::dto::FieldType::INT32T;
        } else {
            ereport(ERROR,
                (errmodule(MOD_K2),
                errcode(ERRCODE_INVALID_COLUMN_DEFINITION),
                errmsg("Column type is not supported"),
                errdetail("target table is a foreign table")));
            failure = true;
            break;
        }

        ++fieldIdx;
    }

    std::cout << "create table finishing" << std::endl;
    if (!failure) {
        std::cout << "creating table for foid: " << stmt->base.relation->foreignOid << std::endl;

        schemaCache[stmt->base.relation->foreignOid] = std::make_shared<skv::http::dto::Schema>(std::move(schema));
    }

    // Primary key fields come in separate create index statement
}

void k2CreateIndex(IndexStmt* stmt) {
    std::cout << "create index start" << std::endl;
    auto it = schemaCache.find(stmt->relation->foreignOid);
    auto& schema = it->second;
    std::cout << "creating index for foid: " << stmt->relation->foreignOid << std::endl;
    if (it == schemaCache.end()) {
        ereport(ERROR,
            (errmodule(MOD_K2),
                errcode(ERRCODE_UNDEFINED_TABLE),
                errmsg("Table not found for oid %u", stmt->relation->foreignOid)));
        return;
    }

    if (!stmt->primary) {
        // TODO
        return;
    }


    // TODO check for index expression and error

    std::cout << "create index start column iteration" << std::endl;
    schema->partitionKeyFields.push_back(0);
    schema->partitionKeyFields.push_back(1);
    ListCell* lc = nullptr;
    foreach (lc, stmt->indexParams) {
        IndexElem* ielem = (IndexElem*)lfirst(lc);

        for (size_t i=2; i<schema->fields.size(); ++i) {
            if (schema->fields[i].name == ielem->name) {
                schema->partitionKeyFields.push_back(i);
                if (ielem->nulls_ordering == SORTBY_NULLS_LAST) {
                    schema->fields[i].nullLast = true;
                }
            }
        }
    }

    auto reqFut = client->createSchema("postgres", *schema);
    auto&&[status] = reqFut.get();
    if (!status.is2xxOK()) {
        ereport(ERROR,
            (errmodule(MOD_K2),
            errcode(ERRCODE_UNDEFINED_DATABASE),
            errmsg("Could not create K2 collection")));
    }
    std::cout << "Made a K2 Schema for table: " << it->first << "with " << schema->fields.size()-2 << " fields and " << schema->partitionKeyFields.size() - 2 << " key fields" << std::endl;
}

TupleTableSlot* k2ExecForeignInsert(EState* estate, ResultRelInfo* resultRelInfo, TupleTableSlot* slot, TupleTableSlot* planSlot) {

    Oid foid = RelationGetRelid(resultRelInfo->ri_RelationDesc);
    auto schema = getSchema(foid);
    if (schema == nullptr) {
        reportRC(RCStatus::RC_TABLE_NOT_FOUND, "Schema not found for insert op");
        return nullptr;
    }

    std::cout << "Insert row, got schema: " << schema->name << std::endl;
    skv::http::dto::SKVRecordBuilder build("postgres", schema);

    // HeapTuple srcData = (HeapTuple)slot->tts_tuple;
    TupleDesc tupdesc = slot->tts_tupleDescriptor;
    uint64_t i = 0;
    uint64_t j = 1;
    uint64_t cols = schema->fields.size() - 2;

    build.serializeNext<String>(schema->name);
    // TODO how does a secondary index update happen? This assumes primary index
    build.serializeNext<int32_t>(0);

    for (; i < cols; i++, j++) {
        bool isnull = false;
        std::cout << "trying to get datum for colid: " << j << std::endl;
        Datum value = heap_slot_getattr(slot, j, &isnull);

        if (!isnull) {
            Oid type = tupdesc->attrs[i]->atttypid;
            switch (type) {
                case INT4OID:
                    if (schema->fields[i+2].type != skv::http::dto::FieldType::INT32T) {
                        std::cout << "Types do not match" << std::endl;
                    }
                    int32_t k2val = (int32_t)(value & 0xffffffff);
                    std::cout << "col: " << j << " val: " << k2val << std::endl;
                    build.serializeNext<int32_t>(k2val);
                    break;
            }
        }
    }

    skv::http::dto::SKVRecord record = build.build();
    std::cout << "my record: " << record << std::endl;

    // TODO txn handling
    auto beginFut = client->beginTxn(skv::http::dto::TxnOptions());
    auto&& [status, txnHandle] = beginFut.get();
    if (status.is2xxOK()) {
        auto writeFut = txnHandle.write(record, false, skv::http::dto::ExistencePrecondition::NotExists);
        auto&& [writeStatus] = writeFut.get();
        if (!writeStatus.is2xxOK()) {
            // TODO error types
            reportRC(RCStatus::RC_ERROR, "K2 Write failed");
            return nullptr;
        }
    }
    else {
        reportRC(RCStatus::RC_ERROR, "K2 begin txn failed");
        return nullptr;
    }

    auto endFut = txnHandle.endTxn(skv::http::dto::EndAction::Commit);
    auto&&[endStatus] = endFut.get();
    if (!endStatus.is2xxOK()) {
        // TODO error types
        reportRC(RCStatus::RC_ERROR, "K2 end txn failed");
        return nullptr;
    }

    std::cout << "write was committed!" << std::endl;

    return slot;
}

void k2ValidateTableDef(Node* obj)
{
    if (obj == nullptr) {
        return;
    }

    switch (nodeTag(obj)) {
        case T_AlterTableStmt: {
            ereport(ERROR,
                (errcode(ERRCODE_FDW_OPERATION_NOT_SUPPORTED),
                    errmodule(MOD_K2),
                    errmsg("Alter table operation is not supported for chogori.")));
            break;
        }
        case T_CreateForeignTableStmt: {
            k2CreateTable((CreateForeignTableStmt*)obj);
            break;
        }
        case T_IndexStmt: {
            k2CreateIndex((IndexStmt*) obj);
            break;
        }
        case T_ReindexStmt: {
            ereport(ERROR,
                (errcode(ERRCODE_FDW_OPERATION_NOT_SUPPORTED),
                    errmodule(MOD_K2),
                    errmsg("Reindex is not supported for chogori.")));
            break;
        }
        case T_DropForeignStmt: {
            ereport(ERROR,
                (errcode(ERRCODE_FDW_OPERATION_NOT_SUPPORTED),
                    errmodule(MOD_K2),
                    errmsg("Drop table is not supported for chogori.")));

            break;
        }
        default:
            elog(ERROR, "unrecognized node type: %u", nodeTag(obj));
    }
}

} // ns
