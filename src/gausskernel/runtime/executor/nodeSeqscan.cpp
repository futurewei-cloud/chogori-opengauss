/* -------------------------------------------------------------------------
 *
 * nodeSeqscan.cpp
 *	  Support routines for sequential scans of relations.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/runtime/executor/nodeSeqscan.cpp
 *
 * -------------------------------------------------------------------------
 *
 * INTERFACE ROUTINES
 *		ExecSeqScan				sequentially scans a relation.
 *		ExecSeqNext				retrieve next tuple in sequential order.
 *		ExecInitSeqScan			creates and initializes a seqscan node.
 *		ExecEndSeqScan			releases any storage allocated.
 *		ExecReScanSeqScan		rescans the relation
 *		ExecSeqMarkPos			marks scan position
 *		ExecSeqRestrPos			restores scan position
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/relscan.h"
#include "access/tableam.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_operator.h"
#include "commands/cluster.h"
#include "executor/exec/execdebug.h"
#include "executor/node/nodeModifyTable.h"
#include "executor/node/nodeSamplescan.h"
#include "executor/node/nodeSeqscan.h"
#include "pgxc/redistrib.h"
#include "miscadmin.h"
#include "storage/buf/bufmgr.h"
#include "storage/tcap.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/snapmgr.h"
#include "nodes/execnodes.h"
#include "nodes/makefuncs.h"
#include "optimizer/pruning.h"
#include "optimizer/restrictinfo.h"
#include "parser/parsetree.h"
#include "access/ustore/knl_uheap.h"
#include "access/ustore/knl_uscan.h"

#include "optimizer/var.h"
#include "optimizer/tlist.h"

#include <vector>
#include <set>
#include <map>
#include "access/k2/k2pg_aux.h"

struct K2ColumnRef {
   AttrNumber attr_num;
   int attno;
   int attr_typid;
   int atttypmod;
};

struct K2ConstValue
{
   Oid   atttypid;
   int attlen;
   bool attbyval;
   Datum value;
   bool is_null;
};

struct K2ExprRefValues
{
   Oid opno;  // PG_OPERATOR OID of the operator
   RegProcedure opfunc_id; // PG func oid
   List *column_refs;
   List *const_values;
   ParamListInfo paramLI; // parameters binding information for prepare statements
};

static std::unordered_map<int, StrategyNumber> operator_to_strategy_map =
{
    {INT48EQOID, BTEqualStrategyNumber},
    {BooleanEqualOperator, BTEqualStrategyNumber},
    {CHAREQOID, BTEqualStrategyNumber},
    {INT2EQOID, BTEqualStrategyNumber},
    {INT4EQOID, BTEqualStrategyNumber},
    {TEXTEQOID, BTEqualStrategyNumber},
    {INT8EQOID, BTEqualStrategyNumber},
    {INT84EQOID, BTEqualStrategyNumber},
    {INT42EQOID, BTEqualStrategyNumber},
    {FLOAT4EQOID, BTEqualStrategyNumber},
    {INT1EQOID, BTEqualStrategyNumber},
    {FLOAT8EQOID, BTEqualStrategyNumber},
    {TIMETZEQOID, BTEqualStrategyNumber},
    {FLOAT48EQOID, BTEqualStrategyNumber},
    {FLOAT84EQOID, BTEqualStrategyNumber},
    {INTERVALEQOID, BTEqualStrategyNumber},
    {NUMEQOID, BTEqualStrategyNumber},
    {NUMERICEQOID, BTEqualStrategyNumber},
    {INT28EQOID, BTEqualStrategyNumber},
    {INT82EQOID, BTEqualStrategyNumber},
    {TIMESTAMPEQOID, BTEqualStrategyNumber},
    {BPCHAREQOID, BTEqualStrategyNumber},
    {DATEEQOID, BTEqualStrategyNumber},
    {INT48LTOID, BTLessStrategyNumber},
    {INT2LTOID, BTLessStrategyNumber},
    {INT4LTOID, BTLessStrategyNumber},
    {INT8LTOID, BTLessStrategyNumber},
    {INT84LTOID, BTLessStrategyNumber},
    {INT24LTOID, BTLessStrategyNumber},
    {INT42LTOID, BTLessStrategyNumber},
    {FLOAT4LTOID, BTLessStrategyNumber},
    {TEXTLTOID, BTLessStrategyNumber},
    {FLOAT8LTOID, BTLessStrategyNumber},
    {BPCHARLTOID, BTLessStrategyNumber},
    {DATELTOID, BTLessStrategyNumber},
    {FLOAT48LTOID, BTLessStrategyNumber},
    {FLOAT84LTOID, BTLessStrategyNumber},
    {TIMESTAMPTZLTOID, BTLessStrategyNumber},
    {NUMERICLTOID, BTLessStrategyNumber},
    {INT28LTOID, BTLessStrategyNumber},
    {INT82LTOID, BTLessStrategyNumber},
    {TIMESTAMPLTOID, BTLessStrategyNumber},
    {INT48LEOID, BTLessEqualStrategyNumber},
    {INT8LEOID, BTLessEqualStrategyNumber},
    {INT84LEOID, BTLessEqualStrategyNumber},
    {INT2LEOID, BTLessEqualStrategyNumber},
    {INT4LEOID, BTLessEqualStrategyNumber},
    {INT24LEOID, BTLessEqualStrategyNumber},
    {INT42LEOID, BTLessEqualStrategyNumber},
    {FLOAT4LEOID, BTLessEqualStrategyNumber},
    {FLOAT8LEOID, BTLessEqualStrategyNumber},
    {DATELEOID, BTLessEqualStrategyNumber},
    {FLOAT48LEOID, BTLessEqualStrategyNumber},
    {FLOAT84LEOID, BTLessEqualStrategyNumber},
    {TIMESTAMPTZLEOID, BTLessEqualStrategyNumber},
    {INTERVALEQOID, BTLessEqualStrategyNumber},
    {NUMERICLEOID, BTLessEqualStrategyNumber},
    {INT28LEOID, BTLessEqualStrategyNumber},
    {INT82LEOID, BTLessEqualStrategyNumber},
    {TIMESTAMPLEOID, BTLessEqualStrategyNumber},
    {INT48GTOID, BTGreaterStrategyNumber},
    {INT8GTOID, BTGreaterStrategyNumber},
    {INT84GTOID, BTGreaterStrategyNumber},
    {INT2GTOID, BTGreaterStrategyNumber},
    {INT4GTOID, BTGreaterStrategyNumber},
    {INT24GTOID, BTGreaterStrategyNumber},
    {INT42GTOID, BTGreaterStrategyNumber},
    {FLOAT4GTOID, BTGreaterStrategyNumber},
    {TEXTGTOID, BTGreaterStrategyNumber},
    {FLOAT8GTOID, BTGreaterStrategyNumber},
    {BPCHARGTOID, BTGreaterStrategyNumber},
    {DATEGTOID, BTGreaterStrategyNumber},
    {FLOAT48GTOID, BTGreaterStrategyNumber},
    {FLOAT84GTOID, BTGreaterStrategyNumber},
    {TIMESTAMPTZGTOID, BTGreaterStrategyNumber},
    {NUMERICGTOID, BTGreaterStrategyNumber},
    {INT28GTOID, BTGreaterStrategyNumber},
    {INT82GTOID, BTGreaterStrategyNumber},
    {TIMESTAMPGTOID, BTGreaterStrategyNumber},
    {INT48GEOID, BTGreaterEqualStrategyNumber},
    {INT8GEOID, BTGreaterEqualStrategyNumber},
    {INT84GEOID, BTGreaterEqualStrategyNumber},
    {INT2GEOID, BTGreaterEqualStrategyNumber},
    {INT4GEOID, BTGreaterEqualStrategyNumber},
    {INT24GEOID, BTGreaterEqualStrategyNumber},
    {INT42GEOID, BTGreaterEqualStrategyNumber},
    {FLOAT4GEOID, BTGreaterEqualStrategyNumber},
    {FLOAT8GEOID, BTGreaterEqualStrategyNumber},
    {DATEGEOID, BTGreaterEqualStrategyNumber},
    {FLOAT48GEOID, BTGreaterEqualStrategyNumber},
    {FLOAT84GEOID, BTGreaterEqualStrategyNumber},
    {TIMESTAMPTZGEOID, BTGreaterEqualStrategyNumber},
    {NUMERICGEOID, BTGreaterEqualStrategyNumber},
    {INT28GEOID, BTGreaterEqualStrategyNumber},
    {INT82GEOID, BTGreaterEqualStrategyNumber},
    {TIMESTAMPGEOID, BTGreaterEqualStrategyNumber}
};

static StrategyNumber get_strategy_number(int operator_no) {
    auto result = operator_to_strategy_map.find(operator_no);
    if (result == operator_to_strategy_map.end()) {
        return InvalidStrategy;
    }
    return result->second;
}

std::vector<ScanKeyData> parse_conditions(List *exprs, ParamListInfo paramLI);

void parse_expr(Expr *node,  K2ExprRefValues *ref_values);

void parse_op_expr(OpExpr *node, K2ExprRefValues *ref_values);

void parse_relable_type(RelabelType *node, K2ExprRefValues *ref_values);

void parse_var(Var *node, K2ExprRefValues *ref_values);

void parse_const(Const *node, K2ExprRefValues *ref_values);

void parse_param(Param *node, K2ExprRefValues *ref_values);

extern void StrategyGetRingPrefetchQuantityAndTrigger(BufferAccessStrategy strategy, int* quantity, int* trigger);
/* ----------------------------------------------------------------
 *		prefetch_pages
 *
 *		1,Offset and last here are used as signed  quantities that can be less than 0
 *		2,Put a kludge here to make the <0 check work, to prevent propagating a negative
 *		  number of pages to PageRangePrefetch()
 * ----------------------------------------------------------------
 */
void prefetch_pages(TableScanDesc scan, BlockNumber start_block, BlockNumber offset)
{
    /* Open it at the smgr level if not already done */
    PageRangePrefetch(scan->rs_rd, MAIN_FORKNUM, start_block, offset, 0, 0);

    return;
}

/* ----------------------------------------------------------------
 *		Start_Prefetch
 *
 *		1,Prefetch interfaces for Sequential Scans
 *		2,Start_Prefetch() calculates the optimal prefetch distance and invokes
 *		  prefetch_pages() to invoke the prefetch interface
 *		3,only the ADIO prefetch is provided here now
 * ----------------------------------------------------------------
 */
void Start_Prefetch(TableScanDesc scan, SeqScanAccessor* p_accessor, ScanDirection dir)
{
    BlockNumber start_pref, offset, last;
    uint32 quantity, trigger;
    bool forward = false;

    /* Disable prefetch on unsupported scans */
    if (p_accessor == NULL)
        return;  // cannot prefetch this scan

    if (scan->rs_nblocks == 0)
        return;

    quantity = p_accessor->sa_prefetch_quantity;
    trigger = p_accessor->sa_prefetch_trigger;
    last = p_accessor->sa_last_prefbf;
    forward = ScanDirectionIsForward(dir);

    if (last == InvalidBlockNumber) { // first prefetch
        if (forward) {
            if (scan->rs_cblock == InvalidBlockNumber) {
                start_pref = scan->rs_startblock + 1;
            } else {
                start_pref = scan->rs_cblock + 1;
            }

            offset = 2 * quantity;
            if (start_pref + offset > scan->rs_nblocks) {
                offset = scan->rs_nblocks - start_pref;
            }

            if (((int32)offset) > 0) {
                prefetch_pages(scan, start_pref, offset);
                last = start_pref + offset - 1;
            } else
                return;  // nothing to prefetch
        } else {
            offset = 2 * quantity;
            if (scan->rs_cblock == InvalidBlockNumber) {
                start_pref = scan->rs_nblocks;
            } else {
                start_pref = scan->rs_cblock;
            }

            if (start_pref > offset)
                start_pref = start_pref - offset;
            else {
                offset = start_pref;
                start_pref = 0;
            }
            if (((int32)offset) > 0) {
                prefetch_pages(scan, start_pref, offset);
                last = start_pref;
            } else
                return;  // nothing to prefetch
        }
    } else {
        if (scan->rs_cblock == InvalidBlockNumber)
            return;  // do nothing
        if (forward) {
            if (last >= scan->rs_nblocks - 1)
                return;  // nothing to prefetch

            if (scan->rs_cblock >= last - trigger) {
                start_pref = last + 1;
                offset = quantity;
                if (start_pref + offset > scan->rs_nblocks) {
                    offset = scan->rs_nblocks - start_pref;
                }

                prefetch_pages(scan, start_pref, offset);
                last = start_pref + offset - 1;
            }
        } else {
            if (((int32)last) <= 0)
                return;  // nothing to prefetch
            if (scan->rs_cblock < last + trigger) {
                start_pref = last - quantity;
                offset = quantity;
                if ((int32)start_pref < 0) {
                    start_pref = 0;
                    offset = last;
                }
                prefetch_pages(scan, start_pref, offset);
                last = start_pref;
            }
        }
    }
    if (last != p_accessor->sa_last_prefbf) {
        if (forward) {
            ereport(DEBUG1,
                (errmodule(MOD_ADIO),
                    errmsg("start forward prefetch for %s last pref(%u), current block(%u)",
                        RelationGetRelationName(scan->rs_rd),
                        last,
                        scan->rs_cblock)));
        } else {
            ereport(DEBUG1,
                (errmodule(MOD_ADIO),
                    errmsg("start backword prefetch for %s last pref(%u), current block(%u)",
                        RelationGetRelationName(scan->rs_rd),
                        last,
                        scan->rs_cblock)));
        }
    }
    p_accessor->sa_last_prefbf = last;
}

static TupleTableSlot* SeqNext(SeqScanState* node);

static void ExecInitNextPartitionForSeqScan(SeqScanState* node);

/* ----------------------------------------------------------------
 *						Scan Support
 * ----------------------------------------------------------------
 */
/* ----------------------------------------------------------------
 *		SeqNext
 *
 *		This is a workhorse for ExecSeqScan
 * ----------------------------------------------------------------
 */
static TupleTableSlot* SeqNext(SeqScanState* node)
{
    Tuple tuple;
    TableScanDesc scanDesc;
    EState* estate = NULL;
    ScanDirection direction;
    TupleTableSlot* slot = NULL;

    /*
     * get information from the estate and scan state
     */
    scanDesc = node->ss_currentScanDesc;
    estate = node->ps.state;
    direction = estate->es_direction;
    slot = node->ss_ScanTupleSlot;

    GetTableScanDesc(scanDesc, node->ss_currentRelation)->rs_ss_accessor = node->ss_scanaccessor;

    /*
     * get the next tuple from the table for seqscan.
     */
    tuple = scan_handler_tbl_getnext(scanDesc, direction, node->ss_currentRelation);

    ADIO_RUN()
    {
        Start_Prefetch(GetTableScanDesc(scanDesc, node->ss_currentRelation), node->ss_scanaccessor, direction);
    }
    ADIO_END();

    /*
     * save the tuple and the buffer returned to us by the access methods in
     * our scan tuple slot and return the slot.  Note: we pass 'false' because
     * tuples returned by heap_getnext() are pointers onto disk pages and were
     * not created with palloc() and so should not be pfree_ext()'d.  Note also
     * that ExecStoreTuple will increment the refcount of the buffer; the
     * refcount will not be dropped until the tuple table slot is cleared.
     */
    return ExecMakeTupleSlot(tuple, GetTableScanDesc(scanDesc, node->ss_currentRelation), slot, node->ss_currentRelation->rd_tam_type);
}

/*
 * SeqRecheck -- access method routine to recheck a tuple in EvalPlanQual
 */
static bool SeqRecheck(SeqScanState* node, TupleTableSlot* slot)
{
    /*
     * Note that unlike IndexScan, SeqScan never use keys in heap_beginscan
     * (and this is very bad) - so, here we do not check are keys ok or not.
     */
    return true;
}

/* ----------------------------------------------------------------
 *		ExecSeqScan(node)
 *
 *		Scans the relation sequentially and returns the next qualifying
 *		tuple.
 *		We call the ExecScan() routine and pass it the appropriate
 *		access method functions.
 * ----------------------------------------------------------------
 */
TupleTableSlot* ExecSeqScan(SeqScanState* node)
{
    return ExecScan((ScanState*)node, node->ScanNextMtd, (ExecScanRecheckMtd)SeqRecheck);
}

/* ----------------------------------------------------------------
 *		SeqScan_Pref_Quantity
 *
 *		1,do init prefetch quantity and prefetch trigger for adio
 *		2,prefetch quantity can not exceed NBuffers / 4
 *		3,we set prefetch trigger equal to prefetch quantity, later we can use smart preftch policy,
 *		  if used ring policy, we need check ring size and correct prefetch quantity and trigger
 * ----------------------------------------------------------------
 */
void SeqScan_Pref_Quantity(TableScanDesc scan, SeqScanAccessor* p_accessor, Relation relation)
{
    int threshold = (g_instance.attr.attr_storage.NBuffers / 4);
    int prefetch_trigger = u_sess->attr.attr_storage.prefetch_quantity;

    p_accessor->sa_prefetch_quantity = (uint32)rtl::min(threshold, prefetch_trigger);
    p_accessor->sa_prefetch_trigger = p_accessor->sa_prefetch_quantity;

    if (scan == NULL) {
        return;
    }
    BufferAccessStrategy bas = GetTableScanDesc(scan, relation)->rs_strategy;
    if (bas != NULL) {
        StrategyGetRingPrefetchQuantityAndTrigger(
            bas, (int*)&p_accessor->sa_prefetch_quantity, (int*)&p_accessor->sa_prefetch_trigger);
    }
}

/* ----------------------------------------------------------------
 *		SeqScan_Init
 *
 *		API funtion to init prefetch quantity and prefetch trigger for adio
 * ----------------------------------------------------------------
 */
void SeqScan_Init(TableScanDesc scan, SeqScanAccessor* p_accessor, Relation relation)
{
    p_accessor->sa_last_prefbf = InvalidBlockNumber;
    p_accessor->sa_pref_trigbf = InvalidBlockNumber;
    SeqScan_Pref_Quantity(scan, p_accessor, relation);
}

RangeScanInRedis reset_scan_qual(Relation curr_heap_rel, ScanState* node, bool isRangeScanInRedis)
{
    if (node == NULL) {
        return {isRangeScanInRedis, 0, 0};
    }
    if (u_sess->attr.attr_sql.enable_cluster_resize && RelationInRedistribute(curr_heap_rel)) {
        List* new_qual = eval_ctid_funcs(curr_heap_rel, node->ps.plan->qual, &node->rangeScanInRedis);
        node->ps.qual = (List*)ExecInitExpr((Expr*)new_qual, (PlanState*)&node->ps);
        node->ps.qual_is_inited = true;
    } else if (!node->ps.qual_is_inited) {
        node->ps.qual = (List*)ExecInitExpr((Expr*)node->ps.plan->qual, (PlanState*)&node->ps);
        node->ps.qual_is_inited = true;
        node->rangeScanInRedis = {false,0,0};
    }
    return node->rangeScanInRedis;
}

static TableScanDesc InitBeginScan(SeqScanState* node, Relation current_relation)
{
    TableScanDesc current_scan_desc = NULL;
    Snapshot scanSnap;

    /*
     * Choose user-specified snapshot if TimeCapsule clause exists, otherwise
     * estate->es_snapshot instead.
     */
    scanSnap = TvChooseScanSnap(current_relation, (Scan *)node->ps.plan, (ScanState *)node);
    if (IsK2PgRelation(current_relation)) {
        std::vector<ScanKeyData> skeys = std::move(parse_conditions(node->ps.plan->qual, node->ps.state->es_param_list_info));
        if (!skeys.empty()) {
            elog(DEBUG1, "InitBeginScan with %lu skeys", skeys.size());
            current_scan_desc = scan_handler_tbl_beginscan(current_relation,
                scanSnap, skeys.size(), skeys.data(), (ScanState*)node);
        } else {
            current_scan_desc = scan_handler_tbl_beginscan(current_relation,
                scanSnap, 0, NULL, (ScanState*)node);
        }
    } else {
        if (!node->isSampleScan) {
            current_scan_desc = scan_handler_tbl_beginscan(current_relation,
                scanSnap, 0, NULL, (ScanState*)node);
        } else {
            current_scan_desc = InitSampleScanDesc((ScanState*)node, current_relation);
        }
    }

    return current_scan_desc;
}

TableScanDesc BeginScanRelation(SeqScanState* node, Relation relation, TransactionId relfrozenxid64, int eflags)
{
    Snapshot scanSnap;
    TableScanDesc current_scan_desc = NULL;
    bool isUstoreRel = RelationIsUstoreFormat(relation);

    /*
     * Choose user-specified snapshot if TimeCapsule clause exists, otherwise
     * estate->es_snapshot instead.
     */
    scanSnap = TvChooseScanSnap(relation, (Scan *)node->ps.plan, (ScanState *)node);

    if (isUstoreRel) {
        /*
         * Verify if a DDL operation that froze all tuples in the relation
         * occured after taking the snapshot. Skip for explain only commands.
         */
        if (!(eflags & EXEC_FLAG_EXPLAIN_ONLY)) {
            if (TransactionIdPrecedes(FirstNormalTransactionId, scanSnap->xmax) &&
                !TransactionIdIsCurrentTransactionId(relfrozenxid64) &&
                TransactionIdPrecedes(scanSnap->xmax, relfrozenxid64)) {
                ereport(ERROR,
                        (errcode(ERRCODE_SNAPSHOT_INVALID),
                         (errmsg("Snapshot too old."))));
            }
        }

        if (!node->isPartTbl) {
            (void)reset_scan_qual(relation, node);
        }

        current_scan_desc = UHeapBeginScan(relation, scanSnap, 0);
    } else {
        current_scan_desc = InitBeginScan(node, relation);
    }

    return current_scan_desc;
}

/* ----------------------------------------------------------------
 *		InitScanRelation
 *
 *		This does the initialization for scan relations and
 *		subplans of scans.
 * ----------------------------------------------------------------
 */
void InitScanRelation(SeqScanState* node, EState* estate, int eflags)
{
    Relation current_relation;
    Relation current_part_rel = NULL;
    SeqScan* plan = NULL;
    bool is_target_rel = false;
    LOCKMODE lockmode = AccessShareLock;
    TableScanDesc current_scan_desc = NULL;
    bool isUstoreRel = false;

    is_target_rel = ExecRelationIsTargetRelation(estate, ((SeqScan*)node->ps.plan)->scanrelid);

    /*
     * get the relation object id from the relid'th entry in the range table,
     * open that relation and acquire appropriate lock on it.
     */
    current_relation = ExecOpenScanRelation(estate, ((SeqScan*)node->ps.plan)->scanrelid);

    isUstoreRel = RelationIsUstoreFormat(current_relation);

    /*
     * tuple table initialization
     */
    ExecInitResultTupleSlot(estate, &node->ps, current_relation->rd_tam_type);
    ExecInitScanTupleSlot(estate, node, current_relation->rd_tam_type);

    if (((Scan*)node->ps.plan)->tablesample && node->sampleScanInfo.tsm_state == NULL) {
        if (isUstoreRel) {
            node->sampleScanInfo.tsm_state = New(CurrentMemoryContext) UstoreTableSample((ScanState*)node);
        } else {
            node->sampleScanInfo.tsm_state = New(CurrentMemoryContext) RowTableSample((ScanState*)node);
        }
    }

    if (!node->isPartTbl) {
        /* add qual for redis */
        TransactionId relfrozenxid64 = getRelationRelfrozenxid(current_relation);
        current_scan_desc = BeginScanRelation(node, current_relation, relfrozenxid64, eflags);
    } else {
        plan = (SeqScan*)node->ps.plan;

        /* initiate partition list */
        node->partitions = NULL;

        if (!is_target_rel) {
            lockmode = AccessShareLock;
        } else {
            switch (estate->es_plannedstmt->commandType) {
                case CMD_UPDATE:
                case CMD_DELETE:
                case CMD_MERGE:
                    lockmode = RowExclusiveLock;
                    break;

                case CMD_SELECT:
                    lockmode = AccessShareLock;
                    break;

                default:
                    ereport(ERROR,
                        (errcode(ERRCODE_INVALID_OPERATION),
                            errmodule(MOD_EXECUTOR),
                            errmsg("invalid operation %d on partition for seqscan, allowed are UPDATE/DELETE/SELECT",
                                estate->es_plannedstmt->commandType)));
                    break;
            }
        }
        node->lockMode = lockmode;
        /* Generate node->partitions if exists */
        if (plan->itrs > 0) {
            Partition part = NULL;
            PruningResult* resultPlan = NULL;

            if (plan->pruningInfo->expr != NULL) {
                resultPlan = GetPartitionInfo(plan->pruningInfo, estate, current_relation);
            } else {
                resultPlan = plan->pruningInfo;
            }

            ListCell* cell = NULL;
            List* part_seqs = resultPlan->ls_rangeSelectedPartitions;

            foreach (cell, part_seqs) {
                Oid tablepartitionid = InvalidOid;
                int partSeq = lfirst_int(cell);

                tablepartitionid = getPartitionOidFromSequence(current_relation, partSeq);
                part = partitionOpen(current_relation, tablepartitionid, lockmode);
                node->partitions = lappend(node->partitions, part);
            }
            if (resultPlan->ls_rangeSelectedPartitions != NULL) {
                node->part_id = resultPlan->ls_rangeSelectedPartitions->length;
            } else {
                node->part_id = 0;
            }
        }

        if (node->partitions != NIL) {
            /* construct HeapScanDesc for first partition */
            Partition currentPart = (Partition)list_nth(node->partitions, 0);
            current_part_rel = partitionGetRelation(current_relation, currentPart);
            node->ss_currentPartition = current_part_rel;

            /* add qual for redis */
            TransactionId relfrozenxid64 = getPartitionRelfrozenxid(current_part_rel);
            current_scan_desc = BeginScanRelation(node, current_part_rel, relfrozenxid64, eflags);
        } else {
            node->ss_currentPartition = NULL;
            node->ps.qual = (List*)ExecInitExpr((Expr*)node->ps.plan->qual, (PlanState*)&node->ps);
        }
    }
    node->ss_currentRelation = current_relation;
    node->ss_currentScanDesc = current_scan_desc;

    ExecAssignScanType(node, RelationGetDescr(current_relation));
}

static inline void InitSeqNextMtd(SeqScan* node, SeqScanState* scanstate)
{
    if (!node->tablesample) {
        scanstate->ScanNextMtd = SeqNext;
    } else {
        if (RELATION_OWN_BUCKET(scanstate->ss_currentRelation)) {
            scanstate->ScanNextMtd = HbktSeqSampleNext;
            return;
        }
        scanstate->ScanNextMtd = SeqSampleNext;
    }
}

/*
 * Extract column numbers (var numbers) from a flattened targetlist
 * Set valid found columns to false (not null) in boolArr
 */
static inline void FlatTLtoBool(const List* targetList, bool* boolArr, AttrNumber natts)
{
    ListCell* tl = NULL;
    foreach (tl, targetList) {
        GenericExprState* gstate = (GenericExprState*)lfirst(tl);
        Var* variable = (Var*)gstate->xprstate.expr;
        Assert(variable != NULL); /* if this happens we've messed up */
        if ((variable->varoattno > 0) && (variable->varoattno <= natts)) {
            boolArr[variable->varoattno - 1] = false; /* sometimes varattno in parent is different */
        }
        elog(DEBUG1, "PMZ-InitSeq FlatTLtoBool: varoattno: %d", variable->varoattno);
    }
}

std::vector<ScanKeyData> parse_conditions(List *exprs, ParamListInfo paramLI) {
    elog(INFO, "SeqScan parsing %d remote expressions", list_length(exprs));
    ListCell   *lc;
    std::vector<ScanKeyData> result;
    foreach(lc, exprs)
    {
        Expr *expr = (Expr *) lfirst(lc);

        /* Extract clause from RestrictInfo, if required */
        if (IsA(expr, RestrictInfo)) {
            expr = ((RestrictInfo *) expr)->clause;
        }
        elog(INFO, "SeqScan parsing expression: %s", nodeToString(expr));
        // parse a single clause
        K2ExprRefValues ref_values;
        ref_values.column_refs = NIL;
        ref_values.const_values = NIL;
        ref_values.paramLI = paramLI;
        parse_expr(expr, &ref_values);
        if (list_length(ref_values.column_refs) == 1 && list_length(ref_values.const_values) == 1) {
            ScanKeyData skey;
            K2ColumnRef *col_ref = (K2ColumnRef *)linitial(ref_values.column_refs);
            K2ConstValue *const_value = (K2ConstValue *)linitial(ref_values.const_values);
            StrategyNumber strategy_no = get_strategy_number(ref_values.opno);
            if (strategy_no!= InvalidStrategy) {
                elog(INFO, "SeqScan using strategy number %d for operator: %d", strategy_no, ref_values.opno);
                ScanKeyInit(&skey, col_ref->attr_num, strategy_no, ref_values.opfunc_id, const_value->value);
                result.push_back(std::move(skey));
            } else {
                elog(WARNING, "Unsupported operator for SeqScan: %d", ref_values.opno);
            }
        }
    }
    return result;
}

void parse_expr(Expr *node,  K2ExprRefValues *ref_values) {
    if (node == NULL)
        return;

    switch (nodeTag(node))
    {
        case T_Var:
            parse_var((Var *) node, ref_values);
            break;
        case T_Const:
            parse_const((Const *) node, ref_values);
            break;
        case T_OpExpr:
            parse_op_expr((OpExpr *) node, ref_values);
            break;
        case T_Param:
            parse_param((Param *) node, ref_values);
            break;
        case T_RelabelType:
            parse_relable_type((RelabelType *)node, ref_values);
            break;
        default:
            elog(INFO, "K2: unsupported expression type for expr: %s", nodeToString(node));
            break;
    }
}

void parse_op_expr(OpExpr *node, K2ExprRefValues *ref_values) {
    if (list_length(node->args) != 2) {
        elog(INFO, "K2: we only handle binary opclause, actual args length: %d for node %s", list_length(node->args), nodeToString(node));
        return;
    } else {
        elog(DEBUG1, "K2: handing binary opclause for node %s", nodeToString(node));
    }

    StrategyNumber strategy_no = get_strategy_number(node->opno);
    if (strategy_no!= InvalidStrategy) {
        ref_values->opno = node->opno;
        ref_values->opfunc_id = node->opfuncid;
        ListCell *lc;
        foreach(lc, node->args)
        {
            Expr *arg = (Expr *) lfirst(lc);
            parse_expr(arg, ref_values);
        }
    } else {
        elog(WARNING, "Unsupported operator for SeqScan: %d", node->opno);
    }
}

void parse_relable_type(RelabelType *node, K2ExprRefValues *ref_values) {
    elog(DEBUG1, "K2: parsing RelabelType %s", nodeToString(node));
    // the condition is at the current level
    parse_expr(node->arg, ref_values);
}

void parse_var(Var *node, K2ExprRefValues *ref_values) {
    elog(DEBUG1, "K2: parsing Var %s", nodeToString(node));
    // the condition is at the current level
    if (node->varlevelsup == 0) {
        K2ColumnRef *col_ref = (K2ColumnRef *)palloc0(sizeof(K2ColumnRef));
        col_ref->attno = node->varno;
        col_ref->attr_num = node->varattno;
        col_ref->attr_typid = node->vartype;
        col_ref->atttypmod = node->vartypmod;
        ref_values->column_refs = lappend(ref_values->column_refs, col_ref);
    }
}

void parse_const(Const *node, K2ExprRefValues *ref_values) {
    elog(DEBUG1, "K2: parsing Const %s", nodeToString(node));
    K2ConstValue *val = (K2ConstValue *)palloc0(sizeof(K2ConstValue));
    val->atttypid = node->consttype;
    val->is_null = node->constisnull;
    val->attlen = node->constlen;
    val->attbyval = node->constbyval;

    val->value = 0;
    if (node->constisnull || node->constbyval)
        val->value = node->constvalue;
    else
        val->value = PointerGetDatum(node->constvalue);

    ref_values->const_values = lappend(ref_values->const_values, val);
}

void parse_param(Param *node, K2ExprRefValues *ref_values) {
    elog(DEBUG1, "K2: parsing Param %s", nodeToString(node));
    ParamExternData *prm = NULL;
    prm = &ref_values->paramLI->params[node->paramid - 1];

    if (!OidIsValid(prm->ptype) ||
        prm->ptype != node->paramtype ||
        !(prm->pflags & PARAM_FLAG_CONST))
    {
        /* Planner should ensure this does not happen */
        elog(ERROR, "Invalid parameter: %s", nodeToString(node));
    }

    K2ConstValue *val = (K2ConstValue *)palloc0(sizeof(K2ConstValue));
    val->atttypid = prm->ptype;
    val->is_null = prm->isnull;
    int16        typLen = 0;
    bool        typByVal = false;
    val->value = 0;

    get_typlenbyval(node->paramtype, &typLen, &typByVal);
    val->attlen = typLen;
    val->attbyval = typByVal;
    if (prm->isnull || typByVal)
        val->value = prm->value;
    else
        val->value = datumCopy(prm->value, typByVal, typLen);

    ref_values->const_values = lappend(ref_values->const_values, val);
}


/*
 * Given a seq scan node's quals, extract all valid column numbers
 * Set valid found columns to false (not null) in boolArr
 */
uint16 TraverseVars(List* qual, bool* boolArr, int natts, List* vars, ListCell* l)
{
    uint16 foundvars = 0;
    foreach (l, vars) {
        Var* var = (Var*)lfirst(l);
        AttrNumber varoattno = var->varoattno;
        if ((varoattno > 0) && (varoattno <= natts)) {
            elog(DEBUG1, "PMZ-newQual: qual found varoattno: %d", varoattno);
            boolArr[varoattno - 1] = false;
            foundvars++;
        } else {
            elog(DEBUG1, "PMZ-newQual: rejecting varoattno: %d", varoattno);
        }
    }
    return foundvars;
}

static inline bool QualtoBool(List* qual, bool* boolArr, int natts)
{
    List* vars = NIL;
    ListCell* l = NULL;
    vars = pull_var_clause((Node*)qual, PVC_RECURSE_AGGREGATES, PVC_RECURSE_PLACEHOLDERS);
    if (TraverseVars(qual, boolArr, natts, vars, l) > 0)
        return true;
    return false;
}

/* create a new flattened tlist from targetlist, if valid then set attributes in boolArr */
static inline void TLtoBool(List* targetlist, bool* boolArr, AttrNumber natts)
{
    PVCAggregateBehavior myaggbehavior = PVC_RECURSE_AGGREGATES;
    PVCPlaceHolderBehavior myphbehavior = PVC_RECURSE_PLACEHOLDERS;
    List* flatlist = NULL;
    flatlist = flatten_tlist(targetlist, myaggbehavior, myphbehavior);
    if ((flatlist != NULL) && (flatlist->length > 0)) {
        FlatTLtoBool(flatlist, boolArr, natts);
    }
}

/* return total count of non-null attributes */
static inline int BoolNatts(const bool* boolArr, AttrNumber natts)
{
    AttrNumber result = 0;
    for (int i = 0; i < natts; i++) {
        if (boolArr[i] == false) {
            result++;
        }
    }
    return result;
}

/* calulate position of last column (non-inclusive) */
static inline AttrNumber LastVarPos(bool* boolArr, AttrNumber natts)
{
    AttrNumber lastVar = natts - 1;
    while ((boolArr[lastVar] == true) && (lastVar > -1)) {
        lastVar--;
    }
    lastVar++;
    return lastVar;
}

/* get simple vars in projinfo and set corresponding bool in boolArr */
static inline void SimpleVartoBool(bool* boolArr, AttrNumber natts, ProjectionInfo* psProjInfo)
{
    for (int i = 0; i<psProjInfo->pi_numSimpleVars; i++) {
        if ((psProjInfo->pi_varNumbers[i] > 0) && (psProjInfo->pi_varNumbers[i] <= natts)) {
            boolArr[psProjInfo->pi_varNumbers[i] - 1] = false;
        }
    }
}

static inline bool CheckSeqScanFallback(const AttrNumber natts, const AttrNumber selectAtts, const AttrNumber lastVar)
{
    return ((selectAtts > natts / 2) || (selectAtts == 0) || (lastVar >= (natts * 7 / 10)) || (lastVar <= 0));
}

/* ----------------------------------------------------------------
 *		ExecInitSeqScan
 * ----------------------------------------------------------------
 */
SeqScanState* ExecInitSeqScan(SeqScan* node, EState* estate, int eflags)
{
    TableSampleClause* tsc = NULL;
    int rc;

    /*
     * Once upon a time it was possible to have an outerPlan of a SeqScan, but
     * not any more.
     */
    Assert(outerPlan(node) == NULL);
    Assert(innerPlan(node) == NULL);

    /*
     * create state structure
     */
    SeqScanState* scanstate = makeNode(SeqScanState);

    scanstate->ps.plan = (Plan*)node;
    scanstate->ps.state = estate;
    scanstate->isPartTbl = node->isPartTbl;
    scanstate->currentSlot = 0;
    scanstate->partScanDirection = node->partScanDirection;
    scanstate->rangeScanInRedis = {false,0,0};

    if (!node->tablesample) {
        scanstate->isSampleScan = false;
    } else {
        scanstate->isSampleScan = true;
        tsc = node->tablesample;
    }

    /*
     * Miscellaneous initialization
     *
     * create expression context for node
     */
    ExecAssignExprContext(estate, &scanstate->ps);

    /*
     * initialize child expressions
     */
    scanstate->ps.targetlist = (List*)ExecInitExpr((Expr*)node->plan.targetlist, (PlanState*)scanstate);

    if (node->tablesample) {
        scanstate->sampleScanInfo.args = (List*)ExecInitExpr((Expr*)tsc->args, (PlanState*)scanstate);
        scanstate->sampleScanInfo.repeatable = ExecInitExpr(tsc->repeatable, (PlanState*)scanstate);

        scanstate->sampleScanInfo.sampleType = tsc->sampleType;
    }

    /*
     * tuple table initialization
     */
    ExecInitResultTupleSlot(estate, &scanstate->ps);
    ExecInitScanTupleSlot(estate, scanstate);

    InitScanRelation(scanstate, estate, eflags);

    ADIO_RUN()
    {
        /* add prefetch related information */
        scanstate->ss_scanaccessor = (SeqScanAccessor*)palloc(sizeof(SeqScanAccessor));
        SeqScan_Init(scanstate->ss_currentScanDesc, scanstate->ss_scanaccessor, scanstate->ss_currentRelation);
    }
    ADIO_END();

    /*
     * initialize scan relation
     */
    InitSeqNextMtd(node, scanstate);
    if (IsValidScanDesc(scanstate->ss_currentScanDesc)) {
        scan_handler_tbl_init_parallel_seqscan(scanstate->ss_currentScanDesc,
            scanstate->ps.plan->dop, scanstate->partScanDirection);
    } else {
        scanstate->ps.stubType = PST_Scan;
    }

    scanstate->ps.ps_TupFromTlist = false;

    /*
     * Initialize result tuple type and projection info.
     */
    ExecAssignResultTypeFromTL(
            &scanstate->ps,
            scanstate->ss_currentRelation->rd_tam_type);

    ExecAssignScanProjectionInfo(scanstate);

    AttrNumber natts = scanstate->ss_ScanTupleSlot->tts_tupleDescriptor->natts;
    AttrNumber lastVar = -1;
    bool *isNullProj = NULL;

    /* if scanstate or scantupleslot are NULL immediately fallback */
    if ((scanstate == NULL) || (scanstate->ss_ScanTupleSlot == NULL))
        goto fallback;

    /* if partial sequential scan is disabled by GUC, or table is not ustore, or partition table, skip */
    if ((!u_sess->attr.attr_storage.enable_ustore_partial_seqscan) ||
        (scanstate->ss_ScanTupleSlot->tts_tupleDescriptor->tdTableAmType != TAM_USTORE) ||
        (scanstate->ss_currentScanDesc == NULL) || (scanstate->ss_currentScanDesc->rs_rd == NULL) ||
        (!RelationIsNonpartitioned(scanstate->ss_currentScanDesc->rs_rd)) ||
        (RelationIsPartition(scanstate->ss_currentScanDesc->rs_rd))) {
        elog(DEBUG1, "Partial seq scan disabled by GUC, or table is not UStore, or table is/has partition");
        goto fallback;
    }

    /* Get number of attributes (natts), allocate null array, initialize all as null  */

    elog(DEBUG1, "PMZ-InitScan: preparing scan on relation with %d total atts", natts);
    isNullProj = (bool *)palloc(sizeof(bool) * natts);
    rc = memset_s(isNullProj, natts, 1, natts);
    securec_check(rc, "\0", "\0");

    if ((scanstate->ps.plan->flatList != NULL) && (scanstate->ps.plan->flatList->length > 0)) {
        if (scanstate->ps.ps_ProjInfo != NULL) {
            TLtoBool(scanstate->ps.plan->targetlist, isNullProj, natts);
            SimpleVartoBool(isNullProj, natts, scanstate->ps.ps_ProjInfo);
        }

        /* likely got here from ExecInitAgg, expand flatList that was passed down */
        FlatTLtoBool(scanstate->ps.plan->flatList, isNullProj, natts);
        if (scanstate->ps.plan->targetlist->length < natts)
            if (scanstate->ps.plan->targetlist->length > scanstate->ps.plan->flatList->length) {
                FlatTLtoBool(scanstate->ps.plan->targetlist, isNullProj, natts); /* parent unaware of 'HAVING' clause */
            }

        if ((scanstate->ps.plan->qual != NULL) && (scanstate->ps.plan->qual->length > 0)) /* query has qualifications */
            if (QualtoBool(scanstate->ps.plan->qual, isNullProj, natts) == false) {
                elog(DEBUG1, "PMZ-InitScan-wfl: triggering fallback because QualtoBool failed");
                goto fallback;
            }
        lastVar = LastVarPos(isNullProj, natts);
        AttrNumber selectAtts = BoolNatts(isNullProj, natts);
        elog(DEBUG1, "PMZ-InitScan-wfl: selectAtts=%d , lastVar=%d", selectAtts, lastVar);

        if (CheckSeqScanFallback(natts, selectAtts, lastVar)) {
            elog(DEBUG1, "PMZ-InitScan-wfl: fallback because no selected atts, or writing over 5/10 of atts, or "
                         "reading over 7/10 of src tuple");
            goto fallback;
        }

        scanstate->ss_currentScanDesc->lastVar = lastVar;
        scanstate->ss_currentScanDesc->boolArr = isNullProj;
        elog(DEBUG1, "PMZ-InitScan-wfl: partial seq scan with sort/aggregation,  %d selected atts, and lastVar %d",
            selectAtts, lastVar);
    /* ProjInfo may be null if parent node is aggregate */
    } else if (scanstate->ps.ps_ProjInfo != NULL) {
        /* a basic seq scan node may contain FuncExpr in tlist so check targetlist */
        if (scanstate->ps.ps_ProjInfo->pi_numSimpleVars > 0) {
            TLtoBool(scanstate->ps.plan->targetlist, isNullProj, natts);
            /* if query has quals then parse and add them */
            if ((scanstate->ps.plan->qual != NULL) && (scanstate->ps.plan->qual->length > 0)) {
                if (QualtoBool(scanstate->ps.plan->qual, isNullProj, natts) == false) {
                    elog(DEBUG1, "PMZ-InitScan-simple: triggering fallback because QualtoBool failed");
                    goto fallback;
                }
            }
            SimpleVartoBool(isNullProj, natts, scanstate->ps.ps_ProjInfo);
            lastVar = LastVarPos(isNullProj, natts);
            AttrNumber selectAtts = BoolNatts(isNullProj, natts);
            elog(DEBUG1, "PMZ-InitScan-simple: selectAtts=%d , lastVar=%d", selectAtts, lastVar);

            if (CheckSeqScanFallback(natts, selectAtts, lastVar)) {
                elog(DEBUG1, "PMZ-InitScan-simple: fallback because no selected atts, or writing over 5/10 of atts, or "
                             "reading over 7/10 of src tuple");
                goto fallback;
            }

            scanstate->ss_currentScanDesc->lastVar = lastVar;
            scanstate->ss_currentScanDesc->boolArr = isNullProj;

            elog(DEBUG1, "PMZ-InitScan-simple: regular seq scan with %d simple vars, %d selected atts, and lastVar %d",
                scanstate->ps.ps_ProjInfo->pi_numSimpleVars, selectAtts, lastVar);
        } else {
            elog(DEBUG1, "PMZ-InitScan-simple: triggering fallback because no simple vars");
            goto fallback;
        }
    } else {
        /* usually SELECT * (no flatlist from parent or proj info or sufficiently selective targetlist) */
        elog(DEBUG1,
            "PMZ-InitScan: fallback because no parent flatlist or proj info (probably SELECT *)");
    fallback:
        if (isNullProj != NULL) {
            pfree(isNullProj);
        }
        elog(DEBUG1, "PMZ-InitScan: using fallback mode for seq scan");
        if (scanstate->ss_currentScanDesc != NULL) {
            if (scanstate->ss_currentScanDesc->rs_rd != NULL &&
                RELATION_CREATE_BUCKET(scanstate->ss_currentScanDesc->rs_rd)) {
            } else {
                scanstate->ss_currentScanDesc->lastVar = -1;
                scanstate->ss_currentScanDesc->boolArr = NULL;
            }
        } else {
            ((Plan *) node)->flatList = NULL;
            elog(DEBUG1, "PMZ-InitScan: No scan descriptor");
        }
    }

    return scanstate;
}

/* ----------------------------------------------------------------
 *		ExecEndSeqScan
 *
 *		frees any storage allocated through C routines.
 * ----------------------------------------------------------------
 */
void ExecEndSeqScan(SeqScanState* node)
{
    Relation relation;
    TableScanDesc scanDesc;

    /*
     * get information from node
     */
    relation = node->ss_currentRelation;
    scanDesc = node->ss_currentScanDesc;

    /*
     * Free the exprcontext
     */
    ExecFreeExprContext(&node->ps);

    /*
     * clean out the tuple table
     */
    (void)ExecClearTuple(node->ps.ps_ResultTupleSlot);
    (void)ExecClearTuple(node->ss_ScanTupleSlot);

    /*
     * close heap scan
     */
    if (scanDesc != NULL) {
        scan_handler_tbl_endscan((TableScanDesc)scanDesc);
    }
    if (node->isPartTbl) {
        if (PointerIsValid(node->partitions)) {
            Assert(node->ss_currentPartition);
            releaseDummyRelation(&(node->ss_currentPartition));
            releasePartitionList(node->ss_currentRelation, &(node->partitions), NoLock);
        }
    }

    ADIO_RUN()
    {
        /* add prefetch related information */
        pfree_ext(node->ss_scanaccessor);
    }
    ADIO_END();

    /*
     * close the heap relation.
     */
    if (relation) {
        ExecCloseScanRelation(relation);
    }
}

/* ----------------------------------------------------------------
 *						Join Support
 * ----------------------------------------------------------------
 */
/* ----------------------------------------------------------------
 *		ExecReScanSeqScan
 *
 *		Rescans the relation.
 * ----------------------------------------------------------------
 */
void ExecReScanSeqScan(SeqScanState* node)
{
    TableScanDesc scan;

    if (node->isSampleScan) {
        /* Remember we need to do BeginSampleScan again (if we did it at all) */
        (((RowTableSample*)node->sampleScanInfo.tsm_state)->resetSampleScan)();
    }

    scan = node->ss_currentScanDesc;
    if (node->isPartTbl) {
        if (PointerIsValid(node->partitions)) {
            /* end scan the prev partition first, */
            scan_handler_tbl_endscan(scan);

            /* finally init Scan for the next partition */
            ExecInitNextPartitionForSeqScan(node);
            scan = node->ss_currentScanDesc;
        }
    } else {
        scan_handler_tbl_rescan(scan, NULL, node->ss_currentRelation);
    }

    if ((scan != NULL) && (scan->rs_rd->rd_tam_type == TAM_USTORE)) {
        scan->lastVar = -1;
        scan->boolArr = NULL;
    }
    scan_handler_tbl_init_parallel_seqscan(scan, node->ps.plan->dop, node->partScanDirection);
    ExecScanReScan((ScanState*)node);
}

/* ----------------------------------------------------------------
 *		ExecSeqMarkPos(node)
 *
 *		Marks scan position.
 * ----------------------------------------------------------------
 */
void ExecSeqMarkPos(SeqScanState* node)
{
    scan_handler_tbl_markpos(node->ss_currentScanDesc);
}

/* ----------------------------------------------------------------
 *		ExecSeqRestrPos
 *
 *		Restores scan position.
 * ----------------------------------------------------------------
 */
void ExecSeqRestrPos(SeqScanState* node)
{
    /*
     * Clear any reference to the previously returned tuple.  This is needed
     * because the slot is simply pointing at scan->rs_cbuf, which
     * heap_restrpos will change; we'd have an internally inconsistent slot if
     * we didn't do this.
     */
    (void)ExecClearTuple(node->ss_ScanTupleSlot);

    scan_handler_tbl_restrpos(node->ss_currentScanDesc);
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		: This does the initialization for scan partition
 * Description	:
 * Input		:
 * Output	:
 * Notes		:
 */
static void ExecInitNextPartitionForSeqScan(SeqScanState* node)
{
    Partition currentpartition = NULL;
    Relation currentpartitionrel = NULL;

    int paramno = -1;
    ParamExecData* param = NULL;
    SeqScan* plan = NULL;

    plan = (SeqScan*)node->ps.plan;

    /* get partition sequnce */
    paramno = plan->plan.paramno;
    param = &(node->ps.state->es_param_exec_vals[paramno]);
    node->currentSlot = (int)param->value;

    /* construct HeapScanDesc for new partition */
    currentpartition = (Partition)list_nth(node->partitions, node->currentSlot);
    currentpartitionrel = partitionGetRelation(node->ss_currentRelation, currentpartition);

    releaseDummyRelation(&(node->ss_currentPartition));
    node->ss_currentPartition = currentpartitionrel;

    /* add qual for redis */

    /* update partition scan-related fileds in SeqScanState  */
    node->ss_currentScanDesc = InitBeginScan(node, currentpartitionrel);

    ADIO_RUN()
    {
        SeqScan_Init(node->ss_currentScanDesc, node->ss_scanaccessor, currentpartitionrel);
    }
    ADIO_END();
}
