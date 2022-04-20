#include <ostream>
#include <istream>
#include "funcapi.h"
#include "access/reloptions.h"
#include "access/transam.h"
#include "postgres.h"

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
#include "parser/parsetree.h"
#include "access/sysattr.h"

#define K2PG_MAX_SCAN_KEYS (INDEX_MAX_KEYS * 2) /* A pair of lower/upper bounds per column max */
#define FirstBootstrapObjectId 10000            // TODO check if true for og

/*
 * Return true if given object is one of PostgreSQL's built-in objects.
 *
 * We use FirstBootstrapObjectId as the cutoff, so that we only consider
 * objects with hand-assigned OIDs to be "built in", not for instance any
 * function or type defined in the information_schema.
 *
 * Our constraints for dealing with types are tighter than they are for
 * functions or operators: we want to accept only types that are in pg_catalog,
 * else deparse_type_name might incorrectly fail to schema-qualify their names.
 * Thus we must exclude information_schema types.
 *
 * XXX there is a problem with this, which is that the set of built-in
 * objects expands over time.  Something that is built-in to us might not
 * be known to the remote server, if it's of an older version.  But keeping
 * track of that would be a huge exercise.
 */
bool
is_builtin(Oid objectId)
{
	return (objectId < FirstBootstrapObjectId);
}


// Postgres object identifier (OID) defined in Postgres' postgres_ext.h
typedef unsigned int K2PgOid;
#define kInvalidOid ((K2PgOid) 0)

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
// Attribute "querying_colocated_table"
//   - If 'true', SELECT from SQL system catalogs or colocated tables.
//   - Note that the system catalogs are specifically for Postgres API and not K2 SQL
//     system-tables.
typedef struct PgPrepareParameters {
  K2PgOid index_oid;
  bool index_only_scan;
  bool use_secondary_index;
  bool querying_colocated_table;
} K2PgPrepareParameters;

// Structure to hold the execution-control parameters.
typedef struct PgExecParameters {
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
} K2PgExecParameters;

typedef struct K2FdwPlanState
{
   /* Bitmap of attribute (column) numbers that we need to fetch from K2. */
   Bitmapset *target_attrs;
   /*
    * Restriction clauses, divided into safe and unsafe to pushdown subsets.
    */
   List      *remote_conds;
   List      *local_conds;
} K2FdwPlanState;

/*
 * Global context for foreign_expr_walker's search of an expression tree.
 */
typedef struct foreign_glob_cxt
{
   PlannerInfo *root;         /* global planner state */
   RelOptInfo *foreignrel;      /* the foreign relation we are planning for */
} foreign_glob_cxt;

/*
 * Local (per-tree-level) context for foreign_expr_walker's search.
 * This is concerned with identifying collations used in the expression.
 */
typedef enum
{
   FDW_COLLATE_NONE,         /* expression is of a noncollatable type, or
                         * it has default collation that is not
                         * traceable to a foreign Var */
   FDW_COLLATE_SAFE,         /* collation derives from a foreign Var */
   FDW_COLLATE_UNSAFE         /* collation is non-default and derives from
                         * something other than a foreign Var */
} FDWCollateState;

typedef struct foreign_loc_cxt
{
   Oid         collation;      /* OID of current collation, if any */
   FDWCollateState state;      /* state of current collation choice */
} foreign_loc_cxt;

typedef struct FDWColumnRef {
   AttrNumber attr_num;
   int attno;
   int attr_typid;
   int atttypmod;
} FDWColumnRef;

typedef struct FDWConstValue
{
   Oid   atttypid;
   Datum value;
   bool is_null;
} FDWConstValue;

typedef struct FDWExprRefValues
{
   Oid opno;  // PG_OPERATOR OID of the operator
   List *column_refs;
   List *const_values;
   ParamListInfo paramLI; // parameters binding information for prepare statements
    bool column_ref_first;
} FDWExprRefValues;

typedef struct FDWOprCond
{
   Oid opno;  // PG_OPERATOR OID of the operator
   FDWColumnRef *ref; // column reference
   FDWConstValue *val; // column value
    bool column_ref_first;
} FDWOprCond;

typedef struct foreign_expr_cxt {
   List *opr_conds;          /* opr conditions */
} foreign_expr_cxt;

class K2PgStatement;

/*
 * FDW-specific information for ForeignScanState.fdw_state.
 */

typedef struct K2FdwExecState
{
   /* The handle for the internal K2PG Select statement. */
   K2PgStatement*   handle;
   ResourceOwner   stmt_owner;

   Relation index;

   List *remote_exprs;

   /* Oid of the table being scanned */
   Oid tableOid;

   /* Kept query-plan control to pass it to PgGate during preparation */
   K2PgPrepareParameters prepare_params;

   K2PgExecParameters *exec_params; /* execution control parameters for K2 PG */
   bool is_exec_done; /* Each statement should be executed exactly one time */
} K2FdwExecState;

typedef struct K2FdwScanPlanData
{
   /* The relation where to read data from */
   Relation target_relation;

   int nkeys; // number of keys
   int nNonKeys; // number of non-keys

   /* Primary and hash key columns of the referenced table/relation. */
   Bitmapset *primary_key;

   /* Set of key columns whose values will be used for scanning. */
   Bitmapset *sk_cols;

   // ParamListInfo structures are used to pass parameters into the executor for parameterized plans
   ParamListInfo paramLI;

   /* Description and attnums of the columns to bind */
   TupleDesc bind_desc;
   AttrNumber bind_key_attnums[K2PG_MAX_SCAN_KEYS];
   AttrNumber bind_nonkey_attnums[K2PG_MAX_SCAN_KEYS];
} K2FdwScanPlanData;

typedef K2FdwScanPlanData *K2FdwScanPlan;

/*
 * Functions to determine whether an expression can be evaluated safely on
 * remote server.
 */
static bool foreign_expr_walker(Node *node,
               foreign_glob_cxt *glob_cxt,
               foreign_loc_cxt *outer_cxt);


static bool is_foreign_expr(PlannerInfo *root,
            RelOptInfo *baserel,
            Expr *expr);

static bool
is_foreign_expr(PlannerInfo *root,
            RelOptInfo *baserel,
            Expr *expr)
{
   foreign_glob_cxt glob_cxt;
   foreign_loc_cxt loc_cxt;

   /*
    * Check that the expression consists of nodes that are safe to execute
    * remotely.
    */
   glob_cxt.root = root;
   glob_cxt.foreignrel = baserel;

   loc_cxt.collation = InvalidOid;
   loc_cxt.state = FDW_COLLATE_NONE;

   if (!foreign_expr_walker((Node *) expr, &glob_cxt, &loc_cxt))
      return false;

   /*
    * If the expression has a valid collation that does not arise from a
    * foreign var, the expression can not be sent over.
    */
   if (loc_cxt.state == FDW_COLLATE_UNSAFE)
      return false;

   /*
    * An expression which includes any mutable functions can't be sent over
    * because its result is not stable.  For example, sending now() remote
    * side could cause confusion from clock offsets.  Future versions might
    * be able to make this choice with more granularity.  (We check this last
    * because it requires a lot of expensive catalog lookups.)
    */
   if (contain_mutable_functions((Node *) expr))
      return false;

   /* OK to evaluate on the remote server */
   return true;
}

/*
 * Check if expression is safe to execute remotely, and return true if so.
 *
 * In addition, *outer_cxt is updated with collation information.
 *
 * We must check that the expression contains only node types we can deparse,
 * that all types/functions/operators are safe to send (they are "shippable"),
 * and that all collations used in the expression derive from Vars of the
 * foreign table.  Because of the latter, the logic is pretty close to
 * assign_collations_walker() in parse_collate.c, though we can assume here
 * that the given expression is valid.  Note function mutability is not
 * currently considered here.
 */
static bool
foreign_expr_walker(Node *node,
               foreign_glob_cxt *glob_cxt,
               foreign_loc_cxt *outer_cxt)
{
   bool      check_type = true;
   foreign_loc_cxt inner_cxt;
   Oid         collation;
   FDWCollateState state;

   /* Need do nothing for empty subexpressions */
   if (node == NULL)
      return true;

   /* Set up inner_cxt for possible recursion to child nodes */
   inner_cxt.collation = InvalidOid;
   inner_cxt.state = FDW_COLLATE_NONE;

   switch (nodeTag(node))
   {
      case T_Var:
         {
            Var         *var = (Var *) node;

            /*
             * If the Var is from the foreign table, we consider its
             * collation (if any) safe to use.  If it is from another
             * table, we treat its collation the same way as we would a
             * Param's collation, i.e. it's not safe for it to have a
             * non-default collation.
             */
            if (var->varno == glob_cxt->foreignrel->relid &&
               var->varlevelsup == 0)
            {
               /* Var belongs to foreign table */
               collation = var->varcollid;
               state = OidIsValid(collation) ? FDW_COLLATE_SAFE : FDW_COLLATE_NONE;
            }
            else
            {
               /* Var belongs to some other table */
               collation = var->varcollid;
               if (var->varcollid != InvalidOid &&
                  var->varcollid != DEFAULT_COLLATION_OID)
                  return false;

               if (collation == InvalidOid ||
                  collation == DEFAULT_COLLATION_OID)
               {
                  /*
                   * It's noncollatable, or it's safe to combine with a
                   * collatable foreign Var, so set state to NONE.
                   */
                  state = FDW_COLLATE_NONE;
               }
               else
               {
                  /*
                   * Do not fail right away, since the Var might appear
                   * in a collation-insensitive context.
                   */
                  state = FDW_COLLATE_UNSAFE;
               }
            }
         }
         break;
      case T_Const:
         {
            Const      *c = (Const *) node;

            /*
             * If the constant has nondefault collation, either it's of a
             * non-builtin type, or it reflects folding of a CollateExpr.
             * It's unsafe to send to the remote unless it's used in a
             * non-collation-sensitive context.
             */
            collation = c->constcollid;
            if (collation == InvalidOid ||
               collation == DEFAULT_COLLATION_OID)
               state = FDW_COLLATE_NONE;
            else
               state = FDW_COLLATE_UNSAFE;
         }
         break;
      case T_Param:
         {
            Param      *p = (Param *) node;

            /*
             * Collation rule is same as for Consts and non-foreign Vars.
             */
            collation = p->paramcollid;
            if (collation == InvalidOid ||
               collation == DEFAULT_COLLATION_OID)
               state = FDW_COLLATE_NONE;
            else
               state = FDW_COLLATE_UNSAFE;
         }
         break;
      case T_ArrayRef:
         {
            ArrayRef   *ar = (ArrayRef *) node;

            /* Assignment should not be in restrictions. */
            if (ar->refassgnexpr != NULL)
               return false;

            /*
             * Recurse to remaining subexpressions.  Since the array
             * subscripts must yield (noncollatable) integers, they won't
             * affect the inner_cxt state.
             */
            if (!foreign_expr_walker((Node *) ar->refupperindexpr,
                               glob_cxt, &inner_cxt))
               return false;
            if (!foreign_expr_walker((Node *) ar->reflowerindexpr,
                               glob_cxt, &inner_cxt))
               return false;
            if (!foreign_expr_walker((Node *) ar->refexpr,
                               glob_cxt, &inner_cxt))
               return false;

            /*
             * Array subscripting should yield same collation as input,
             * but for safety use same logic as for function nodes.
             */
            collation = ar->refcollid;
            if (collation == InvalidOid)
               state = FDW_COLLATE_NONE;
            else if (inner_cxt.state == FDW_COLLATE_SAFE &&
                   collation == inner_cxt.collation)
               state = FDW_COLLATE_SAFE;
            else if (collation == DEFAULT_COLLATION_OID)
               state = FDW_COLLATE_NONE;
            else
               state = FDW_COLLATE_UNSAFE;
         }
         break;
      case T_FuncExpr:
         {
            FuncExpr   *fe = (FuncExpr *) node;

            /*
             * Recurse to input subexpressions.
             */
            if (!foreign_expr_walker((Node *) fe->args,
                               glob_cxt, &inner_cxt))
               return false;

            /*
             * If function's input collation is not derived from a foreign
             * Var, it can't be sent to remote.
             */
            if (fe->inputcollid == InvalidOid)
                /* OK, inputs are all noncollatable */ ;
            else if (inner_cxt.state != FDW_COLLATE_SAFE ||
                   fe->inputcollid != inner_cxt.collation)
               return false;

            /*
             * Detect whether node is introducing a collation not derived
             * from a foreign Var.  (If so, we just mark it unsafe for now
             * rather than immediately returning false, since the parent
             * node might not care.)
             */
            collation = fe->funccollid;
            if (collation == InvalidOid)
               state = FDW_COLLATE_NONE;
            else if (inner_cxt.state == FDW_COLLATE_SAFE &&
                   collation == inner_cxt.collation)
               state = FDW_COLLATE_SAFE;
            else if (collation == DEFAULT_COLLATION_OID)
               state = FDW_COLLATE_NONE;
            else
               state = FDW_COLLATE_UNSAFE;
         }
         break;
      case T_OpExpr:
      case T_DistinctExpr:   /* struct-equivalent to OpExpr */
         {
            OpExpr      *oe = (OpExpr *) node;

            /*
             * Recurse to input subexpressions.
             */
            if (!foreign_expr_walker((Node *) oe->args,
                               glob_cxt, &inner_cxt))
               return false;

            /*
             * If operator's input collation is not derived from a foreign
             * Var, it can't be sent to remote.
             */
            if (oe->inputcollid == InvalidOid)
                /* OK, inputs are all noncollatable */ ;
            else if (inner_cxt.state != FDW_COLLATE_SAFE ||
                   oe->inputcollid != inner_cxt.collation)
               return false;

            /* Result-collation handling is same as for functions */
            collation = oe->opcollid;
            if (collation == InvalidOid)
               state = FDW_COLLATE_NONE;
            else if (inner_cxt.state == FDW_COLLATE_SAFE &&
                   collation == inner_cxt.collation)
               state = FDW_COLLATE_SAFE;
            else if (collation == DEFAULT_COLLATION_OID)
               state = FDW_COLLATE_NONE;
            else
               state = FDW_COLLATE_UNSAFE;
         }
         break;
      case T_ScalarArrayOpExpr:
         {
            ScalarArrayOpExpr *oe = (ScalarArrayOpExpr *) node;

            /*
             * Recurse to input subexpressions.
             */
            if (!foreign_expr_walker((Node *) oe->args,
                               glob_cxt, &inner_cxt))
               return false;

            /*
             * If operator's input collation is not derived from a foreign
             * Var, it can't be sent to remote.
             */
            if (oe->inputcollid == InvalidOid)
                /* OK, inputs are all noncollatable */ ;
            else if (inner_cxt.state != FDW_COLLATE_SAFE ||
                   oe->inputcollid != inner_cxt.collation)
               return false;

            /* Output is always boolean and so noncollatable. */
            collation = InvalidOid;
            state = FDW_COLLATE_NONE;
         }
         break;
      case T_RelabelType:
         {
            RelabelType *r = (RelabelType *) node;

            /*
             * Recurse to input subexpression.
             */
            if (!foreign_expr_walker((Node *) r->arg,
                               glob_cxt, &inner_cxt))
               return false;

            /*
             * RelabelType must not introduce a collation not derived from
             * an input foreign Var (same logic as for a real function).
             */
            collation = r->resultcollid;
            if (collation == InvalidOid)
               state = FDW_COLLATE_NONE;
            else if (inner_cxt.state == FDW_COLLATE_SAFE &&
                   collation == inner_cxt.collation)
               state = FDW_COLLATE_SAFE;
            else if (collation == DEFAULT_COLLATION_OID)
               state = FDW_COLLATE_NONE;
            else
               state = FDW_COLLATE_UNSAFE;
         }
         break;
      case T_BoolExpr:
         {
            BoolExpr   *b = (BoolExpr *) node;
            switch (b->boolop)
            {
               case AND_EXPR:
                  break;
               case OR_EXPR:  // do not support OR and NOT for now
               case NOT_EXPR:
                  return false;
                  break;
               default:
                  elog(ERROR, "unrecognized boolop: %d", (int) b->boolop);
                  return false;
                  break;
            }
            /*
             * Recurse to input subexpressions.
             */
            if (!foreign_expr_walker((Node *) b->args,
                               glob_cxt, &inner_cxt))
               return false;

            /* Output is always boolean and so noncollatable. */
            collation = InvalidOid;
            state = FDW_COLLATE_NONE;
         }
         break;
      case T_NullTest:
         {
            NullTest   *nt = (NullTest *) node;

            /*
             * Recurse to input subexpressions.
             */
            if (!foreign_expr_walker((Node *) nt->arg,
                               glob_cxt, &inner_cxt))
               return false;

            /* Output is always boolean and so noncollatable. */
            collation = InvalidOid;
            state = FDW_COLLATE_NONE;
         }
         break;
      case T_ArrayExpr:
         {
            ArrayExpr  *a = (ArrayExpr *) node;

            /*
             * Recurse to input subexpressions.
             */
            if (!foreign_expr_walker((Node *) a->elements,
                               glob_cxt, &inner_cxt))
               return false;

            /*
             * ArrayExpr must not introduce a collation not derived from
             * an input foreign Var (same logic as for a function).
             */
            collation = a->array_collid;
            if (collation == InvalidOid)
               state = FDW_COLLATE_NONE;
            else if (inner_cxt.state == FDW_COLLATE_SAFE &&
                   collation == inner_cxt.collation)
               state = FDW_COLLATE_SAFE;
            else if (collation == DEFAULT_COLLATION_OID)
               state = FDW_COLLATE_NONE;
            else
               state = FDW_COLLATE_UNSAFE;
         }
         break;
      case T_List:
         {
            List      *l = (List *) node;
            ListCell   *lc;

            /*
             * Recurse to component subexpressions.
             */
            foreach(lc, l)
            {
               if (!foreign_expr_walker((Node *) lfirst(lc),
                                  glob_cxt, &inner_cxt))
                  return false;
            }

            /*
             * When processing a list, collation state just bubbles up
             * from the list elements.
             */
            collation = inner_cxt.collation;
            state = inner_cxt.state;

            /* Don't apply exprType() to the list. */
            check_type = false;
         }
         break;
      case T_Aggref:
         {
	   return false;
         }
         break;
      default:

         /*
          * If it's anything else, assume it's unsafe.  This list can be
          * expanded later, but don't forget to add deparse support below.
          */
         return false;
   }

   /*
    * If result type of given expression is not built-in, it can't be sent to
    * remote because it might have incompatible semantics on remote side.
    */
   if (check_type && !is_builtin(exprType(node)))
      return false;

   /*
    * Now, merge my collation information into my parent's state.
    */
   if (state > outer_cxt->state)
   {
      /* Override previous parent state */
      outer_cxt->collation = collation;
      outer_cxt->state = state;
   }
   else if (state == outer_cxt->state)
   {
      /* Merge, or detect error if there's a collation conflict */
      switch (state)
      {
         case FDW_COLLATE_NONE:
            /* Nothing + nothing is still nothing */
            break;
         case FDW_COLLATE_SAFE:
            if (collation != outer_cxt->collation)
            {
               /*
                * Non-default collation always beats default.
                */
               if (outer_cxt->collation == DEFAULT_COLLATION_OID)
               {
                  /* Override previous parent state */
                  outer_cxt->collation = collation;
               }
               else if (collation != DEFAULT_COLLATION_OID)
               {
                  /*
                   * Conflict; show state as indeterminate.  We don't
                   * want to "return false" right away, since parent
                   * node might not care about collation.
                   */
                  outer_cxt->state = FDW_COLLATE_UNSAFE;
               }
            }
            break;
         case FDW_COLLATE_UNSAFE:
            /* We're still conflicted ... */
            break;
      }
   }

   /* It looks OK */
   return true;
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
   elog(WARNING, "K2FDW: k2GetForeignRelSize %d base restrictinfos for relation %d", list_length(baserel->baserestrictinfo), baserel->relid);

   foreach(lc, baserel->baserestrictinfo)
   {
      RestrictInfo *ri = lfirst_node(RestrictInfo, lc);
      elog(DEBUG4, "FDW: classing baserestrictinfo: %s", nodeToString(ri));
      if (is_foreign_expr(root, baserel, ri->clause))
         fdw_plan->remote_conds = lappend(fdw_plan->remote_conds, ri);
      else
         fdw_plan->local_conds = lappend(fdw_plan->local_conds, ri);
   }
   elog(WARNING, "K2FDW: classified %d remote_conds, %d local_conds", list_length(fdw_plan->remote_conds), list_length(fdw_plan->local_conds));

   /*
    * Test any indexes of rel for applicability also.
    */

   //check_index_predicates(root, baserel);
   check_partial_indexes(root, baserel);
}
