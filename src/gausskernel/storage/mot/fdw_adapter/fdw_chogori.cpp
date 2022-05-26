#include <cstdint>
#include <iostream>
#include <string>
#include <unordered_map>

#include "global.h"
#include "funcapi.h"
#include "access/reloptions.h"
#include "access/transam.h"
#include "mot_fdw_error.h"
#include "postgres.h"

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

#include "utils/selfuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/print.h"
#include "parser/parsetree.h"
#include "parser/parse_coerce.h"
#include "utils/extended_statistics.h"
#include "utils/syscache.h"


#include <skv/client/SKVClient.h>

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

class PgStatement;
typedef PgStatement *K2PgStatement;

/*
 * FDW-specific information for ForeignScanState.fdw_state.
 */

typedef struct K2FdwExecState
{
   /* The handle for the internal K2PG Select statement. */
   K2PgStatement   handle;
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

   /* Set of key columns whose values will be used for scanning. */
   Bitmapset *sk_cols;

   // ParamListInfo structures are used to pass parameters into the executor for parameterized plans
   ParamListInfo paramLI;

   /* Description and attnums of the columns to bind */
   TupleDesc bind_desc;

  std::shared_ptr<skv::http::dto::Schema> schema;
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

static void parse_conditions(List *exprs, ParamListInfo paramLI, foreign_expr_cxt *expr_cxt);

static void parse_expr(Expr *node, FDWExprRefValues *ref_values);

static void parse_op_expr(OpExpr *node, FDWExprRefValues *ref_values);

static void parse_var(Var *node, FDWExprRefValues *ref_values);

static void parse_const(Const *node, FDWExprRefValues *ref_values);

static void parse_param(Param *node, FDWExprRefValues *ref_values);


using skv::http::String;
// Foreign Oid -> Schema
static std::unordered_map<uint64_t, std::shared_ptr<skv::http::dto::Schema>> schemaCache;
static skv::http::Client client;
static bool initialized=false;

std::shared_ptr<skv::http::dto::Schema> getSchema(Oid foid) {
  auto it = schemaCache.find(foid);
  std::cout << "trying to get schema for foid: " << foid << std::endl;
  if (it == schemaCache.end()) {
    String schemaName = std::to_string(foid);
    
    auto&&[status, schemaResp] = client.getSchema("postgres", schemaName).get();
    if (!status.is2xxOK()) {    
      return nullptr;
    }

    schemaCache[foid] = std::make_shared<skv::http::dto::Schema>(schemaResp);
    it = schemaCache.find(foid);
  }

  return it->second;
}

/*
 * Get k2Sql-specific table metadata and load it into the scan_plan.
 * Currently only the hash and primary key info.
 */
static void LoadTableInfo(Oid foid, K2FdwScanPlan scan_plan)
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
static void parse_conditions(List *exprs, ParamListInfo paramLI, foreign_expr_cxt *expr_cxt) {
	elog(DEBUG4, "FDW: parsing %d remote expressions", list_length(exprs));
	ListCell   *lc;
	foreach(lc, exprs)
	{
		Expr	   *expr = (Expr *) lfirst(lc);

		/* Extract clause from RestrictInfo, if required */
		if (IsA(expr, RestrictInfo)) {
			expr = ((RestrictInfo *) expr)->clause;
		}
		elog(DEBUG4, "FDW: parsing expression: %s", nodeToString(expr));
		// parse a single clause
		FDWExprRefValues ref_values;
		ref_values.column_refs = NIL;
		ref_values.const_values = NIL;
		ref_values.paramLI = paramLI;
		parse_expr(expr, &ref_values);
		if (list_length(ref_values.column_refs) == 1 && list_length(ref_values.const_values) == 1) {
			FDWOprCond *opr_cond = (FDWOprCond *)palloc0(sizeof(FDWOprCond));
			opr_cond->opno = ref_values.opno;
			// found a binary condition
			ListCell   *rlc;
			foreach(rlc, ref_values.column_refs) {
				opr_cond->ref = (FDWColumnRef *)lfirst(rlc);
			}

			foreach(rlc, ref_values.const_values) {
				opr_cond->val = (FDWConstValue *)lfirst(rlc);
			}

            opr_cond->column_ref_first = ref_values.column_ref_first;

			expr_cxt->opr_conds = lappend(expr_cxt->opr_conds, opr_cond);
		}
	}
}

static void parse_expr(Expr *node, FDWExprRefValues *ref_values) {
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
		default:
			elog(DEBUG4, "FDW: unsupported expression type for expr: %s", nodeToString(node));
			break;
	}
}

static void parse_op_expr(OpExpr *node, FDWExprRefValues *ref_values) {
	if (list_length(node->args) != 2) {
		elog(DEBUG4, "FDW: we only handle binary opclause, actual args length: %d for node %s", list_length(node->args), nodeToString(node));
		return;
	} else {
		elog(DEBUG4, "FDW: handing binary opclause for node %s", nodeToString(node));
	}

	ListCell *lc;
    bool checkOrder;
	switch (get_oprrest(node->opno))
	{
		case F_EQSEL: //  equal =
		case F_SCALARLTSEL: // Less than <
          // TODO not supported by OG?
          //case F_SCALARLESEL: // Less Equal <=
		case F_SCALARGTSEL: // Greater than >
          //case F_SCALARGESEL: // Greater Equal >=
			elog(DEBUG4, "FDW: parsing OpExpr: %d", get_oprrest(node->opno));

            // Creating the FDWExprRefValues loses the tree structure of the original expression
            // so we need to keep track if the column reference or the constant was first
            checkOrder = true;

			ref_values->opno = node->opno;
			foreach(lc, node->args)
			{
				Expr *arg = (Expr *) lfirst(lc);
				parse_expr(arg, ref_values);

                if (checkOrder && list_length(ref_values->column_refs) == 1) {
                    ref_values->column_ref_first = true;
                } else if (checkOrder) {
                    ref_values->column_ref_first = false;
                }
                checkOrder = false;
			}

			break;
		default:
			elog(DEBUG4, "FDW: unsupported OpExpr type: %d", get_oprrest(node->opno));
			break;
	}
}

static void parse_var(Var *node, FDWExprRefValues *ref_values) {
	elog(DEBUG4, "FDW: parsing Var %s", nodeToString(node));
	// the condition is at the current level
	if (node->varlevelsup == 0) {
		FDWColumnRef *col_ref = (FDWColumnRef *)palloc0(sizeof(FDWColumnRef));
		col_ref->attno = node->varno;
		col_ref->attr_num = node->varattno;
		col_ref->attr_typid = node->vartype;
		col_ref->atttypmod = node->vartypmod;
		ref_values->column_refs = lappend(ref_values->column_refs, col_ref);
	}
}

static void parse_const(Const *node, FDWExprRefValues *ref_values) {
	elog(DEBUG4, "FDW: parsing Const %s", nodeToString(node));
	FDWConstValue *val = (FDWConstValue *)palloc0(sizeof(FDWConstValue));
	val->atttypid = node->consttype;
	val->is_null = node->constisnull;

	val->value = 0;
	if (node->constisnull || node->constbyval)
		val->value = node->constvalue;
	else
		val->value = PointerGetDatum(node->constvalue);

	ref_values->const_values = lappend(ref_values->const_values, val);
}

static void parse_param(Param *node, FDWExprRefValues *ref_values) {
	elog(DEBUG4, "FDW: parsing Param %s", nodeToString(node));
	ParamExternData *prm = NULL;
	ParamExternData prmdata;
    // TODO
	//if (ref_values->paramLI->paramFetch != NULL)
	//	prm = ref_values->paramLI->paramFetch(ref_values->paramLI, node->paramid,
	//			true, &prmdata);
	//else
		prm = &ref_values->paramLI->params[node->paramid - 1];

	if (!OidIsValid(prm->ptype) ||
		prm->ptype != node->paramtype ||
		!(prm->pflags & PARAM_FLAG_CONST))
	{
		/* Planner should ensure this does not happen */
		elog(ERROR, "Invalid parameter: %s", nodeToString(node));
	}

	FDWConstValue *val = (FDWConstValue *)palloc0(sizeof(FDWConstValue));
	val->atttypid = prm->ptype;
	val->is_null = prm->isnull;
	int16		typLen = 0;
	bool		typByVal = false;
	val->value = 0;

	get_typlenbyval(node->paramtype, &typLen, &typByVal);
	if (prm->isnull || typByVal)
		val->value = prm->value;
	else
		val->value = datumCopy(prm->value, typByVal, typLen);

	ref_values->const_values = lappend(ref_values->const_values, val);
}

// search for the column in the equal conditions, the performance is fine for small number of equal conditions
static List *findOprCondition(foreign_expr_cxt context, int attr_num) {
	List * result = NIL;
	ListCell *lc = NULL;
	foreach (lc, context.opr_conds) {
		FDWOprCond *first = (FDWOprCond *) lfirst(lc);
		if (first->ref->attr_num == attr_num) {
			result = lappend(result, first);
		}
	}

	return result;
}

static constexpr uint32_t K2_FIELD_OFFSET = 2;
skv::http::dto::FieldType PGTypidToK2Type(Oid) {
  return skv::http::dto::FieldType::INT32T;
}

skv::http::dto::expression::Expression build_expr(K2FdwScanPlan scan_plan, FDWOprCond *opr_cond) {
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

    expression::Value col_ref = expression::makeValueReference(scan_plan->schema->fields[K2_FIELD_OFFSET + opr_cond->ref->attr_num].name);
    // TODO k2 types, null?
 	int32_t k2val = (int32_t)(opr_cond->val->value & 0xffffffff);
    expression::Value constant = expression::makeValueLiteral<int32_t>(std::move(k2val));
    opr_expr.valueChildren.push_back(std::move(col_ref));
    opr_expr.valueChildren.push_back(std::move(constant));
    
	return opr_expr;
}

static void BindScanKeys(Relation relation,
							K2FdwExecState *fdw_state,
							K2FdwScanPlan scan_plan) {
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
	for (int i = 0; i < nKeys; i++)
	{
        // check if the key is in the Opr conditions
        List *opr_conds = findOprCondition(context, i);
        if (opr_conds != NIL) {
            elog(DEBUG4, "FDW: binding key with attr_num %d for relation: %d", i, relation->rd_id);
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
	//													fdw_state->handle,
	//													fdw_state->stmt_owner);

	Expression where_conds{};
	// Top level should be an "AND" node
    where_conds.op = Operation::AND;
    int nNonKeys = scan_plan->schema->fields.size() - nKeys - K2_FIELD_OFFSET;
	// bind non-keys
	for (int i = nKeys; i < nNonKeys + nNonKeys; i++)
	{
		// check if the column is in the Opr conditions
		List *opr_conds = findOprCondition(context, i);
		if (opr_conds != NIL) {
			elog(DEBUG4, "FDW: binding key with attr_num %d for relation: %d", i, relation->rd_id);
			ListCell *lc = NULL;
			foreach (lc, opr_conds) {
				FDWOprCond *opr_cond = (FDWOprCond *) lfirst(lc);
				Expression arg = build_expr(scan_plan, opr_cond);
                if (arg.op != Operation::UNKNOWN) {
                    // use primary keys as range condition
                  where_conds.expressionChildren.push_back(std::move(arg));
                }
			}
		}
	}

	//HandleK2PgStatusWithOwner(PgGate_DmlBindWhereConds(fdw_state->handle, where_conds),
	//													fdw_state->handle,
	//													fdw_state->stmt_owner);
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

   foreach(lc, baserel->baserestrictinfo)
   {
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
 *		Create possible access paths for a scan on the foreign table, which is
 *      the full table scan plus available index paths (including the  primary key
 *      scan path if any).
 */
void
k2GetForeignPaths(PlannerInfo *root,
				   RelOptInfo *baserel,
				   Oid foreigntableid)
{
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
 *		Create a ForeignScan plan node for scanning the foreign table
 */
ForeignScan *
k2GetForeignPlan(PlannerInfo *root,
				  RelOptInfo *baserel,
				  Oid foreigntableid,
				  ForeignPath *best_path,
				  List *tlist,
		 List *scan_clauses)
{
	K2FdwPlanState *fdw_plan_state = (K2FdwPlanState *) baserel->fdw_private;
	Index          scan_relid;
	ListCell       *lc;
	List	   *local_exprs = NIL;
	List	   *remote_exprs = NIL;

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
					//		        "System column with id %d is not supported yet",
					//		        attnum)));
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
  skv::http::dto::CollectionMetadata meta;
  meta.name = "postgres";
  meta.hashScheme = skv::http::dto::HashScheme::Range;
  meta.storageDriver = skv::http::dto::StorageDriver::K23SI;
 
    auto reqFut = client.createCollection(std::move(meta), std::vector<String>{"",""});
    auto&&[status] = reqFut.get();
    if (!status.is2xxOK()) {
	ereport(ERROR,
	    (errmodule(MOD_MOT),
		errcode(ERRCODE_UNDEFINED_DATABASE),
		errmsg("Could not create K2 collection")));
    }
}

void k2CreateTable(CreateForeignTableStmt* stmt) {
  std::cout << "Start create table" << std::endl;

  if (!initialized) {
    createCollection();
  }
  
    char* dbname = NULL;
    bool failure = false;
    dbname = get_database_name(u_sess->proc_cxt.MyDatabaseId);
    if (dbname == nullptr) {
	ereport(ERROR,
	    (errmodule(MOD_MOT),
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
	    (errmodule(MOD_MOT),
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
	    (errmodule(MOD_MOT),
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
            (errmodule(MOD_MOT),
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

	for (int i=2; i<schema->fields.size(); ++i) {
	  if (schema->fields[i].name == ielem->name) {
	    schema->partitionKeyFields.push_back(i);
	    if (ielem->nulls_ordering == SORTBY_NULLS_LAST) {
	      schema->fields[i].nullLast = true;
	    }
	  }
	}
    }

    auto reqFut = client.createSchema("postgres", *schema);
    auto&&[status] = reqFut.get();
    if (!status.is2xxOK()) {
	ereport(ERROR,
	    (errmodule(MOD_MOT),
		errcode(ERRCODE_UNDEFINED_DATABASE),
		errmsg("Could not create K2 collection")));
    } 
    std::cout << "Made a K2 Schema for table: " << it->first << "with " << schema->fields.size()-2 << " fields and " << schema->partitionKeyFields.size() - 2 << " key fields" << std::endl;
}

TupleTableSlot* k2ExecForeignInsert(EState* estate, ResultRelInfo* resultRelInfo, TupleTableSlot* slot, TupleTableSlot* planSlot) {

  Oid foid = RelationGetRelid(resultRelInfo->ri_RelationDesc);
  auto schema = getSchema(foid);
  if (schema == nullptr) {
    report_pg_error(MOT::RC_TABLE_NOT_FOUND);
    return nullptr;
  }
  
  std::cout << "Insert row, got schema: " << schema->name << std::endl;
  skv::http::dto::SKVRecordBuilder build("postgres", schema);;
  
    HeapTuple srcData = (HeapTuple)slot->tts_tuple;
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
  auto beginFut = client.beginTxn(skv::http::dto::TxnOptions());
  auto&& [status, txnHandle] = beginFut.get();
  if (status.is2xxOK()) {
    auto writeFut = txnHandle.write(record, false, skv::http::dto::ExistencePrecondition::NotExists);
    auto&& [writeStatus] = writeFut.get();
    if (!writeStatus.is2xxOK()) {
      // TODO error types
       report_pg_error(MOT::RC_TABLE_NOT_FOUND);
      return nullptr;     
    }
  } else {
      report_pg_error(MOT::RC_TABLE_NOT_FOUND);
      return nullptr;
  }

  auto endFut = txnHandle.endTxn(true);
  auto&&[endStatus] = endFut.get();
  if (!endStatus.is2xxOK()) {
    // TODO error types
    report_pg_error(MOT::RC_TABLE_NOT_FOUND);
    return nullptr;
  }
 
  return slot;
}
