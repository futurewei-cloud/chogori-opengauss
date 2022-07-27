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

#include "funcapi.h"
#include "access/reloptions.h"
#include "access/transam.h"
#include "postgres.h"

namespace k2fdw {

constexpr uint32_t FistBootstrapObjectId = 10000;  // TODO True for OG?

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
bool is_builtin(Oid objectId);

/*
 * Global context for foreign_expr_walker's search of an expression tree.
 */
struct foreign_glob_cxt
{
   PlannerInfo *root;         /* global planner state */
   RelOptInfo *foreignrel;      /* the foreign relation we are planning for */
};

/*
 * Local (per-tree-level) context for foreign_expr_walker's search.
 * This is concerned with identifying collations used in the expression.
 */
enum class FDWCollateState
{
   FDW_COLLATE_NONE,         /* expression is of a noncollatable type, or
                         * it has default collation that is not
                         * traceable to a foreign Var */
   FDW_COLLATE_SAFE,         /* collation derives from a foreign Var */
   FDW_COLLATE_UNSAFE         /* collation is non-default and derives from
                         * something other than a foreign Var */
};

struct foreign_loc_cxt
{
   Oid         collation;      /* OID of current collation, if any */
   FDWCollateState state;      /* state of current collation choice */
};

struct FDWColumnRef {
   AttrNumber attr_num;
   int attno;
   int attr_typid;
   int atttypmod;
};

struct FDWConstValue
{
   Oid   atttypid;
   Datum value;
   bool is_null;
};

struct FDWExprRefValues
{
   Oid opno;  // PG_OPERATOR OID of the operator
   List *column_refs;
   List *const_values;
   ParamListInfo paramLI; // parameters binding information for prepare statements
    bool column_ref_first;
};

struct FDWOprCond
{
   Oid opno;  // PG_OPERATOR OID of the operator
   FDWColumnRef *ref; // column reference
   FDWConstValue *val; // column value
    bool column_ref_first;
};

struct foreign_expr_cxt {
   List *opr_conds;          /* opr conditions */
};

/*
 * Functions to determine whether an expression can be evaluated safely on
 * remote server.
 */
bool foreign_expr_walker(Node *node,
			foreign_glob_cxt *glob_cxt,
			foreign_loc_cxt *outer_cxt);

bool is_foreign_expr(PlannerInfo *root,
		RelOptInfo *baserel,
		Expr *expr);

void parse_conditions(List *exprs, ParamListInfo paramLI, foreign_expr_cxt *expr_cxt);

void parse_expr(Expr *node, FDWExprRefValues *ref_values);

void parse_op_expr(OpExpr *node, FDWExprRefValues *ref_values);

void parse_var(Var *node, FDWExprRefValues *ref_values);

void parse_const(Const *node, FDWExprRefValues *ref_values);

void parse_param(Param *node, FDWExprRefValues *ref_values);

List *findOprCondition(foreign_expr_cxt context, int attr_num);

} // ns
