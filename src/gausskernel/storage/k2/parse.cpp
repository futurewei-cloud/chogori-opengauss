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

#include "postgres.h"
#include "funcapi.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_proc.h"
#include "optimizer/clauses.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"

#include "parse.h"

namespace k2fdw {

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
bool is_builtin(Oid objectId) {
  return (objectId < FistBootstrapObjectId);
}

/*
 * Functions to determine whether an expression can be evaluated safely on
 * remote server.
 */

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
bool foreign_expr_walker(Node *node,
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
   inner_cxt.state = FDWCollateState::FDW_COLLATE_NONE;

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
               state = OidIsValid(collation) ? FDWCollateState::FDW_COLLATE_SAFE : FDWCollateState::FDW_COLLATE_NONE;
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
                  state = FDWCollateState::FDW_COLLATE_NONE;
               }
               else
               {
                  /*
                   * Do not fail right away, since the Var might appear
                   * in a collation-insensitive context.
                   */
                  state = FDWCollateState::FDW_COLLATE_UNSAFE;
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
               state = FDWCollateState::FDW_COLLATE_NONE;
            else
               state = FDWCollateState::FDW_COLLATE_UNSAFE;
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
               state = FDWCollateState::FDW_COLLATE_NONE;
            else
               state = FDWCollateState::FDW_COLLATE_UNSAFE;
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
               state = FDWCollateState::FDW_COLLATE_NONE;
            else if (inner_cxt.state == FDWCollateState::FDW_COLLATE_SAFE &&
                   collation == inner_cxt.collation)
               state = FDWCollateState::FDW_COLLATE_SAFE;
            else if (collation == DEFAULT_COLLATION_OID)
               state = FDWCollateState::FDW_COLLATE_NONE;
            else
               state = FDWCollateState::FDW_COLLATE_UNSAFE;
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
            else if (inner_cxt.state != FDWCollateState::FDW_COLLATE_SAFE ||
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
               state = FDWCollateState::FDW_COLLATE_NONE;
            else if (inner_cxt.state == FDWCollateState::FDW_COLLATE_SAFE &&
                   collation == inner_cxt.collation)
               state = FDWCollateState::FDW_COLLATE_SAFE;
            else if (collation == DEFAULT_COLLATION_OID)
               state = FDWCollateState::FDW_COLLATE_NONE;
            else
               state = FDWCollateState::FDW_COLLATE_UNSAFE;
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
            else if (inner_cxt.state != FDWCollateState::FDW_COLLATE_SAFE ||
                   oe->inputcollid != inner_cxt.collation)
               return false;

            /* Result-collation handling is same as for functions */
            collation = oe->opcollid;
            if (collation == InvalidOid)
               state = FDWCollateState::FDW_COLLATE_NONE;
            else if (inner_cxt.state == FDWCollateState::FDW_COLLATE_SAFE &&
                   collation == inner_cxt.collation)
               state = FDWCollateState::FDW_COLLATE_SAFE;
            else if (collation == DEFAULT_COLLATION_OID)
               state = FDWCollateState::FDW_COLLATE_NONE;
            else
               state = FDWCollateState::FDW_COLLATE_UNSAFE;
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
            else if (inner_cxt.state != FDWCollateState::FDW_COLLATE_SAFE ||
                   oe->inputcollid != inner_cxt.collation)
               return false;

            /* Output is always boolean and so noncollatable. */
            collation = InvalidOid;
            state = FDWCollateState::FDW_COLLATE_NONE;
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
               state = FDWCollateState::FDW_COLLATE_NONE;
            else if (inner_cxt.state == FDWCollateState::FDW_COLLATE_SAFE &&
                   collation == inner_cxt.collation)
               state = FDWCollateState::FDW_COLLATE_SAFE;
            else if (collation == DEFAULT_COLLATION_OID)
               state = FDWCollateState::FDW_COLLATE_NONE;
            else
               state = FDWCollateState::FDW_COLLATE_UNSAFE;
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
            state = FDWCollateState::FDW_COLLATE_NONE;
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
            state = FDWCollateState::FDW_COLLATE_NONE;
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
               state = FDWCollateState::FDW_COLLATE_NONE;
            else if (inner_cxt.state == FDWCollateState::FDW_COLLATE_SAFE &&
                   collation == inner_cxt.collation)
               state = FDWCollateState::FDW_COLLATE_SAFE;
            else if (collation == DEFAULT_COLLATION_OID)
               state = FDWCollateState::FDW_COLLATE_NONE;
            else
               state = FDWCollateState::FDW_COLLATE_UNSAFE;
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
         case FDWCollateState::FDW_COLLATE_NONE:
            /* Nothing + nothing is still nothing */
            break;
         case FDWCollateState::FDW_COLLATE_SAFE:
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
                  outer_cxt->state = FDWCollateState::FDW_COLLATE_UNSAFE;
               }
            }
            break;
         case FDWCollateState::FDW_COLLATE_UNSAFE:
            /* We're still conflicted ... */
            break;
      }
   }

   /* It looks OK */
   return true;
}


bool is_foreign_expr(PlannerInfo *root,
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
   loc_cxt.state = FDWCollateState::FDW_COLLATE_NONE;

   if (!foreign_expr_walker((Node *) expr, &glob_cxt, &loc_cxt))
      return false;

   /*
    * If the expression has a valid collation that does not arise from a
    * foreign var, the expression can not be sent over.
    */
   if (loc_cxt.state == FDWCollateState::FDW_COLLATE_UNSAFE)
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


void parse_conditions(List *exprs, ParamListInfo paramLI, std::vector<K2PgConstraintDef>& result) {
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
         K2PgConstraintDef constraint;


         // K2PgEpxr and FDWOprCond separate column refs and constant literals without keeping the
         // structure of the expression, so we need to switch the direction of the comparison if the
         // column reference was not first in the original expression.
         switch (get_oprrest(opr_cond->opno)) {
             case F_EQSEL:  //  equal =
                 opr_name = "=";
                 break;
             case F_SCALARLTSEL:  // Less than <
                 opr_name = opr_cond->column_ref_first ? "<" : ">";
                 break;
             case F_SCALARLESEL:  // Less Equal <=
                 opr_name = opr_cond->column_ref_first ? "<=" : ">=";
                 break;
             case F_SCALARGTSEL:  // Greater than >
                 opr_name = opr_cond->column_ref_first ? ">" : "<";
                 break;
             case F_SCALARGESEL:  // Greater Euqal >=
                 opr_name = opr_cond->column_ref_first ? ">=" : "<=";
                 break;
             default:
                 elog(DEBUG4, "FDW: unsupported OpExpr type: %d", opr_cond->opno);
                 return opr_expr;
         }



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



         result.push_back(std::move(constraint));
		}
	}
}


void parse_expr(Expr *node, FDWExprRefValues *ref_values) {
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

void parse_op_expr(OpExpr *node, FDWExprRefValues *ref_values) {
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

void parse_var(Var *node, FDWExprRefValues *ref_values) {
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

void parse_const(Const *node, FDWExprRefValues *ref_values) {
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

void parse_param(Param *node, FDWExprRefValues *ref_values) {
	elog(DEBUG4, "FDW: parsing Param %s", nodeToString(node));
	ParamExternData *prm = NULL;
   // TODO
	// ParamExternData prmdata;
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
List *findOprCondition(foreign_expr_cxt context, int attr_num) {
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

} // ns
