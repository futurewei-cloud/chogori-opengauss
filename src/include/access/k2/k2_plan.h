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

#ifndef K2_PLAN_H
#define K2_PLAN_H

#include "postgres.h"
#include "nodes/plannodes.h"
#include "nodes/relation.h"


void K2PgExprInstantiateParams(Expr* expr, ParamListInfo paramLI);

bool K2PgIsSupportedSingleRowModifyWhereExpr(Expr *expr);

bool K2PgIsSupportedSingleRowModifyAssignExpr(Expr *expr,
                                             AttrNumber target_attno,
                                             bool *needs_pushdown);

bool K2PgIsSingleRowModify(PlannedStmt *pstmt);

bool K2PgIsSingleRowUpdateOrDelete(ModifyTable *modifyTable);

bool K2PgAllPrimaryKeysProvided(Oid relid, Bitmapset *attrs);

#endif // K2_PLAN_H
