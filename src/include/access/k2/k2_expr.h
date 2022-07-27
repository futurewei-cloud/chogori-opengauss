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

#ifndef K2_EXPR_H
#define K2_EXPR_H

#include "postgres.h"
#include "nodes/primnodes.h"

#include "access/k2/pg_gate_typedefs.h"

#include "access/k2/pg_gate_api.h"

// Construct column reference expression.
extern K2PgExpr K2PgNewColumnRef(K2PgStatement k2pg_stmt, int16_t attr_num, int attr_typid,
																 const K2PgTypeAttrs *type_attrs);

// Construct constant expression using the given datatype "type_id" and value "datum".
extern K2PgExpr K2PgNewConstant(K2PgStatement k2pg_stmt, Oid type_id, Datum datum, bool is_null);

// Construct a generic eval_expr call for given a PG Expr and its expected type and attno.
extern K2PgExpr K2PgNewEvalExprCall(K2PgStatement k2pg_stmt, Expr *expr, int32_t attno, int32_t type_id, int32_t type_mod);

#endif							/* K2_EXPR_H */
