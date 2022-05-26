#include "k2_expr.h"
#include "access/k2/k2_type.h"
#include "access/k2/k2pg_aux.h"

K2PgExpr K2PgNewColumnRef(K2PgStatement k2pg_stmt, int16_t attr_num, int attr_typid,
						  const K2PgTypeAttrs *type_attrs) {
	K2PgExpr expr = NULL;
	const K2PgTypeEntity *type_entity = K2PgDataTypeFromOidMod(attr_num, attr_typid);
	HandleK2PgStatus(PgGate_NewColumnRef(k2pg_stmt, attr_num, type_entity, type_attrs, &expr));
	return expr;
}

K2PgExpr K2PgNewConstant(K2PgStatement k2pg_stmt, Oid type_id, Datum datum, bool is_null) {
	K2PgExpr expr = NULL;
	const K2PgTypeEntity *type_entity = K2PgDataTypeFromOidMod(InvalidAttrNumber, type_id);
	HandleK2PgStatus(PgGate_NewConstant(k2pg_stmt, type_entity, datum, is_null, &expr));
	return expr;
}

K2PgExpr K2PgNewEvalExprCall(K2PgStatement k2pg_stmt,
                             Expr *pg_expr,
                             int32_t attno,
                             int32_t typid,
                             int32_t typmod) {
	K2PgExpr k2pg_expr = NULL;
	const K2PgTypeEntity *type_ent = K2PgDataTypeFromOidMod(InvalidAttrNumber, typid);
	PgGate_NewOperator(k2pg_stmt, "eval_expr_call", type_ent, &k2pg_expr);

	Datum expr_datum = CStringGetDatum(nodeToString(pg_expr));
	K2PgExpr expr = K2PgNewConstant(k2pg_stmt, CSTRINGOID, expr_datum , /* IsNull */ false);
	PgGate_OperatorAppendArg(k2pg_expr, expr);

	/*
	 * Adding the column type id and mod to the message since we only have the PG Sql types in the
	 * K2 schema Schema.
	 */
	K2PgExpr attno_expr = K2PgNewConstant(k2pg_stmt, INT4OID, (Datum) attno, /* IsNull */ false);
	PgGate_OperatorAppendArg(k2pg_expr, attno_expr);
	K2PgExpr typid_expr = K2PgNewConstant(k2pg_stmt, INT4OID, (Datum) typid, /* IsNull */ false);
	PgGate_OperatorAppendArg(k2pg_expr, typid_expr);
	K2PgExpr typmod_expr = K2PgNewConstant(k2pg_stmt, INT4OID, (Datum) typmod, /* IsNull */ false);
	PgGate_OperatorAppendArg(k2pg_expr, typmod_expr);

	return k2pg_expr;
}
