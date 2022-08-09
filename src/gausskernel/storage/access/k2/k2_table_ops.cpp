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

#include "access/sysattr.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "utils/relcache.h"
#include "utils/rel.h"
#include "utils/lsyscache.h"
#include "nodes/execnodes.h"
#include "commands/dbcommands.h"
#include "executor/tuptable.h"
#include "miscadmin.h"
#include "catalog/catalog.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_database.h"
#include "utils/catcache.h"
#include "utils/inval.h"
#include "utils/relcache.h"
#include "utils/rel.h"
#include "executor/tuptable.h"

#include "utils/syscache.h"
#include "access/k2/k2_table_ops.h"
#include "access/k2/k2_expr.h"
#include "access/k2/k2_type.h"
#include "access/k2/k2_plan.h"
#include "access/k2/pg_gate_api.h"
#include "access/k2/k2pg_aux.h"

/*
 * Hack to ensure that the next CommandCounterIncrement() will call
 * CommandEndInvalidationMessages(). The result of this call is not
 * needed on the yb side, however the side effects are.
 */
void MarkCurrentCommandUsed() {
	(void) GetCurrentCommandId(true);
}

/*
 * Returns whether a relation's attribute is a real column in the backing
 * K2 table. (It implies we can both read from and write to it).
 */
bool IsRealK2PgColumn(Relation rel, int attrNum)
{
	return (attrNum > 0 && !TupleDescAttr(rel->rd_att, attrNum - 1)->attisdropped) ||
	       (rel->rd_rel->relhasoids && attrNum == ObjectIdAttributeNumber);
}

/*
 * Returns whether a relation's attribute is a K2PG system column.
 */
bool IsK2PgSystemColumn(int attrNum)
{
	return (attrNum == K2PgRowIdAttributeNumber ||
			attrNum == K2PgIdxBaseTupleIdAttributeNumber ||
			attrNum == K2PgUniqueIdxKeySuffixAttributeNumber);
}

/*
 * Returns whether relation is capable of single row execution.
 */
bool K2PgIsSingleRowTxnCapableRel(ResultRelInfo *resultRelInfo)
{
	bool has_triggers = resultRelInfo->ri_TrigDesc && resultRelInfo->ri_TrigDesc->numtriggers > 0;
	bool has_indices = K2PgRelInfoHasSecondaryIndices(resultRelInfo);
	return !has_indices && !has_triggers;
}

/*
 * Get the type ID of a real or virtual attribute (column).
 * Returns InvalidOid if the attribute number is invalid.
 */
static Oid GetTypeId(int attrNum, TupleDesc tupleDesc)
{
	switch (attrNum)
	{
		case SelfItemPointerAttributeNumber:
			return TIDOID;
		case ObjectIdAttributeNumber:
			return OIDOID;
		case MinTransactionIdAttributeNumber:
			return XIDOID;
		case MinCommandIdAttributeNumber:
			return CIDOID;
		case MaxTransactionIdAttributeNumber:
			return XIDOID;
		case MaxCommandIdAttributeNumber:
			return CIDOID;
		case TableOidAttributeNumber:
			return OIDOID;
		default:
			if (attrNum > 0 && attrNum <= tupleDesc->natts)
				return TupleDescAttr(tupleDesc, attrNum - 1)->atttypid;
			else
				return InvalidOid;
	}
}

/*
 * Get primary key columns as bitmap of a table.
 */
static Bitmapset *GetTablePrimaryKey(Relation rel,
									 AttrNumber minattr,
									 bool includeK2PgSystemColumns)
{
	Oid            dboid         = K2PgGetDatabaseOid(rel);
	Oid            relid         = RelationGetRelid(rel);
	int            natts         = RelationGetNumberOfAttributes(rel);
	Bitmapset      *pkey         = NULL;
	K2PgTableDesc k2pg_tabledesc = NULL;

	/* Get the primary key columns 'pkey' from K2PG. */
	HandleK2PgStatus(PgGate_GetTableDesc(dboid, relid, &k2pg_tabledesc));
	for (AttrNumber attnum = minattr; attnum <= natts; attnum++)
	{
		if ((!includeK2PgSystemColumns && !IsRealK2PgColumn(rel, attnum)) ||
			(!IsRealK2PgColumn(rel, attnum) && !IsK2PgSystemColumn(attnum)))
		{
			continue;
		}

		bool is_primary = false;
		bool is_hash    = false;
		HandleK2PgTableDescStatus(PgGate_GetColumnInfo(k2pg_tabledesc,
		                                           attnum,
		                                           &is_primary,
		                                           &is_hash), k2pg_tabledesc);
		if (is_primary)
		{
			pkey = bms_add_member(pkey, attnum - minattr);
		}
	}

	return pkey;
}

/*
 * Get primary key columns as bitmap of a table for real K2PG columns.
 */
Bitmapset *GetK2PgTablePrimaryKey(Relation rel)
{
	return GetTablePrimaryKey(rel, FirstLowInvalidHeapAttributeNumber + 1 /* minattr */,
							  false /* includeK2PgSystemColumns */);
}

/*
 * Get primary key columns as bitmap of a table for real and system K2PG columns.
 */
Bitmapset *GetFullK2PgTablePrimaryKey(Relation rel)
{
	return GetTablePrimaryKey(rel, K2PgSystemFirstLowInvalidAttributeNumber + 1 /* minattr */,
							  true /* includeK2PgSystemColumns */);
}

/*
 * Get the k2pgctid from a K2PG scan slot for UPDATE/DELETE.
 */
Datum K2PgGetPgTupleIdFromSlot(TupleTableSlot *slot)
{
	/*
	 * Look for k2pgctid in the tuple first if the slot contains a tuple packed with k2pgctid.
	 * Otherwise, look for it in the attribute list as a junk attribute.
	 */
	if (slot->tts_tuple != NULL &&  ((HeapTuple)(slot->tts_tuple))->t_k2pgctid != 0)
	{
		return ((HeapTuple)(slot->tts_tuple))->t_k2pgctid;
	}

	for (int idx = 0; idx < slot->tts_nvalid; idx++)
	{
		Form_pg_attribute att = TupleDescAttr(slot->tts_tupleDescriptor, idx);
		if (strcmp(NameStr(att->attname), "k2pgctid") == 0 && !slot->tts_isnull[idx])
		{
			Assert(att->atttypid == BYTEAOID);
			return slot->tts_values[idx];
		}
	}

	return 0;
}

/*
 * Get the k2pgctid from a tuple.
 *
 * Note that if the relation has a K2 PG RowId attribute, this will generate a new RowId value
 * meaning the k2pgctid will be unique. Therefore you should only use this if the relation has
 * a primary key or you're doing an insert.
 */
Datum K2PgGetPgTupleIdFromTuple(Relation rel,
							   HeapTuple tuple,
							   TupleDesc tupleDesc) {
	Bitmapset *pkey = GetFullK2PgTablePrimaryKey(rel);
	AttrNumber minattr = K2PgSystemFirstLowInvalidAttributeNumber + 1;
	const int nattrs = bms_num_members(pkey);
	K2PgAttrValueDescriptor *attrs =
			(K2PgAttrValueDescriptor*)palloc(nattrs * sizeof(K2PgAttrValueDescriptor));
	uint64_t tuple_id = 0;
	K2PgAttrValueDescriptor *next_attr = attrs;
	int col = -1;
	while ((col = bms_next_member(pkey, col)) >= 0) {
		AttrNumber attnum = col + minattr;
		next_attr->attr_num = attnum;
		/*
		 * Don't need to fill in for the K2 PG RowId column, however we still
		 * need to add the column to the statement to construct the k2pgctid.
		 */
		if (attnum != K2PgRowIdAttributeNumber) {
			Oid	type_id = (attnum > 0) ?
					TupleDescAttr(tupleDesc, attnum - 1)->atttypid : InvalidOid;

			next_attr->type_entity = K2PgDataTypeFromOidMod(attnum, type_id);
			next_attr->datum = heap_getattr(tuple, attnum, tupleDesc, &next_attr->is_null);
		} else {
			next_attr->datum = 0;
			next_attr->is_null = false;
			next_attr->type_entity = NULL;
		}
		++next_attr;
	}
    // TODO see if statement parameter is necessary
	HandleK2PgStatus(PgGate_DmlBuildPgTupleId(NULL, attrs, nattrs, &tuple_id));
	pfree(attrs);
	return (Datum)tuple_id;
}

/*
 * Check if operation changes a system table, ignore changes during
 * initialization (bootstrap mode).
 */
static bool IsSystemCatalogChange(Relation rel)
{
	return IsSystemRelation(rel) && !IsBootstrapProcessingMode();
}

/*
 * Utility method to insert a tuple into the relation's backing K2PG table.
 */
static Oid K2PgExecuteInsertInternal(Relation rel,
                                    TupleDesc tupleDesc,
                                     HeapTuple tuple)
{
	Oid            dboid    = K2PgGetDatabaseOid(rel);
	Oid            relid    = RelationGetRelid(rel);
	AttrNumber     minattr  = FirstLowInvalidHeapAttributeNumber + 1;
	int            natts    = RelationGetNumberOfAttributes(rel);
	Bitmapset      *pkey    = GetK2PgTablePrimaryKey(rel);
	bool           is_null  = false;
    std::vector<K2PgWriteColumnDef> columns;

	/* Generate a new oid for this row if needed */
	if (rel->rd_rel->relhasoids)
	{
		if (!OidIsValid(HeapTupleGetOid(tuple)))
			HeapTupleSetOid(tuple, GetNewOid(rel));
	}

	for (AttrNumber attnum = minattr; attnum <= natts; attnum++)
	{
		/* Skip virtual (system) and dropped columns */
		if (!IsRealK2PgColumn(rel, attnum))
		{
			continue;
		}

		Oid   type_id = GetTypeId(attnum, tupleDesc);
		Datum datum   = heap_getattr(tuple, attnum, tupleDesc, &is_null);

		/* Check not-null constraint on primary key early */
		if (is_null && bms_is_member(attnum - minattr, pkey))
		{
			ereport(ERROR,
			        (errcode(ERRCODE_NOT_NULL_VIOLATION), errmsg(
					        "Missing/null value for primary key column")));
		}

		/* Add the column value to the insert request */
        K2PgWriteColumnDef column {
            .attr_num = attnum,
            .type_id = type_id,
            .datum = datum,
            .is_null = is_null
        };
        columns.push_back(std::move(column));
	}

	/*
	 * For system tables, mark tuple for invalidation from system caches
	 * at next command boundary. Do this now so if there is an error with insert
	 * we will re-query to get the correct state from the master.
	 */
	if (IsCatalogRelation(rel))
	{
		MarkCurrentCommandUsed();
		CacheInvalidateHeapTuple(rel, tuple, NULL);
	}

    // TODO: && RelationHasCachedLists(rel)
	bool is_syscatalog_change = IsSystemCatalogChange(rel);
	/* Execute the insert */
	HandleK2PgStatus(PgGate_ExecInsert(dboid, relid, false /* upsert */, is_syscatalog_change, columns));

	/*
	 * Optimization to increment the catalog version for the local cache as
	 * this backend is already aware of this change and should update its
	 * catalog caches accordingly (without needing to ask the master).
	 * Note that, since the master catalog version should have been identically
	 * incremented, it will continue to match with the local cache version if
	 * and only if no other master changes occurred in the meantime (i.e. from
	 * other backends).
	 * If changes occurred, then a cache refresh will be needed as usual.
	 */
	if (is_syscatalog_change)
	{
		// TODO(shane) also update the shared memory catalog version here.
		k2pg_catalog_cache_version += 1;
	}

	return HeapTupleGetOid(tuple);
}

/*
 * Utility method to set keys and value to index write statement
 */
static void PrepareIndexWriteStmt(Relation index,
                                  Datum *values,
                                  bool *isnull,
                                  int natts,
                                  Datum k2pgbasectid,
                                  bool k2pgctid_as_value,
                                  std::vector<K2PgWriteColumnDef>& columns)
{
	TupleDesc tupdesc = RelationGetDescr(index);

	if (k2pgbasectid == 0)
	{
		ereport(ERROR,
		(errcode(ERRCODE_INTERNAL_ERROR), errmsg(
			"Missing base table k2pgctid in index write request")));
	}

	bool has_null_attr = false;
	for (AttrNumber attnum = 1; attnum <= natts; ++attnum)
	{
		Oid   type_id = GetTypeId(attnum, tupdesc);
		Datum value   = values[attnum - 1];
		bool  is_null = isnull[attnum - 1];
		has_null_attr = has_null_attr || is_null;
        K2PgWriteColumnDef column {
            .attr_num = attnum,
            .type_id = type_id,
            .datum = value,
            .is_null = is_null
        };
        columns.push_back(std::move(column));
	}

	const bool unique_index = index->rd_index->indisunique;

	/*
	 * For unique indexes we need to set the key suffix system column:
	 * - to k2pgbasectid if at least one index key column is null.
	 * - to NULL otherwise (setting is_null to true is enough).
	 */
	if (unique_index) {
         K2PgWriteColumnDef column {
            .attr_num = K2PgUniqueIdxKeySuffixAttributeNumber,
            .type_id = BYTEAOID,
            .datum = k2pgbasectid,
            .is_null = !has_null_attr
        };
        columns.push_back(std::move(column));
   }

	/*
	 * We may need to set the base ctid column:
	 * - for unique indexes only if we need it as a value (i.e. for inserts)
	 * - for non-unique indexes always (it is a key column).
	 */
	if (k2pgctid_as_value || !unique_index) {
          K2PgWriteColumnDef column {
            .attr_num =	K2PgIdxBaseTupleIdAttributeNumber,
            .type_id = BYTEAOID,
            .datum = k2pgbasectid,
            .is_null = false
        };
        columns.push_back(std::move(column));
    }
}

Oid K2PgExecuteInsert(Relation rel,
                     TupleDesc tupleDesc,
                     HeapTuple tuple)
{
	return K2PgExecuteInsertInternal(rel,
	                                tupleDesc,
                                     tuple);
}

Oid K2PgExecuteNonTxnInsert(Relation rel,
						   TupleDesc tupleDesc,
						   HeapTuple tuple)
{
	return K2PgExecuteInsertInternal(rel,
	                                tupleDesc,
                                     tuple);
}

Oid K2PgHeapInsert(TupleTableSlot *slot,
				  HeapTuple tuple,
				  EState *estate)
{
	/*
	 * get information on the (current) result relation
	 */
	ResultRelInfo *resultRelInfo = estate->es_result_relation_info;
	Relation resultRelationDesc = resultRelInfo->ri_RelationDesc;

	if (estate->es_k2pg_is_single_row_modify_txn)
	{
		/*
		 * Try to execute the statement as a single row transaction (rather
		 * than a distributed transaction) if it is safe to do so.
		 * I.e. if we are in a single-statement transaction that targets a
		 * single row (i.e. single-row-modify txn), and there are no indices
		 * or triggers on the target table.
		 */
		return K2PgExecuteNonTxnInsert(resultRelationDesc, slot->tts_tupleDescriptor, tuple);
	}
	else
	{
		return K2PgExecuteInsert(resultRelationDesc, slot->tts_tupleDescriptor, tuple);
	}
}

void K2PgExecuteInsertIndex(Relation index,
						   Datum *values,
						   bool *isnull,
                            Datum k2pgctid)
{
	Assert(index->rd_rel->relkind == RELKIND_INDEX);
	Assert(k2pgctid != 0);

	Oid            dboid    = K2PgGetDatabaseOid(index);
	Oid            relid    = RelationGetRelid(index);
    std::vector<K2PgWriteColumnDef> columns;
    bool upsert = false;

	PrepareIndexWriteStmt(index, values, isnull,
						  RelationGetNumberOfAttributes(index),
						  k2pgctid, true /* k2pgctid_as_value */, columns);

	/*
	 * For non-unique indexes the primary-key component (base tuple id) already
	 * guarantees uniqueness, so no need to read and check it in K2 PG.
	 */
	if (!index->rd_index->indisunique) {
        upsert = true;
	}

	/* Execute the insert and clean up. */
	HandleK2PgStatus(PgGate_ExecInsert(dboid, relid, upsert, false, columns));
}

bool K2PgExecuteDelete(Relation rel, TupleTableSlot *slot, EState *estate, ModifyTableState *mtstate)
{
	Oid            dboid          = K2PgGetDatabaseOid(rel);
	Oid            relid          = RelationGetRelid(rel);

	// TODO: consider removing this logic if not needed
	bool           isSingleRow    = mtstate->k2pg_mt_is_single_row_update_or_delete;
	Datum          k2pgctid         = 0;
    std::vector<K2PgWriteColumnDef> columns;

	/*
	 * Look for k2pgctid. Raise error if k2pgctid is not found.
	 *
	 * If single row delete, generate k2pgctid from tuple values, otherwise
	 * retrieve it from the slot.
	 */
	if (isSingleRow)
	{
		HeapTuple tuple = ExecMaterializeSlot(slot);
		k2pgctid = K2PgGetPgTupleIdFromTuple(rel,
										  tuple,
										  slot->tts_tupleDescriptor);
	}
	else
	{
		k2pgctid = K2PgGetPgTupleIdFromSlot(slot);
	}

	if (k2pgctid == 0)
	{
		ereport(ERROR,
		        (errcode(ERRCODE_UNDEFINED_COLUMN), errmsg(
					"Missing column k2pgctid in DELETE request to K2PG database")));
	}

	/* Bind k2pgctid to identify the current row. */
    K2PgWriteColumnDef k2id {
        .attr_num = K2PgTupleIdAttributeNumber,
        .type_id = BYTEAOID,
        .datum = k2pgctid,
        .is_null = false
    };
    columns.push_back(std::move(k2id));


	/* Delete row from foreign key cache */
	HandleK2PgStatus(PgGate_DeleteFromForeignKeyReferenceCache(relid, k2pgctid));

    bool increment_catalog = IsSystemCatalogChange(rel);

	/* Execute the statement. */
	int rows_affected_count = 0;
    HandleK2PgStatus(PgGate_ExecDelete(dboid, relid, increment_catalog, &rows_affected_count, columns));

	/*
	 * Optimization to increment the catalog version for the local cache as
	 * this backend is already aware of this change and should update its
	 * catalog caches accordingly (without needing to ask the master).
	 * Note that, since the master catalog version should have been identically
	 * incremented, it will continue to match with the local cache version if
	 * and only if no other master changes occurred in the meantime (i.e. from
	 * other backends).
	 * If changes occurred, then a cache refresh will be needed as usual.
	 */
	if (increment_catalog)
	{
		// TODO(shane) also update the shared memory catalog version here.
		k2pg_catalog_cache_version += 1;
	}

	return !isSingleRow || rows_affected_count > 0;
}

void K2PgExecuteDeleteIndex(Relation index, Datum *values, bool *isnull, Datum k2pgctid)
{
  Assert(index->rd_rel->relkind == RELKIND_INDEX);

	Oid            dboid    = K2PgGetDatabaseOid(index);
	Oid            relid    = RelationGetRelid(index);
    std::vector<K2PgWriteColumnDef> columns;

	PrepareIndexWriteStmt(index, values, isnull,
	                      IndexRelationGetNumberOfKeyAttributes(index),
	                      k2pgctid, false /* k2pgctid_as_value */, columns);

	/* Delete row from foreign key cache */
	HandleK2PgStatus(PgGate_DeleteFromForeignKeyReferenceCache(relid, k2pgctid));

    HandleK2PgStatus(PgGate_ExecDelete(dboid, relid, false, NULL, columns));
}

bool K2PgExecuteUpdate(Relation rel,
					  TupleTableSlot *slot,
					  HeapTuple tuple,
					  EState *estate,
					  ModifyTableState *mtstate,
					  Bitmapset *updatedCols)
{
	TupleDesc      tupleDesc      = slot->tts_tupleDescriptor;
	Oid            dboid          = K2PgGetDatabaseOid(rel);
	Oid            relid          = RelationGetRelid(rel);
	bool           isSingleRow    = mtstate->k2pg_mt_is_single_row_update_or_delete;
	Datum          k2pgctid         = 0;
    std::vector<K2PgWriteColumnDef> columns;

	/*
	 * Look for k2pgctid. Raise error if k2pgctid is not found.
	 *
	 * If single row update, generate k2pgctid from tuple values, otherwise
	 * retrieve it from the slot.
	 */
	if (isSingleRow)
	{
		k2pgctid = K2PgGetPgTupleIdFromTuple(rel,
										  tuple,
										  slot->tts_tupleDescriptor);
	}
	else
	{
		k2pgctid = K2PgGetPgTupleIdFromSlot(slot);
	}

	if (k2pgctid == 0)
	{
		ereport(ERROR,
		        (errcode(ERRCODE_UNDEFINED_COLUMN), errmsg(
					"Missing column k2pgctid in UPDATE request to K2PG database")));
	}

	/* Bind k2pgctid to identify the current row. */
    K2PgWriteColumnDef k2id {
        .attr_num = K2PgTupleIdAttributeNumber,
        .type_id = BYTEAOID,
        .datum = k2pgctid,
        .is_null = false
    };
    columns.push_back(std::move(k2id));

	/* Assign new values to the updated columns for the current row. */
	tupleDesc = RelationGetDescr(rel);
	bool whole_row = bms_is_member(InvalidAttrNumber, updatedCols);

    /* TODO in chogori-sql, pushdown ops were retrieved from the list like below.
     * We don't support these pushdowns in k2 and we should make sure opengauss does not try
     * to pass them to us.
     *
     * ModifyTable *mt_plan = (ModifyTable *) mtstate->ps.plan;
     * ListCell* pushdown_lc = list_head(mt_plan->k2PushdownTlist);
     *
     * Also we should eventually support SQL such as "SET balance = balance + 1" even if it is not
     * pushed down to the storage. We should automatically convert it to a read-modify-write for k2
     */
    
	for (int idx = 0; idx < tupleDesc->natts; idx++)
	{
		FormData_pg_attribute *att_desc = TupleDescAttr(tupleDesc, idx);

		AttrNumber attnum = att_desc->attnum;
		Oid type_id = att_desc->atttypid;

		/* Skip virtual (system) and dropped columns */
		if (!IsRealK2PgColumn(rel, attnum))
			continue;

		/*
		 * Skip unmodified columns if possible.
		 * Note: we only do this for the single-row case, as otherwise there
		 * might be triggers that modify the heap tuple to set (other) columns
		 * (e.g. using the SPI module functions).
		 */
		int bms_idx = attnum - K2PgGetFirstLowInvalidAttributeNumber(rel);
		if (isSingleRow && !whole_row && !bms_is_member(bms_idx, updatedCols))
			continue;

        bool is_null = false;
        Datum d = heap_getattr(tuple, attnum, tupleDesc, &is_null);
        K2PgWriteColumnDef column {
            .attr_num = attnum,
            .type_id = type_id,
            .datum = d,
            .is_null = is_null
        };
        columns.push_back(std::move(column));
	}

    bool increment_catalog = IsSystemCatalogChange(rel);
    
	/* Execute the statement. */
	int rows_affected_count = 0;
    HandleK2PgStatus(PgGate_ExecUpdate(dboid, relid, increment_catalog, &rows_affected_count, columns));

	/*
	 * Optimization to increment the catalog version for the local cache as
	 * this backend is already aware of this change and should update its
	 * catalog caches accordingly (without needing to ask the master).
	 * Note that, since the master catalog version should have been identically
	 * incremented, it will continue to match with the local cache version if
	 * and only if no other master changes occurred in the meantime (i.e. from
	 * other backends).
	 * If changes occurred, then a cache refresh will be needed as usual.
	 */
	if (increment_catalog)
	{
		// TODO(shane) also update the shared memory catalog version here.
		k2pg_catalog_cache_version += 1;
	}

	/*
	 * If the relation has indexes, save the k2pgctid to insert the updated row into the indexes.
	 */
	if (K2PgRelHasSecondaryIndices(rel))
	{
		tuple->t_k2pgctid = k2pgctid;
	}

	return !isSingleRow || rows_affected_count > 0;
}

void K2PgDeleteSysCatalogTuple(Relation rel, HeapTuple tuple)
{
	Oid            dboid       = K2PgGetDatabaseOid(rel);
	Oid            relid       = RelationGetRelid(rel);
    std::vector<K2PgWriteColumnDef> columns;

	if (tuple->t_k2pgctid == 0)
		ereport(ERROR,
		        (errcode(ERRCODE_UNDEFINED_COLUMN), errmsg(
				        "Missing column k2pgctid in DELETE request to K2PG database")));

	/* Bind k2pgctid to identify the current row. */
    K2PgWriteColumnDef k2id {
        .attr_num = K2PgTupleIdAttributeNumber,
        .type_id = BYTEAOID,
        .datum = tuple->t_k2pgctid,
        .is_null = false
    };
    columns.push_back(std::move(k2id));


	/* Delete row from foreign key cache */
	HandleK2PgStatus(PgGate_DeleteFromForeignKeyReferenceCache(relid, tuple->t_k2pgctid));

	/*
	 * Mark tuple for invalidation from system caches at next command
	 * boundary. Do this now so if there is an error with delete we will
	 * re-query to get the correct state from the master.
	 */
	MarkCurrentCommandUsed();
	CacheInvalidateHeapTuple(rel, tuple, NULL);

    HandleK2PgStatus(PgGate_ExecDelete(dboid, relid, true /* increment_catalog */, NULL /* rows_affected */, columns));
}

void K2PgUpdateSysCatalogTuple(Relation rel, HeapTuple oldtuple, HeapTuple tuple)
{
	Oid            dboid       = K2PgGetDatabaseOid(rel);
	Oid            relid       = RelationGetRelid(rel);
	TupleDesc      tupleDesc   = RelationGetDescr(rel);
	int            natts       = RelationGetNumberOfAttributes(rel);
    std::vector<K2PgWriteColumnDef> columns;

	AttrNumber minattr = FirstLowInvalidHeapAttributeNumber + 1;
	Bitmapset  *pkey   = GetK2PgTablePrimaryKey(rel);

	/* Bind the k2pgctid to the statement. */
    K2PgWriteColumnDef tidColumn {
        .attr_num = K2PgTupleIdAttributeNumber,
        .type_id = BYTEAOID,
        .datum = tuple->t_k2pgctid,
        .is_null = false
    };
    columns.push_back(std::move(tidColumn));

	/* Assign values to the non-primary-key columns to update the current row. */
	for (int idx = 0; idx < natts; idx++)
	{
		AttrNumber attnum = TupleDescAttr(tupleDesc, idx)->attnum;

		/* Skip primary-key columns */
		if (bms_is_member(attnum - minattr, pkey))
		{
			continue;
		}

		bool is_null = false;
		Datum d = heap_getattr(tuple, attnum, tupleDesc, &is_null);
        K2PgWriteColumnDef column {
            .attr_num = attnum,
            .type_id = TupleDescAttr(tupleDesc, idx)->atttypid,
            .datum = d,
            .is_null = is_null
        };
        columns.push_back(std::move(column));
	}

	/*
	 * Mark old tuple for invalidation from system caches at next command
	 * boundary, and mark the new tuple for invalidation in case we abort.
	 * In case when there is no old tuple, we will invalidate with the
	 * new tuple at next command boundary instead. Do these now so if there
	 * is an error with update we will re-query to get the correct state
	 * from the master.
	 */
	MarkCurrentCommandUsed();
	if (oldtuple)
		CacheInvalidateHeapTuple(rel, oldtuple, tuple);
	else
		CacheInvalidateHeapTuple(rel, tuple, NULL);

	/* Execute the statement and clean up */
    HandleK2PgStatus(PgGate_ExecUpdate(dboid, relid, true /* increment catalog */, NULL /* rows affected */, columns)); 
}

bool
K2PgRelInfoHasSecondaryIndices(ResultRelInfo *resultRelInfo)
{
	return resultRelInfo->ri_NumIndices > 1 ||
			(resultRelInfo->ri_NumIndices == 1 &&
			 !resultRelInfo->ri_IndexRelationDescs[0]->rd_index->indisprimary);
}
