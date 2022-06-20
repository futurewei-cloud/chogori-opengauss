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

#ifndef K2_TABLE_OPS_H
#define K2_TABLE_OPS_H

#include "nodes/execnodes.h"
#include "executor/tuptable.h"

//------------------------------------------------------------------------------
// K2 PG modify table API.

/*
 * Insert data into K2 PG table.
 * This function is equivalent to "heap_insert", but it sends data to K2 platform.
 */
extern Oid K2PgHeapInsert(TupleTableSlot *slot,
												 HeapTuple tuple,
												 EState *estate);

/*
 * Insert a tuple into a K2PG table. Will execute within a distributed
 * transaction if the table is transactional (PSQL default).
 */
extern Oid K2PgExecuteInsert(Relation rel,
                            TupleDesc tupleDesc,
                            HeapTuple tuple);

/*
 * Execute the insert outside of a transaction.
 * Assumes the caller checked that it is safe to do so.
 *
 * disable this for now since K2PG is always transactional
 */
//extern Oid K2PgExecuteNonTxnInsert(Relation rel,
//								  TupleDesc tupleDesc,
//								  HeapTuple tuple);

/*
 * Insert a tuple into the an index's backing K2PG index table.
 */
extern void K2PgExecuteInsertIndex(Relation rel,
								  Datum *values,
								  bool *isnull,
								  Datum k2pgctid,
								  bool is_backfill);

/*
 * Delete a tuple (identified by k2pgctid) from a K2PG table.
 * If this is a single row op we will return false in the case that there was
 * no row to delete. This can occur because we do not first perform a scan if
 * it is a single row op.
 */
extern bool K2PgExecuteDelete(Relation rel,
							 TupleTableSlot *slot,
							 EState *estate,
							 ModifyTableState *mtstate);
/*
 * Delete a tuple (identified by index columns and base table k2pgctid) from an
 * index's backing K2PG index table.
 */
extern void K2PgExecuteDeleteIndex(Relation index,
                                  Datum *values,
                                  bool *isnull,
                                  Datum k2pgctid);

/*
 * Update a row (identified by k2pgctid) in a K2PG table.
 * If this is a single row op we will return false in the case that there was
 * no row to update. This can occur because we do not first perform a scan if
 * it is a single row op.
 */
extern bool K2PgExecuteUpdate(Relation rel,
							 TupleTableSlot *slot,
							 HeapTuple tuple,
							 EState *estate,
							 ModifyTableState *mtstate,
							 Bitmapset *updatedCols);

//------------------------------------------------------------------------------
// System tables modify-table API.
// For system tables we identify rows to update/delete directly by primary key
// and execute them directly (rather than needing to read k2pgctid first).
// TODO This should be used for regular tables whenever possible.

extern void K2PgDeleteSysCatalogTuple(Relation rel, HeapTuple tuple);

extern void K2PgUpdateSysCatalogTuple(Relation rel,
									 HeapTuple oldtuple,
									 HeapTuple tuple);

//------------------------------------------------------------------------------
// Utility methods.

extern bool K2PgIsSingleRowTxnCapableRel(ResultRelInfo *resultRelInfo);

extern Datum K2PgGetPgTupleIdFromSlot(TupleTableSlot *slot);

extern Datum K2PgGetPgTupleIdFromTuple(K2PgStatement pg_stmt,
									  Relation rel,
									  HeapTuple tuple,
									  TupleDesc tupleDesc);

/*
 * Returns if a table has secondary indices.
 */
extern bool K2PgRelInfoHasSecondaryIndices(ResultRelInfo *resultRelInfo);

/*
 * Get primary key columns as bitmap of a table for real and system K2PG columns.
 */
extern Bitmapset *GetFullK2PgTablePrimaryKey(Relation rel);

/*
 * Get primary key columns as bitmap of a table for real columns.
 */
extern Bitmapset *GetK2PgTablePrimaryKey(Relation rel);

#endif							/* K2_TABLE_OPS_H */
