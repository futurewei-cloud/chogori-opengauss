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

#include "miscadmin.h"
#include "access/nbtree.h"
#include "access/relscan.h"
#include "access/sysattr.h"
#include "access/k2/k2catam.h"
#include "access/k2/k2_index_ops.h"
#include "access/k2/k2cat_cmds.h"
#include "access/k2/k2_table_ops.h"
#include "catalog/index.h"
#include "catalog/pg_type.h"
#include "utils/rel.h"

/* --------------------------------------------------------------------------------------------- */

/* Working state for k2inbuild and its callback */
typedef struct
{
	bool	isprimary;		/* are we building a primary index? */
	double	index_tuples;	/* # of tuples inserted into index */
	bool	is_backfill;	/* are we concurrently backfilling an index? */
} K2PgBuildState;

static void
k2inbuildCallback(Relation index, HeapTuple heapTuple, Datum *values, const bool *isnull,
				   bool tupleIsAlive, void *state)
{
	K2PgBuildState  *buildstate = (K2PgBuildState *)state;

	if (!buildstate->isprimary)
		K2PgExecuteInsertIndex(index,
							  values,
							  (bool *)isnull,
							  heapTuple->t_k2pgctid);

	buildstate->index_tuples += 1;
}

IndexBuildResult *
k2inbuild(Relation heap, Relation index, struct IndexInfo *indexInfo)
{
	K2PgBuildState	buildstate;
	double			heap_tuples = 0;

	/* Do the heap scan */
	buildstate.isprimary = index->rd_index->indisprimary;
	buildstate.index_tuples = 0;
	buildstate.is_backfill = false;
	heap_tuples = IndexBuildHeapScan(heap, index, indexInfo, true, k2inbuildCallback,
									 &buildstate, NULL);

	/*
	 * Return statistics
	 */
	IndexBuildResult *result = (IndexBuildResult *) palloc(sizeof(IndexBuildResult));
	result->heap_tuples  = heap_tuples;
	result->index_tuples = buildstate.index_tuples;
	return result;
}

void
k2inbuildempty(Relation index)
{
	elog(WARNING, "Unexpected building of empty unlogged index");
}

bool
k2ininsert(Relation index, Datum *values, bool *isnull, Datum k2pgctid, Relation heap,
			IndexUniqueCheck checkUnique, struct IndexInfo *indexInfo)
{
	if (!index->rd_index->indisprimary)
		K2PgExecuteInsertIndex(index,
							  values,
							  isnull,
							  k2pgctid);

	return index->rd_index->indisunique ? true : false;
}

void
k2indelete(Relation index, Datum *values, bool *isnull, Datum k2pgctid, Relation heap,
			struct IndexInfo *indexInfo)
{
	if (!index->rd_index->indisprimary)
		K2PgExecuteDeleteIndex(index, values, isnull, k2pgctid);
}

IndexBulkDeleteResult *
k2inbulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
				IndexBulkDeleteCallback callback, void *callback_state)
{
	elog(WARNING, "Unexpected bulk delete of index via vacuum");
	return NULL;
}

IndexBulkDeleteResult *
k2invacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
	elog(WARNING, "Unexpected index cleanup via vacuum");
	return NULL;
}

/* --------------------------------------------------------------------------------------------- */

bool k2incanreturn(Relation index, int attno)
{
	/*
	 * If "canreturn" is true, Postgres will attempt to perform index-only scan on the indexed
	 * columns and expect us to return the column values as an IndexTuple. This will be the case
	 * for secondary index.
	 *
	 * For indexes which are primary keys, we will return the table row as a HeapTuple instead.
	 * For this reason, we set "canreturn" to false for primary keys.
	 */
	return !index->rd_index->indisprimary;
}

void
k2incostestimate(struct PlannerInfo *root, struct IndexPath *path, double loop_count,
				  Cost *indexStartupCost, Cost *indexTotalCost, Selectivity *indexSelectivity,
				  double *indexCorrelation, double *indexPages)
{
	camIndexCostEstimate(path, indexSelectivity, indexStartupCost, indexTotalCost);
}

bytea *
k2inoptions(Datum reloptions, bool validate)
{
	return NULL;
}

bool
k2invalidate(Oid opclassoid)
{
	return true;
}

/* --------------------------------------------------------------------------------------------- */

IndexScanDesc
k2inbeginscan(Relation rel, int nkeys, int norderbys)
{
	IndexScanDesc scan;

	/* no order by operators allowed */
	Assert(norderbys == 0);

	/* get the scan */
	scan = RelationGetIndexScan(rel, nkeys, norderbys);
	scan->opaque = NULL;

	return scan;
}

void
k2inrescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,	ScanKey orderbys, int norderbys)
{
	if (scan->opaque)
	{
		/* For rescan, end the previous scan. */
		k2inendscan(scan);
		scan->opaque = NULL;
	}

	CamScanDesc camScan = camBeginScan(scan->heapRelation, scan->indexRelation, scan->xs_want_itup,
																	 nscankeys, scankey);
	camScan->index = scan->indexRelation;
	scan->opaque = camScan;
}

/*
 * Processing the following SELECT.
 *   SELECT data FROM heapRelation WHERE rowid IN
 *     ( SELECT rowid FROM indexRelation WHERE key = given_value )
 *
 */
bool
k2ingettuple(IndexScanDesc scan, ScanDirection dir)
{
	Assert(dir == ForwardScanDirection || dir == BackwardScanDirection);
	const bool is_forward_scan = (dir == ForwardScanDirection);

	CamScanDesc k2can = (CamScanDesc) scan->opaque;
	k2can->exec_params = scan->k2pg_exec_params;
	if (!is_forward_scan && !k2can->exec_params.limit_use_default) {
		// Ignore limit count for reverse scan since K2 PG cannot push down the limit for reverse scan and
		// rely on PG to process the limit count
		// this only applies if limit_use_default is not true
		k2can->exec_params.limit_count = -1;
	}
	Assert(PointerIsValid(k2can));

	/*
	 * IndexScan(SysTable, Index) --> HeapTuple.
	 */
	scan->xs_ctup.t_k2pgctid = 0;
	if (k2can->prepare_params.index_only_scan)
	{
		IndexTuple tuple = cam_getnext_indextuple(k2can, is_forward_scan, &scan->xs_recheck);
		if (tuple)
		{
			scan->xs_ctup.t_k2pgctid = tuple->t_k2pgctid;
			scan->xs_itup = tuple;
			scan->xs_itupdesc = RelationGetDescr(scan->indexRelation);
		}
	}
	else
	{
		HeapTuple tuple = cam_getnext_heaptuple(k2can, is_forward_scan, &scan->xs_recheck);
		if (tuple)
		{
			scan->xs_ctup.t_k2pgctid = tuple->t_k2pgctid;
			scan->xs_hitup = tuple;
			scan->xs_hitupdesc = RelationGetDescr(scan->heapRelation);
		}
	}

	return scan->xs_ctup.t_k2pgctid != 0;
}

void
k2inendscan(IndexScanDesc scan)
{
	CamScanDesc k2can = (CamScanDesc)scan->opaque;
	Assert(PointerIsValid(k2can));
	camEndScan(k2can);
}
