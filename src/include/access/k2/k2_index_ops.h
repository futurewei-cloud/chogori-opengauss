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


#ifndef K2_INDEX_OPS_H
#define K2_INDEX_OPS_H

#include "access/tableam.h"

/*
 * external entry points for K2PG indexes
 */
extern IndexBuildResult *k2inbuild(Relation heap, Relation index, struct IndexInfo *indexInfo);
extern void k2inbuildempty(Relation index);
extern bool k2ininsert(Relation rel, Datum *values, bool *isnull, Datum k2pgctid, Relation heapRel,
						IndexUniqueCheck checkUnique, struct IndexInfo *indexInfo);
extern void k2indelete(Relation rel, Datum *values, bool *isnull, Datum k2pgctid, Relation heapRel,
						struct IndexInfo *indexInfo);

extern IndexBulkDeleteResult *k2inbulkdelete(IndexVacuumInfo *info,
											  IndexBulkDeleteResult *stats,
											  IndexBulkDeleteCallback callback,
											  void *callback_state);
extern IndexBulkDeleteResult *k2invacuumcleanup(IndexVacuumInfo *info,
												 IndexBulkDeleteResult *stats);

extern bool k2incanreturn(Relation index, int attno);
extern void k2incostestimate(struct PlannerInfo *root,
							  struct IndexPath *path,
							  double loop_count,
							  Cost *indexStartupCost,
							  Cost *indexTotalCost,
							  Selectivity *indexSelectivity,
							  double *indexCorrelation,
							  double *indexPages);
extern bytea *k2inoptions(Datum reloptions, bool validate);

extern bool k2invalidate(Oid opclassoid);

extern IndexScanDesc k2inbeginscan(Relation rel, int nkeys, int norderbys);
extern void k2inrescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
						ScanKey orderbys, int norderbys);
extern bool k2ingettuple(IndexScanDesc scan, ScanDirection dir);
extern void k2inendscan(IndexScanDesc scan);

#endif							/* K2_INDEX_OPS_H */
