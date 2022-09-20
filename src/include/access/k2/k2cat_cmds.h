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
#ifndef K2CAT_CMDS_H
#define K2CAT_CMDS_H

#include "access/htup.h"
#include "catalog/dependency.h"
#include "catalog/objectaddress.h"
#include "nodes/execnodes.h"
#include "nodes/parsenodes.h"
#include "utils/relcache.h"

#include "access/k2/pg_gate_api.h"

/* K2 Cluster Fuctions -------------------------------------------------------------------------- */
extern void K2InitPGCluster();

extern void K2FinishInitDB();

/*  Database Functions -------------------------------------------------------------------------- */

extern void K2PgCreateDatabase(
	Oid dboid, const char *dbname, Oid src_dboid, Oid next_oid);

extern void K2PgDropDatabase(Oid dboid, const char *dbname);

extern void K2PgReservePgOids(Oid dboid, Oid next_oid, uint32 count, Oid *begin_oid, Oid *end_oid);

/*  Table Functions ----------------------------------------------------------------------------- */

extern void K2PgCreateTable(CreateStmt *stmt,
						   char relkind,
						   TupleDesc desc,
						   Oid relationId,
						   Oid namespaceId);

extern void K2PgDropTable(Oid relationId);

extern void K2PgCreateIndex(const char *indexName,
						   IndexInfo *indexInfo,
						   TupleDesc indexTupleDesc,
						   int16 *coloptions,
						   Datum reloptions,
						   Oid indexId,
						   Relation rel,
						   const bool skip_index_backfill);

extern void K2PgDropIndex(Oid relationId);

extern K2PgStatement K2PgPrepareAlterTable(AlterTableStmt* stmt, Relation rel, Oid relationId);

extern void K2PgExecAlterPgTable(K2PgStatement handle, Oid relationId);

extern void K2PgRename(RenameStmt* stmt, Oid relationId);

#endif
