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
#include "access/sysattr.h"
#include "catalog/catalog.h"
#include "catalog/index.h"
#include "catalog/pg_am.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_class.h"
#include "catalog/pg_database.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"
#include "commands/dbcommands.h"

#include "utils/lsyscache.h"
#include "utils/relcache.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "executor/tuptable.h"

#include "access/k2/k2cat_cmds.h"
#include "access/k2/pg_gate_api.h"
#include "access/k2/k2pg_aux.h"
#include "access/k2/k2_expr.h"
#include "access/k2/k2_type.h"

#include "access/nbtree.h"
#include "commands/defrem.h"
#include "nodes/nodeFuncs.h"
#include "parser/parser.h"
#include "parser/parse_coerce.h"
#include "parser/parse_type.h"

/* Utility function to calculate column sorting options */
static void
ColumnSortingOptions(SortByDir dir, SortByNulls nulls, bool* is_desc, bool* is_nulls_first)
{
  if (dir == SORTBY_DESC) {
	/*
	 * From postgres doc NULLS FIRST is the default for DESC order.
	 * So SORTBY_NULLS_DEFAULT is equal to SORTBY_NULLS_FIRST here.
	 */
	*is_desc = true;
	*is_nulls_first = (nulls != SORTBY_NULLS_LAST);
  } else {
	/*
	 * From postgres doc ASC is the default sort order and NULLS LAST is the default for it.
	 * So SORTBY_DEFAULT is equal to SORTBY_ASC and SORTBY_NULLS_DEFAULT is equal
	 * to SORTBY_NULLS_LAST here.
	 */
	*is_desc = false;
	*is_nulls_first = (nulls == SORTBY_NULLS_FIRST);
  }
}

/* -------------------------------------------------------------------------- */
/*  Cluster Functions. */
void
K2InitPGCluster()
{
	HandleK2PgStatus(PgGate_InitPrimaryCluster());
}

void
K2FinishInitDB()
{
	HandleK2PgStatus(PgGate_FinishInitDB());
}

/* -------------------------------------------------------------------------- */
/*  Database Functions. */

void
K2PgCreateDatabase(Oid dboid, const char *dbname, Oid src_dboid, Oid next_oid, bool colocated)
{
    (void) colocated;

	HandleK2PgStatus(PgGate_ExecCreateDatabase(dbname,
										  dboid,
										  src_dboid,
                                          next_oid));
}

void
K2PgDropDatabase(Oid dboid, const char *dbname)
{
	HandleK2PgStatus(PgGate_ExecDropDatabase(dbname,
                                             dboid));
}

void
K2PgReservePgOids(Oid dboid, Oid next_oid, uint32 count, Oid *begin_oid, Oid *end_oid)
{
	HandleK2PgStatus(PgGate_ReserveOids(dboid,
									next_oid,
									count,
									begin_oid,
									end_oid));
}

/* ------------------------------------------------------------------------- */
/*  Table Functions. */

static void CreateTableAddColumn(Form_pg_attribute att,
								 bool is_primary,
								 bool is_desc,
								 bool is_nulls_first,
                                 std::vector<K2PGColumnDef>& columns)
{
	const K2PgTypeEntity *col_type = K2PgDataTypeFromOidMod(att->attnum,
															att->atttypid);
    K2PGColumnDef column {
        .attr_name = NameStr(att->attname),
        .attr_num = att->attnum,
        .attr_type = col_type,
        .is_key = is_primary,
        .is_desc = is_desc,
        .is_nulls_first = is_nulls_first
    };

    columns.push_back(std::move(column));
}

/* Utility function to add columns to the K2PG create statement
 * Columns need to be sent in order first hash columns, then rest of primary
 * key columns, then regular columns.
 */
static void CreateTableAddColumns(TupleDesc desc,
								  Constraint *primary_key,
                                  std::vector<K2PGColumnDef>& columns)
{
	/* Add all key columns first with respect to compound key order */
	ListCell *cell;
	if (primary_key != NULL)
	{
        // TODO: didn't port k2pg_index_params to parser logic, i.e., gram.y and parse_utilcmd.cpp, consider removing this new syntax support.
		foreach(cell, primary_key->k2pg_index_params)
		{
			IndexElem *index_elem = (IndexElem *)lfirst(cell);
			bool column_found = false;
			for (int i = 0; i < desc->natts; ++i)
			{
				Form_pg_attribute att = TupleDescAttr(desc, i);
				if (strcmp(NameStr(att->attname), index_elem->name) == 0)
				{
					if (!K2PgDataTypeIsValidForKey(att->atttypid))
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("PRIMARY KEY containing column of type"
										" '%s' not yet supported",
										K2PgTypeOidToStr(att->atttypid))));
					SortByDir order = index_elem->ordering;
					bool is_desc = false;
					bool is_nulls_first = false;
					ColumnSortingOptions(order,
										 index_elem->nulls_ordering,
										 &is_desc,
										 &is_nulls_first);
					CreateTableAddColumn(att,
										 true /* is_primary */,
										 is_desc,
										 is_nulls_first,
                                         columns);
					column_found = true;
					break;
				}
			}
			if (!column_found)
				ereport(FATAL,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Column '%s' not found in table",
								index_elem->name)));
		}
	}

	/* Add all non-key columns */
	for (int i = 0; i < desc->natts; ++i)
	{
		Form_pg_attribute att = TupleDescAttr(desc, i);
		bool is_key = false;
		if (primary_key)
			foreach(cell, primary_key->k2pg_index_params)
			{
				IndexElem *index_elem = (IndexElem *) lfirst(cell);
				if (strcmp(NameStr(att->attname), index_elem->name) == 0)
				{
					is_key = true;
					break;
				}
			}
		if (!is_key)
			CreateTableAddColumn(att,
								 false /* is_primary */,
								 false /* is_desc */,
								 false /* is_nulls_first */,
                                 columns);
	}
}

void
K2PgCreateTable(CreateStmt *stmt, char relkind, TupleDesc desc, Oid relationId, Oid pgNamespaceId)
{
	if (relkind != RELKIND_RELATION)
	{
		return;
	}

	if (stmt->relation->relpersistence == RELPERSISTENCE_TEMP)
	{
		return; /* Nothing to do. */
	}

	ListCell       *listptr;

	char *db_name = get_database_name(u_sess->proc_cxt.MyDatabaseId);
	char *schema_name = stmt->relation->schemaname;
	if (schema_name == NULL)
	{
		schema_name = get_namespace_name(pgNamespaceId);
	}
	if (!IsBootstrapProcessingMode())
		elog(INFO, "Creating Table %s.%s.%s",
					 db_name,
					 schema_name,
					 stmt->relation->relname);

	Constraint *primary_key = NULL;

	foreach(listptr, stmt->constraints)
	{
		Constraint *constraint = (Constraint *)lfirst(listptr);

		if (constraint->contype == CONSTR_PRIMARY)
		{
			primary_key = constraint;
		}
	}

	/* By default, inherit the colocated option from the database */
	bool colocated = MyDatabaseColocated;

	/* Handle user-supplied colocated reloption */
	ListCell *opt_cell;
	foreach(opt_cell, stmt->options)
	{
		DefElem *def = (DefElem *) lfirst(opt_cell);

		if (strcmp(def->defname, "colocated") == 0)
		{
			bool colocated_relopt = defGetBoolean(def);
			if (MyDatabaseColocated)
				colocated = colocated_relopt;
			else if (colocated_relopt)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot set colocated true on a non-colocated"
								" database")));
			/* The following break is fine because there should only be one
			 * colocated reloption at this point due to checks in
			 * parseRelOptions */
			break;
		}
	}

    std::vector<K2PGColumnDef> columns;
	CreateTableAddColumns(desc, primary_key, columns);
	HandleK2PgStatus(PgGate_ExecCreateTable(db_name,
									   schema_name,
									   stmt->relation->relname,
									   u_sess->proc_cxt.MyDatabaseId,
									   relationId,
									   false, /* if_not_exists */
									   primary_key == NULL /* add_primary_key */,
									   columns));
}

void
K2PgDropTable(Oid relationId)
{
	K2PgStatement	handle = NULL;
	bool			colocated = false;

	/* Determine if table is colocated */
	if (MyDatabaseColocated)
	{
		bool not_found = false;
		HandleK2PgStatusIgnoreNotFound(PgGate_IsTableColocated(u_sess->proc_cxt.MyDatabaseId,
																											 relationId,
																											 &colocated),
																 &not_found);
	}

	/* Create table-level tombstone for colocated tables */
	if (colocated)
	{
		bool not_found = false;
		HandleK2PgStatusIgnoreNotFound(PgGate_NewTruncateColocated(u_sess->proc_cxt.MyDatabaseId,
																													 relationId,
																													 false,
																													 &handle),
																 &not_found);
		/* Since the creation of the handle could return a 'NotFound' error,
		 * execute the statement only if the handle is valid.
		 */
		const bool valid_handle = !not_found;
		if (valid_handle)
		{
			HandleK2PgStatusIgnoreNotFound(PgGate_DmlBindTable(handle), &not_found);
			int rows_affected_count = 0;
			HandleK2PgStatusIgnoreNotFound(PgGate_DmlExecWriteOp(handle, &rows_affected_count), &not_found);
		}
	}

	/* Drop the table */
	{
		bool not_found = false;
		HandleK2PgStatusIgnoreNotFound(PgGate_NewDropTable(u_sess->proc_cxt.MyDatabaseId,
																									 relationId,
																									 false, /* if_exists */
																									 &handle),
																 &not_found);
		const bool valid_handle = !not_found;
		if (valid_handle)
		{
			HandleK2PgStatusIgnoreNotFound(PgGate_ExecDropTable(handle), &not_found);
		}
	}
}

void
K2PgTruncateTable(Relation rel) {
	K2PgStatement	handle;
	Oid				relationId = RelationGetRelid(rel);
	bool			colocated = false;

	/* Determine if table is colocated */
	if (MyDatabaseColocated)
		HandleK2PgStatus(PgGate_IsTableColocated(u_sess->proc_cxt.MyDatabaseId,
											 relationId,
											 &colocated));

	if (colocated)
	{
		/* Create table-level tombstone for colocated tables */
		HandleK2PgStatus(PgGate_NewTruncateColocated(u_sess->proc_cxt.MyDatabaseId,
												 relationId,
												 false,
												 &handle));
		HandleK2PgStatus(PgGate_DmlBindTable(handle));
		int rows_affected_count = 0;
		HandleK2PgStatus(PgGate_DmlExecWriteOp(handle, &rows_affected_count));
	}
	else
	{
		/* Send truncate table RPC to master for non-colocated tables */
		HandleK2PgStatus(PgGate_NewTruncateTable(u_sess->proc_cxt.MyDatabaseId,
											 relationId,
											 &handle));
		HandleK2PgStatus(PgGate_ExecTruncateTable(handle));
	}

	if (!rel->rd_rel->relhasindex)
		return;

	/* Truncate the associated secondary indexes */
	List	 *indexlist = RelationGetIndexList(rel);
	ListCell *lc;

	foreach(lc, indexlist)
	{
		Oid indexId = lfirst_oid(lc);

		if (indexId == rel->rd_pkindex)
			continue;

		/* Determine if table is colocated */
		if (MyDatabaseColocated)
			HandleK2PgStatus(PgGate_IsTableColocated(u_sess->proc_cxt.MyDatabaseId,
												 relationId,
												 &colocated));
		if (colocated)
		{
			/* Create table-level tombstone for colocated tables */
			HandleK2PgStatus(PgGate_NewTruncateColocated(u_sess->proc_cxt.MyDatabaseId,
													 relationId,
													 false,
													 &handle));
			HandleK2PgStatus(PgGate_DmlBindTable(handle));
			int rows_affected_count = 0;
			HandleK2PgStatus(PgGate_DmlExecWriteOp(handle, &rows_affected_count));
		}
		else
		{
			/* Send truncate table RPC to master for non-colocated tables */
			HandleK2PgStatus(PgGate_NewTruncateTable(u_sess->proc_cxt.MyDatabaseId,
												 indexId,
												 &handle));
			HandleK2PgStatus(PgGate_ExecTruncateTable(handle));
		}
	}

	list_free(indexlist);
}

void
K2PgCreateIndex(const char *indexName,
			   IndexInfo *indexInfo,
			   TupleDesc indexTupleDesc,
			   int16 *coloptions,
			   Datum reloptions,
			   Oid indexId,
			   Relation rel,
//			   OptSplit *split_options,
			   const bool skip_index_backfill)
{
	char *db_name	  = get_database_name(u_sess->proc_cxt.MyDatabaseId);
	char *schema_name = get_namespace_name(RelationGetNamespace(rel));

	if (!IsBootstrapProcessingMode())
		elog(INFO, "Creating index %s.%s.%s",
					 db_name,
					 schema_name,
					 indexName);

	/* Check reloptions. */
	ListCell	*opt_cell;
	foreach(opt_cell, untransformRelOptions(reloptions))
	{
		DefElem *def = (DefElem *) lfirst(opt_cell);

		if (strcmp(def->defname, "colocated") == 0)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot set option \"%s\" on index",
							def->defname)));
		}
	}

    std::vector<K2PGColumnDef> columns;

	for (int i = 0; i < indexTupleDesc->natts; i++)
	{
		Form_pg_attribute     att         = TupleDescAttr(indexTupleDesc, i);
		char                  *attname    = NameStr(att->attname);
		AttrNumber            attnum      = att->attnum;
		const K2PgTypeEntity *col_type   = K2PgDataTypeFromOidMod(attnum, att->atttypid);
		const bool            is_key      = (i < indexInfo->ii_NumIndexKeyAttrs);

		if (is_key)
		{
			if (!K2PgDataTypeIsValidForKey(att->atttypid))
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("INDEX on column of type '%s' not yet supported",
								K2PgTypeOidToStr(att->atttypid))));
		}

		const int16 options        = coloptions[i];
		const bool  is_desc        = options & INDOPTION_DESC;
		const bool  is_nulls_first = options & INDOPTION_NULLS_FIRST;

        K2PGColumnDef column {
            .attr_name = attname,
            .attr_num = attnum,
            .attr_type = col_type,
            .is_key = is_key,
            .is_desc = is_desc,
            .is_nulls_first = is_nulls_first
        };

        columns.push_back(std::move(column));
	}

	HandleK2PgStatus(PgGate_ExecCreateIndex(db_name,
									   schema_name,
									   indexName,
									   MyDatabaseId,
									   indexId,
									   RelationGetRelid(rel),
									   indexInfo->ii_Unique,
									   skip_index_backfill,
									   false, /* if_not_exists */
									   columns));
}

K2PgStatement
K2PgPrepareAlterTable(AlterTableStmt *stmt, Relation rel, Oid relationId)
{
	K2PgStatement handle = NULL;
	HandleK2PgStatus(PgGate_NewAlterTable(u_sess->proc_cxt.MyDatabaseId,
									  relationId,
									  &handle));

	ListCell *lcmd;
	int col = 1;
	bool needsK2PgAlter = false;

	foreach(lcmd, stmt->cmds)
	{
		AlterTableCmd *cmd = (AlterTableCmd *) lfirst(lcmd);
		switch (cmd->subtype)
		{
			case AT_AddColumn:
			{
				ColumnDef* colDef = (ColumnDef *) cmd->def;
				Oid			typeOid;
				int32		typmod;
				HeapTuple	typeTuple;
				int order;

				/* Skip yb alter for IF NOT EXISTS with existing column */
				if (cmd->missing_ok)
				{
					HeapTuple tuple = SearchSysCacheAttName(RelationGetRelid(rel), colDef->colname);
					if (HeapTupleIsValid(tuple)) {
						ReleaseSysCache(tuple);
						break;
					}
				}

				typeTuple = typenameType(NULL, colDef->typname, &typmod);
				typeOid = HeapTupleGetOid(typeTuple);
				order = RelationGetNumberOfAttributes(rel) + col;
				const K2PgTypeEntity *col_type = K2PgDataTypeFromOidMod(order, typeOid);

				HandleK2PgStatus(PgGate_AlterTableAddColumn(handle, colDef->colname,
																										order, col_type,
																										colDef->is_not_null));
				++col;
				ReleaseSysCache(typeTuple);
				needsK2PgAlter = true;

				break;
			}
			case AT_DropColumn:
			{
				/* Skip yb alter for IF EXISTS with non-existent column */
				if (cmd->missing_ok)
				{
					HeapTuple tuple = SearchSysCacheAttName(RelationGetRelid(rel), cmd->name);
					if (!HeapTupleIsValid(tuple))
						break;
					ReleaseSysCache(tuple);
				}

				HandleK2PgStatus(PgGate_AlterTableDropColumn(handle, cmd->name));
				needsK2PgAlter = true;

				break;
			}

			case AT_AddIndex:
			case AT_AddIndexConstraint:
			{
				IndexStmt *index = (IndexStmt *) cmd->def;
				/* Only allow adding indexes when it is a unique non-primary-key constraint */
				if (!index->unique || index->primary || !index->isconstraint)
				{
					ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("This ALTER TABLE command is not yet supported.")));
				}

				break;
			}

			case AT_AddConstraint:
			case AT_DropConstraint:
			case AT_DropOids:
			case AT_EnableTrig:
			case AT_EnableAlwaysTrig:
			case AT_EnableReplicaTrig:
			case AT_EnableTrigAll:
			case AT_EnableTrigUser:
			case AT_DisableTrig:
			case AT_DisableTrigAll:
			case AT_DisableTrigUser:
			case AT_ChangeOwner:
			case AT_ColumnDefault:
			case AT_DropNotNull:
			case AT_SetNotNull:
				/* For these cases a K2PG alter isn't required, so we do nothing. */
				break;

			default:
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("This ALTER TABLE command is not yet supported.")));
				break;
		}
	}

	if (!needsK2PgAlter)
	{
		return NULL;
	}

	return handle;
}

void
K2PgExecAlterPgTable(K2PgStatement handle, Oid relationId)
{
	if (handle)
	{
		if (IsK2PgRelationById(relationId)) {
			HandleK2PgStatus(PgGate_ExecAlterTable(handle));
		}
	}
}

void
K2PgRename(RenameStmt *stmt, Oid relationId)
{
	K2PgStatement handle = NULL;
	char *db_name	  = get_database_name(u_sess->proc_cxt.MyDatabaseId);

	switch (stmt->renameType)
	{
		case OBJECT_TABLE:
			HandleK2PgStatus(PgGate_NewAlterTable(u_sess->proc_cxt.MyDatabaseId,
											  relationId,
											  &handle));
			HandleK2PgStatus(PgGate_AlterTableRenameTable(handle, db_name, stmt->newname));
			break;

		case OBJECT_COLUMN:
		case OBJECT_ATTRIBUTE:

			HandleK2PgStatus(PgGate_NewAlterTable(u_sess->proc_cxt.MyDatabaseId,
											  relationId,
											  &handle));

			HandleK2PgStatus(PgGate_AlterTableRenameColumn(handle, stmt->subname, stmt->newname));
			break;

		default:
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("Renaming this object is not yet supported.")));

	}

	K2PgExecAlterPgTable(handle, relationId);
}

void
K2PgDropIndex(Oid relationId)
{
	K2PgStatement	handle;
	bool			colocated = false;

	/* Determine if table is colocated */
	if (MyDatabaseColocated)
	{
		bool not_found = false;
		HandleK2PgStatusIgnoreNotFound(PgGate_IsTableColocated(u_sess->proc_cxt.MyDatabaseId,
																											 relationId,
																											 &colocated),
																 &not_found);
	}

	/* Create table-level tombstone for colocated tables */
	if (colocated)
	{
		bool not_found = false;
		HandleK2PgStatusIgnoreNotFound(PgGate_NewTruncateColocated(u_sess->proc_cxt.MyDatabaseId,
																													 relationId,
																													 false,
																													 &handle),
																 &not_found);
		const bool valid_handle = !not_found;
		if (valid_handle) {
			HandleK2PgStatusIgnoreNotFound(PgGate_DmlBindTable(handle), &not_found);
			int rows_affected_count = 0;
			HandleK2PgStatusIgnoreNotFound(PgGate_DmlExecWriteOp(handle, &rows_affected_count), &not_found);
		}
	}

	/* Drop the index table */
	{
		bool not_found = false;
		HandleK2PgStatusIgnoreNotFound(PgGate_NewDropIndex(u_sess->proc_cxt.MyDatabaseId,
																									 relationId,
																									 false, /* if_exists */
																									 &handle),
																 &not_found);
		const bool valid_handle = !not_found;
		if (valid_handle) {
			HandleK2PgStatusIgnoreNotFound(PgGate_ExecDropIndex(handle), &not_found);
		}
	}
}
