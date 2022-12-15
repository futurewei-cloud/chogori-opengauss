#include "libintl.h"
#include "postgres.h"
#include "access/k2/k2pg_aux.h"

#include "utils/elog.h"
#include "access/sysattr.h"
#include "utils/resowner.h"
#include "utils/builtins.h"
#include "utils/palloc.h"
#include "utils/lsyscache.h"
#include "utils/relcache.h"
#include "catalog/pg_database.h"
#include "commands/dbcommands.h"
#include "tcop/utility.h"

#include "access/k2/pg_gate_typedefs.h"
#include "access/k2/k2_type.h"
#include "access/k2/pg_gate_api.h"

#include <iostream>
//#include "access/k2/log.h"

#define BOOST_STACKTRACE_USE_ADDR2LINE
#include "boost/stacktrace.hpp"
#include <string>

uint64_t k2pg_catalog_cache_version = K2PG_CATCACHE_VERSION_UNINITIALIZED;

/** These values are lazily initialized based on corresponding environment variables. */
int k2pg_pg_double_write = -1;
int k2pg_disable_pg_locking = -1;


bool IsK2PgEnabled()
{
	/* We do not support Init/Bootstrap processing modes yet. */
	return PgGate_IsK2PgEnabled();
}

void CheckIsK2PgSupportedRelation(Relation relation)
{
	const char relkind = relation->rd_rel->relkind;
	CheckIsK2PgSupportedRelationByKind(relkind);
}

void CheckIsK2PgSupportedRelationByKind(char relkind)
{
	if (!(relkind == RELKIND_RELATION || relkind == RELKIND_INDEX ||
		  relkind == RELKIND_VIEW || relkind == RELKIND_SEQUENCE ||
		  relkind == RELKIND_COMPOSITE_TYPE))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("This feature is not supported in K2PG.")));
}

bool IsK2PgRelation(Relation relation)
{
	if (!IsK2PgEnabled()) return false;

	const char relkind = relation->rd_rel->relkind;

	CheckIsK2PgSupportedRelationByKind(relkind);

	/* Currently only support regular tables and indexes.
	 * Temp tables and views are supported, but they are not K2PG relations. */
	return (relkind == RELKIND_RELATION || relkind == RELKIND_INDEX || relation->rd_rel->relkind == RELKIND_VIEW)
				 && relation->rd_rel->relpersistence != RELPERSISTENCE_TEMP;
}

bool IsK2PgRelationById(Oid relid)
{
	Relation relation     = RelationIdGetRelation(relid);
	bool     is_supported = IsK2PgRelation(relation);
	RelationClose(relation);
	return is_supported;
}

bool IsK2PgBackedRelation(Relation relation)
{
	return IsK2PgRelation(relation) ||
		(relation->rd_rel->relkind == RELKIND_VIEW &&
		relation->rd_rel->relpersistence != RELPERSISTENCE_TEMP);
}

bool
K2PgNeedRetryAfterCacheRefresh(ErrorData *edata)
{
	// TODO Inspect error code to distinguish retryable errors.
	return true;
}

AttrNumber K2PgGetFirstLowInvalidAttributeNumber(Relation relation)
{
	return IsK2PgRelation(relation)
	       ? K2PgFirstLowInvalidAttributeNumber
	       : FirstLowInvalidHeapAttributeNumber;
}

AttrNumber K2PgGetFirstLowInvalidAttributeNumberFromOid(Oid relid)
{
	Relation   relation = RelationIdGetRelation(relid);
	AttrNumber attr_num = K2PgGetFirstLowInvalidAttributeNumber(relation);
	RelationClose(relation);
	return attr_num;
}

int K2PgAttnumToBmsIndex(Relation rel, AttrNumber attnum)
{
	return attnum - K2PgGetFirstLowInvalidAttributeNumber(rel);
}

AttrNumber K2PgBmsIndexToAttnum(Relation rel, int idx)
{
	return idx + K2PgGetFirstLowInvalidAttributeNumber(rel);
}


extern bool K2PgRelHasOldRowTriggers(Relation rel, CmdType operation)
{
	TriggerDesc *trigdesc = rel->trigdesc;
	return (trigdesc &&
		((operation == CMD_UPDATE &&
			(trigdesc->trig_update_after_row ||
			trigdesc->trig_update_before_row)) ||
		(operation == CMD_DELETE &&
			(trigdesc->trig_delete_after_row ||
			trigdesc->trig_delete_before_row))));
}

bool K2PgRelHasSecondaryIndices(Relation relation)
{
	if (!relation->rd_rel->relhasindex)
		return false;

	bool	 has_indices = false;
	List	 *indexlist = RelationGetIndexList(relation);
	ListCell *lc;

	foreach(lc, indexlist)
	{
		if (lfirst_oid(lc) == relation->rd_pkindex)
			continue;
		has_indices = true;
		break;
	}

	list_free(indexlist);

	return has_indices;
}

bool K2PgTransactionsEnabled()
{
	static int cached_value = -1;
	if (cached_value == -1)
	{
		cached_value = K2PgIsEnvVarTrueWithDefault("K2PG_TRANSACTIONS_ENABLED", true);
	}
	return IsK2PgEnabled() && cached_value;
}

void K2PgReportFeatureUnsupported(const char *msg)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("%s", msg)));
}


static bool K2PgShouldReportErrorStatus()
{
	static int cached_value = -1;
	if (cached_value == -1)
	{
		cached_value = K2PgIsEnvVarTrue("K2PG_REPORT_ERROR_STATUS");
	}

	return cached_value;
}

void HandleK2PgStatus(const K2PgStatus& status)
{
    if (status.pg_code == ERRCODE_SUCCESSFUL_COMPLETION) {
        return;
    }

	std::string backtrace = boost::stacktrace::to_string(boost::stacktrace::stacktrace());
    elog(INFO, "Status: %s: %s, stacktrace: %s", status.msg.c_str(), status.detail.c_str(), backtrace.c_str());

 //	std::cout << "Error status: " << status.msg << " with stacktrace: " << k2log::TR << std::endl;
    ereport(ERROR, (errcode(status.pg_code), errmsg("Status: %s: %s", status.msg.c_str(), status.detail.c_str())));
}

void HandleK2PgStatusIgnoreNotFound(const K2PgStatus& status, bool *not_found)
{
	if (status.k2_code == 404) {
		*not_found = true;
		return;
	}
	*not_found = false;
	HandleK2PgStatus(status);
}

void HandleK2PgTableDescStatus(const K2PgStatus& status, K2PgTableDesc table)
{
	HandleK2PgStatus(status);
}

/*
 * Fetches relation's unique constraint name to specified buffer.
 * If relation is not an index and it has primary key the name of primary key index is returned.
 * In other cases, relation name is used.
 */
static void FetchUniqueConstraintName(Oid relation_id, char* dest, size_t max_size)
{
	// strncat appends source to destination, so destination must be empty.
	dest[0] = 0;
	Relation rel = RelationIdGetRelation(relation_id);

	if (!rel->rd_index && rel->rd_pkindex != InvalidOid)
	{
		Relation pkey = RelationIdGetRelation(rel->rd_pkindex);

		strncat(dest, RelationGetRelationName(pkey), max_size);

		RelationClose(pkey);
	} else
	{
		strncat(dest, RelationGetRelationName(rel), max_size);
	}

	RelationClose(rel);
}

void K2PgInitPostgresBackend(const char *program_name)
{
	if (K2PgIsEnabledInPostgresEnvVar() && !g_instance.k2_cxt.isK2ModelEnabled) {
		g_instance.k2_cxt.isK2ModelEnabled = true;
	}
	/*
	 * Enable "K2PG mode" for PostgreSQL globally, the session will be initialized by each individual thread
	 */
	if (g_instance.k2_cxt.isK2ModelEnabled) {
		elog(INFO, "Initializing K2PG backend for %s", program_name);
	    HandleK2PgStatus(K2PgInit(program_name, (K2PgPAllocFn)palloc, (K2PgCStringToTextWithLenFn)cstring_to_text_with_len));
        PgGate_InitPgGate();
	}
}

void K2PgInitSession(const char *db_name) {
	/*
	 * For each process, we create one K2PG session for PostgreSQL to use
	 * when accessing K2PG storage.
	 *
	*/
	if (IsK2PgEnabled()) {
		if (db_name != NULL) {
			elog(INFO, "Initialize K2PG session for database: %s", db_name);
    		HandleK2PgStatus(PgGate_InitSession(db_name));
		} else {
			elog(INFO, "database name is null when Initialize K2PG session, use default database %s ", DEFAULT_DATABASE);
			HandleK2PgStatus(PgGate_InitSession(DEFAULT_DATABASE));
		}
	} else {
		ereport(ERROR, (errmsg("K2PG backend has not been initialized for database: %s", db_name)));
	}
}

void K2PgOnPostgresBackendShutdown()
{
	PgGate_DestroyPgGate();
}

static bool k2pg_preparing_templates = false;
void K2PgSetPreparingTemplates() {
	k2pg_preparing_templates = true;
}

bool K2PgIsPreparingTemplates() {
	return k2pg_preparing_templates;
}

const char*
K2PgTypeOidToStr(Oid type_id) {
	switch (type_id) {
		case BOOLOID: return "BOOL";
		case BYTEAOID: return "BYTEA";
		case CHAROID: return "CHAR";
		case NAMEOID: return "NAME";
		case INT8OID: return "INT8";
		case INT2OID: return "INT2";
		case INT2VECTOROID: return "INT2VECTOR";
		case INT4OID: return "INT4";
		case REGPROCOID: return "REGPROC";
		case TEXTOID: return "TEXT";
		case OIDOID: return "OID";
		case TIDOID: return "TID";
		case XIDOID: return "XID";
		case CIDOID: return "CID";
		case OIDVECTOROID: return "OIDVECTOR";
		case JSONOID: return "JSON";
		case XMLOID: return "XML";
		case PGNODETREEOID: return "PGNODETREE";
//		case PGNDISTINCTOID: return "PGNDISTINCT";
//		case PGDEPENDENCIESOID: return "PGDEPENDENCIES";
//		case PGDDLCOMMANDOID: return "PGDDLCOMMAND";
		case POINTOID: return "POINT";
		case LSEGOID: return "LSEG";
		case PATHOID: return "PATH";
		case BOXOID: return "BOX";
		case POLYGONOID: return "POLYGON";
		case LINEOID: return "LINE";
		case FLOAT4OID: return "FLOAT4";
		case FLOAT8OID: return "FLOAT8";
		case ABSTIMEOID: return "ABSTIME";
		case RELTIMEOID: return "RELTIME";
		case TINTERVALOID: return "TINTERVAL";
		case UNKNOWNOID: return "UNKNOWN";
		case CIRCLEOID: return "CIRCLE";
		case CASHOID: return "CASH";
		case MACADDROID: return "MACADDR";
		case INETOID: return "INET";
		case CIDROID: return "CIDR";
//		case MACADDR8OID: return "MACADDR8";
		case INT2ARRAYOID: return "INT2ARRAY";
		case INT4ARRAYOID: return "INT4ARRAY";
		case TEXTARRAYOID: return "TEXTARRAY";
//		case OIDARRAYOID: return "OIDARRAY";
		case FLOAT4ARRAYOID: return "FLOAT4ARRAY";
		case ACLITEMOID: return "ACLITEM";
		case CSTRINGARRAYOID: return "CSTRINGARRAY";
		case BPCHAROID: return "BPCHAR";
		case VARCHAROID: return "VARCHAR";
		case DATEOID: return "DATE";
		case TIMEOID: return "TIME";
		case TIMESTAMPOID: return "TIMESTAMP";
		case TIMESTAMPTZOID: return "TIMESTAMPTZ";
		case INTERVALOID: return "INTERVAL";
		case TIMETZOID: return "TIMETZ";
		case BITOID: return "BIT";
		case VARBITOID: return "VARBIT";
		case NUMERICOID: return "NUMERIC";
		case REFCURSOROID: return "REFCURSOR";
		case REGPROCEDUREOID: return "REGPROCEDURE";
		case REGOPEROID: return "REGOPER";
		case REGOPERATOROID: return "REGOPERATOR";
		case REGCLASSOID: return "REGCLASS";
		case REGTYPEOID: return "REGTYPE";
//		case REGROLEOID: return "REGROLE";
//		case REGNAMESPACEOID: return "REGNAMESPACE";
		case REGTYPEARRAYOID: return "REGTYPEARRAY";
		case UUIDOID: return "UUID";
//		case LSNOID: return "LSN";
		case TSVECTOROID: return "TSVECTOR";
		case GTSVECTOROID: return "GTSVECTOR";
		case TSQUERYOID: return "TSQUERY";
		case REGCONFIGOID: return "REGCONFIG";
		case REGDICTIONARYOID: return "REGDICTIONARY";
		case JSONBOID: return "JSONB";
		case INT4RANGEOID: return "INT4RANGE";
		case RECORDOID: return "RECORD";
		case RECORDARRAYOID: return "RECORDARRAY";
		case CSTRINGOID: return "CSTRING";
		case ANYOID: return "ANY";
		case ANYARRAYOID: return "ANYARRAY";
		case VOIDOID: return "VOID";
		case TRIGGEROID: return "TRIGGER";
//		case EVTTRIGGEROID: return "EVTTRIGGER";
		case LANGUAGE_HANDLEROID: return "LANGUAGE_HANDLER";
		case INTERNALOID: return "INTERNAL";
		case OPAQUEOID: return "OPAQUE";
		case ANYELEMENTOID: return "ANYELEMENT";
		case ANYNONARRAYOID: return "ANYNONARRAY";
		case ANYENUMOID: return "ANYENUM";
		case FDW_HANDLEROID: return "FDW_HANDLER";
//		case INDEX_AM_HANDLEROID: return "INDEX_AM_HANDLER";
//		case TSM_HANDLEROID: return "TSM_HANDLER";
		case ANYRANGEOID: return "ANYRANGE";
		default: return "user_defined_type";
	}
}

const char*
K2PgGetSchemaName(Oid schemaoid)
{
	/*
	 * Hardcode the names for system namespaces since the cache might not
	 * be initialized during initdb (bootstrap mode).
	 * TODO Eventually K2PG should switch to using oid's everywhere so
	 * that dbname and schemaname should not be needed at all.
	 */
	if (IsSystemNamespace(schemaoid))
		return "pg_catalog";
	else if (IsToastNamespace(schemaoid))
		return "pg_toast";
	else
		return get_namespace_name(schemaoid);
}

Oid
K2PgGetDatabaseOid(Relation rel)
{
	return rel->rd_rel->relisshared ? TemplateDbOid : u_sess->proc_cxt.MyDatabaseId;
}

//------------------------------------------------------------------------------
// Debug utils.

bool k2pg_debug_mode = false;

const char*
K2PgDatumToString(Datum datum, Oid typid)
{
	Oid			typoutput = InvalidOid;
	bool		typisvarlena = false;

	getTypeOutputInfo(typid, &typoutput, &typisvarlena);
	return OidOutputFunctionCall(typoutput, datum);
}

const char*
K2PgHeapTupleToString(HeapTuple tuple, TupleDesc tupleDesc)
{
	Datum attr = (Datum) 0;
	int natts = tupleDesc->natts;
	bool isnull = false;
	StringInfoData buf;
	initStringInfo(&buf);

	appendStringInfoChar(&buf, '(');
	for (int attnum = 1; attnum <= natts; ++attnum) {
		attr = heap_getattr(tuple, attnum, tupleDesc, &isnull);
		if (isnull)
		{
			appendStringInfoString(&buf, "null");
		}
		else
		{
			Oid typid = TupleDescAttr(tupleDesc, attnum - 1)->atttypid;
			appendStringInfoString(&buf, K2PgDatumToString(attr, typid));
		}
		if (attnum != natts) {
			appendStringInfoString(&buf, ", ");
		}
	}
	appendStringInfoChar(&buf, ')');
	return buf.data;
}

bool K2PgIsInitDbAlreadyDone()
{
	bool done = false;
	HandleK2PgStatus(PgGate_IsInitDbDone(&done));
	return done;
}

/*---------------------------------------------------------------------------*/
/* Transactional DDL support                                                 */
/*---------------------------------------------------------------------------*/

static ProcessUtility_hook_type prev_ProcessUtility = NULL;
static int ddl_nesting_level = 0;

int
K2PgGetDdlNestingLevel()
{
	return ddl_nesting_level;
}

void
K2PgIncrementDdlNestingLevel()
{
	if (ddl_nesting_level == 0)
		PgGate_EnterSeparateDdlTxnMode();
	ddl_nesting_level++;
}

void
K2PgDecrementDdlNestingLevel(bool success)
{
	ddl_nesting_level--;
	if (ddl_nesting_level == 0)
		PgGate_ExitSeparateDdlTxnMode(success);
}

static bool IsTransactionalDdlStatement(NodeTag node_tag) {
	switch (node_tag) {
		// The lists of tags here have been generated using e.g.:
		// cat $( find src/postgres -name "nodes.h" ) | grep "T_Create" | sort | uniq |
		//   sed 's/,//g' | while read s; do echo -e "\t\tcase $s:"; done
		// All T_Create... tags from nodes.h:
//		case T_CreateAmStmt:
		case T_CreateCastStmt:
		case T_CreateConversionStmt:
		case T_CreateDomainStmt:
		case T_CreateEnumStmt:
//		case T_CreateEventTrigStmt:
		case T_CreateExtensionStmt:
		case T_CreateFdwStmt:
		case T_CreateForeignServerStmt:
		case T_CreateForeignTableStmt:
		case T_CreateFunctionStmt:
		case T_CreateOpClassItem:
		case T_CreateOpClassStmt:
		case T_CreateOpFamilyStmt:
		case T_CreatePLangStmt:
//		case T_CreatePolicyStmt:
//		case T_CreatePublicationStmt:
		case T_CreateRangeStmt:
		case T_CreateReplicationSlotCmd:
		case T_CreateRoleStmt:
		case T_CreateSchemaStmt:
		case T_CreateSeqStmt:
//		case T_CreateStatsStmt:
		case T_CreateStmt:
//		case T_CreateSubscriptionStmt:
		case T_CreateTableAsStmt:
		case T_CreateTableSpaceStmt:
//		case T_CreateTransformStmt:
		case T_CreateTrigStmt:
		case T_CreateUserMappingStmt:
		case T_CreatedbStmt:
		// All T_Drop... tags from nodes.h:
		case T_DropOwnedStmt:
		case T_DropReplicationSlotCmd:
		case T_DropRoleStmt:
		case T_DropStmt:
//		case T_DropSubscriptionStmt:
		case T_DropTableSpaceStmt:
		case T_DropUserMappingStmt:
		case T_DropdbStmt:
		// All T_Alter... tags from nodes.h:
//		case T_AlterCollationStmt:
		case T_AlterDatabaseSetStmt:
		case T_AlterDatabaseStmt:
		case T_AlterDefaultPrivilegesStmt:
		case T_AlterDomainStmt:
		case T_AlterEnumStmt:
//		case T_AlterEventTrigStmt:
		case T_AlterExtensionContentsStmt:
		case T_AlterExtensionStmt:
		case T_AlterFdwStmt:
		case T_AlterForeignServerStmt:
		case T_AlterFunctionStmt:
//		case T_AlterObjectDependsStmt:
		case T_AlterObjectSchemaStmt:
		case T_AlterOpFamilyStmt:
//		case T_AlterOperatorStmt:
		case T_AlterOwnerStmt:
//		case T_AlterPolicyStmt:
//		case T_AlterPublicationStmt:
		case T_AlterRoleSetStmt:
		case T_AlterRoleStmt:
		case T_AlterSeqStmt:
//		case T_AlterSubscriptionStmt:
		case T_AlterSystemStmt:
		case T_AlterTSConfigurationStmt:
		case T_AlterTSDictionaryStmt:
		case T_AlterTableCmd:
//		case T_AlterTableMoveAllStmt:
		case T_AlterTableSpaceOptionsStmt:
		case T_AlterTableStmt:
		case T_AlterUserMappingStmt:
		case T_AlternativeSubPlan:
		case T_AlternativeSubPlanState:
		// T_Grant...
		case T_GrantStmt:
		case T_GrantRoleStmt:
		// T_Index...
		case T_IndexStmt:
			return true;
		default:
			return false;
	}
}

bool IsK2PgLocalNodeInitdbMode()
{
	return K2PgIsEnvVarTrue("K2PG_LOCAL_NODE_INITDB");
}

bool K2PgIsEnvVarTrue(const char* env_var_name)
{
	return K2PgIsEnvVarTrueWithDefault(env_var_name, /* default_value */ false);
}

bool K2PgIsEnvVarTrueWithDefault(const char* env_var_name, bool default_value)
{
	const char* env_var_value = getenv(env_var_name);
	if (!env_var_value ||
		strlen(env_var_value) == 0 ||
		strcmp(env_var_value, "auto") == 0)
	{
		return default_value;
	}
	return strcmp(env_var_value, "1") == 0 || strcmp(env_var_value, "true") == 0;
}

bool K2PgIsEnabledInPostgresEnvVar()
{
	static int cached_value = -1;
	if (cached_value == -1)
	{
		cached_value = K2PgIsEnvVarTrue("K2PG_ENABLED_IN_POSTGRES");
	}
	return cached_value;
}

bool
K2PgShouldAllowRunningAsAnyUser()
{
	if (K2PgIsEnabledInPostgresEnvVar())
    {
		return true;
	}
	static int cached_value = -1;
	if (cached_value == -1)
    {
		cached_value = K2PgIsEnvVarTrue("K2PG_ALLOW_RUNNING_AS_ANY_USER");
	}
	return cached_value;
}

bool K2PgIsInitDbModeEnvVarSet()
{

	static int cached_value = -1;
	if (cached_value == -1)
    {
		cached_value = K2PgIsEnvVarTrue("K2PG_INITDB_MODE");
	}
	return cached_value;
}

void K2PgSetInitDbModeEnvVar()
{
	int setenv_retval = setenv("K2PG_INITDB_MODE", "1", /* overwrite */ true);
	if (setenv_retval != 0)
	{
		perror("Could not set environment variable K2PG_INITDB_MODE");
		exit(EXIT_FAILURE);
	}
}

bool
IsUsingK2PGParser()
{
	static int cached_value = -1;
	if (cached_value == -1) {
		cached_value = !K2PgIsInitDbModeEnvVarSet() && K2PgIsEnabledInPostgresEnvVar();
	}
	return cached_value;
}

int
K2PgUnsupportedFeatureSignalLevel()
{
	static int cached_value = -1;
	if (cached_value == -1) {
		cached_value = K2PgIsEnvVarTrue("K2PG_SUPPRESS_UNSUPPORTED_ERROR") ||
									 K2PgIsEnvVarTrue("FLAGS_psql_suppress_unsupported_error") ? WARNING : ERROR;
	}
	return cached_value;
}

bool
K2PgIsNonTxnCopyEnabled()
{
	static int cached_value = -1;
	if (cached_value == -1)
	{
		cached_value = K2PgIsEnvVarTrue("FLAGS_psql_non_txn_copy");
	}
	return cached_value;
}
