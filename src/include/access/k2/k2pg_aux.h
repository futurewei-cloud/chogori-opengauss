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

#ifndef K2PG_AUX_H
#define K2PG_AUX_H

#include "postgres.h"
#include "utils/relcache.h"
#include "access/reloptions.h"
#include "utils/resowner.h"

#include "access/k2/k2pg_util.h"
#include "access/k2/pg_gate_api.h"

#define DEFAULT_K2PG_INDEX_TYPE	"k2index"

/*
 * Version of the catalog entries in the relcache and catcache.
 * We (only) rely on a following invariant: If the catalog cache version here is
 * actually the latest (master) version, then the catalog data is indeed up to
 * date. In any other case, we will end up doing a cache refresh anyway.
 * (I.e. cache data is only valid if the version below matches with master's
 * version, otherwise all bets are off and we need to refresh.)
 *
 * So we should handle cases like:
 * 1. k2pg_catalog_cache_version being behind the actual data in the caches.
 * 2. Data in the caches spanning multiple version (because catalog was updated
 *    during a cache refresh).
 * As long as the invariant above is not violated we should (at most) end up
 * doing a redundant cache refresh.
 *
 * TODO: Improve cache versioning and refresh logic to be more fine-grained to
 * reduce frequency and/or duration of cache refreshes.
 */
extern uint64_t k2pg_catalog_cache_version;

#define K2PG_CATCACHE_VERSION_UNINITIALIZED (0)

/*
 * Checks whether K2PG functionality is enabled within PostgreSQL.
 * This relies on pgapi being non-NULL, so probably should not be used
 * in postmaster (which does not need to talk to K2 backend) or early
 * in backend process initialization. In those cases the
 * K2PgIsEnabledInPostgresEnvVar function might be more appropriate.
 */
extern bool IsK2PgEnabled();

/*
 * Given a relation, checks whether the relation is supported in K2PG mode.
 */
extern void CheckIsK2PgSupportedRelation(Relation relation);

extern void CheckIsK2PgSupportedRelationByKind(char relkind);

/*
 * Given a relation (table) id, returns whether this table is handled by
 * K2PG: i.e. it is not a temporary or foreign table.
 */
extern bool IsK2PgRelationById(Oid relid);

extern bool IsK2PgRelation(Relation relation);

/*
 * Same as IsK2PgRelation but it additionally includes views on K2PG
 * relations i.e. views on persistent (non-temporary) tables.
 */
extern bool IsK2PgBackedRelation(Relation relation);

extern bool K2PgNeedRetryAfterCacheRefresh(ErrorData *edata);

extern void K2PgReportFeatureUnsupported(const char *err_msg);

extern AttrNumber K2PgGetFirstLowInvalidAttributeNumber(Relation relation);

extern AttrNumber K2PgGetFirstLowInvalidAttributeNumberFromOid(Oid relid);

extern int K2PgAttnumToBmsIndex(Relation rel, AttrNumber attnum);

extern AttrNumber K2PgBmsIndexToAttnum(Relation rel, int idx);

/*
 * Check if a relation has row triggers that may reference the old row.
 * Specifically for an update/delete DML (where there actually is an old row).
 */
extern bool K2PgRelHasOldRowTriggers(Relation rel, CmdType operation);

/*
 * Check if a relation has secondary indices.
 */
extern bool K2PgRelHasSecondaryIndices(Relation relation);

/*
 * Whether to route BEGIN / COMMIT / ROLLBACK to K2PG's distributed
 * transactions.
 */
extern bool K2PgTransactionsEnabled();

/*
 * Given a status returned by K2 PgGate C++ code, reports that status using ereport if
 * it is not OK.
 */
extern void	HandleK2PgStatus(const K2PgStatus& status);

/*
 * Since DDL metadata in K2 platform and postgres system tables is not modified
 * in an atomic fashion, it is possible that we could have a table existing in
 * postgres metadata but not in K2 platform. In the case of a delete it is really
 * problematic, since we can't delete the table nor can we create a new one with
 * the same name. So in this case we just ignore the K2 platform 'NotFound' error and
 * delete our metadata.
 */
extern void HandleK2PgStatusIgnoreNotFound(const K2PgStatus& status, bool *not_found);

/*
 * Same as HandleK2PgStatus but also ask the given resource owner to forget
 * the given K2PG statement.
 */
extern void HandleK2PgStatusWithOwner(const K2PgStatus& status,
																		K2PgScanHandle* k2pg_stmt,
																		ResourceOwner owner);

/*
 * Same as HandleK2PgStatus but delete the table description first if the
 * status is not ok.
 */
extern void HandleK2PgTableDescStatus(const K2PgStatus& status, K2PgTableDesc table);
/*
 * K2PG initialization that needs to happen when a PostgreSQL backend process
 * is started. Reports errors using ereport.
 */
extern void K2PgInitPostgresBackend(const char *program_name,
								  const char *db_name,
								  const char *user_name);

/*
 * This should be called on all exit paths from the PostgreSQL backend process.
 * Only main PostgreSQL backend thread is expected to call this.
 */
extern void K2PgOnPostgresBackendShutdown();

/*
 * Return a string representation of the given type id, or say it is unknown.
 * What is returned is always a static C string constant.
 */
extern const char* K2PgTypeOidToStr(Oid type_id);

/*
 * Report an error saying the given type as not supported by K2PG.
 */
extern void K2PgReportTypeNotSupported(Oid type_id);

#define K2PG_REPORT_TYPE_NOT_SUPPORTED(type_id) do { \
		Oid computed_type_id = type_id; \
		ereport(ERROR, \
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED), \
					errmsg("Type not yet supported in K2PG: %d (%s)", \
						computed_type_id, K2PgTypeOidToStr(computed_type_id)))); \
	} while (0)

/*
 * These functions help indicating if we are creating system catalog.
 */
void K2PgSetPreparingTemplates();
bool K2PgIsPreparingTemplates();

/*
 * Get the schema name for a schema oid (accounts for system namespaces)
 */
const char* K2PgGetSchemaName(Oid schemaoid);

/*
 * Get the real database id of a relation. For shared relations, it will be
 * template1.
 */
Oid K2PgGetDatabaseOid(Relation rel);

//------------------------------------------------------------------------------
// K2PG Debug utils.

/**
 * K2 Sql variable that can be used to enable/disable k2pg debug mode.
 * e.g. 'SET k2pg_debug_mode=true'.
 */
extern bool k2pg_debug_mode;

/*
 * Get a string representation of a datum (given its type).
 */
extern const char* K2PgDatumToString(Datum datum, Oid typid);

/*
 * Get a string representation of a tuple (row) given its tuple description (schema).
 */
extern const char* K2PgHeapTupleToString(HeapTuple tuple, TupleDesc tupleDesc);

/*
 * Checks if the master thinks initdb has already been done.
 */
bool K2PgIsInitDbAlreadyDone();

int K2PgGetDdlNestingLevel();
void K2PgIncrementDdlNestingLevel();
void K2PgDecrementDdlNestingLevel(bool success);

#define K2PG_INITDB_ALREADY_DONE_EXIT_CODE 125

/**
 * Checks if the given environment variable is set to a "true" value (e.g. "1").
 */
extern bool K2PgIsEnvVarTrue(const char* env_var_name);

/**
 * Checks if the given environment variable is set to a "true" value (e.g. "1"),
 * but with the given default value in case the environment variable is not
 * defined, or is set to an empty string or the string "auto".
 */
extern bool K2PgIsEnvVarTrueWithDefault(
    const char* env_var_name,
    bool default_value);

/**
 * Checks if the K2PG_ENABLED_IN_POSTGRES is set. This is different from
 * IsK2PgEnabled(), because the IsK2PgEnabled() also checks that we are
 * in the "normal processing mode" and we have a K2PG client session.
 */
extern bool K2PgIsEnabledInPostgresEnvVar();

/**
 * Returns true to allow running PostgreSQL server and initdb as any user. This
 * is needed by some Docker/Kubernetes environments.
 */
extern bool K2PgShouldAllowRunningAsAnyUser();

/**
 * Check if the environment variable indicating that this is a child process
 * of initdb is set.
 */
extern bool K2PgIsInitDbModeEnvVarSet();

/**
 * Set the environment variable that will tell initdb's child process
 * that they are running as part of initdb.
 */
extern void K2PgSetInitDbModeEnvVar();

/**
 * Checks if environment variables indicating that K2PG's unsupported features must
 * be restricted are set
 */
extern bool IsUsingK2PGParser();

/**
 * Returns ERROR or WARNING level depends on environment variable
 */
extern int K2PgUnsupportedFeatureSignalLevel();

/**
 * Returns whether non-transactional COPY gflag is enabled.
 */
extern bool K2PgIsNonTxnCopyEnabled();

#endif /* K2PG_AUX_H */
