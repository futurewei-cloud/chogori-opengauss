/*
 * contrib/btree_gist/btree_inet.c
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "btree_gist.h"
#include "btree_utils_num.h"
#include "utils/builtins.h"
#include "utils/inet.h"
#include "catalog/pg_type.h"

typedef struct inetkey {
    double lower;
    double upper;
} inetKEY;

/*
** inet ops
*/
PG_FUNCTION_INFO_V1(gbt_inet_compress);
PG_FUNCTION_INFO_V1(gbt_inet_union);
PG_FUNCTION_INFO_V1(gbt_inet_picksplit);
PG_FUNCTION_INFO_V1(gbt_inet_consistent);
PG_FUNCTION_INFO_V1(gbt_inet_penalty);
PG_FUNCTION_INFO_V1(gbt_inet_same);

extern "C" Datum gbt_inet_compress(PG_FUNCTION_ARGS);
extern "C" Datum gbt_inet_union(PG_FUNCTION_ARGS);
extern "C" Datum gbt_inet_picksplit(PG_FUNCTION_ARGS);
extern "C" Datum gbt_inet_consistent(PG_FUNCTION_ARGS);
extern "C" Datum gbt_inet_penalty(PG_FUNCTION_ARGS);
extern "C" Datum gbt_inet_same(PG_FUNCTION_ARGS);

static bool gbt_inetgt(const void* a, const void* b)
{
    return (*((const double*)a) > *((const double*)b));
}
static bool gbt_inetge(const void* a, const void* b)
{
    return (*((const double*)a) >= *((const double*)b));
}
static bool gbt_ineteq(const void* a, const void* b)
{
    return (*((const double*)a) == *((const double*)b));
}
static bool gbt_inetle(const void* a, const void* b)
{
    return (*((const double*)a) <= *((const double*)b));
}
static bool gbt_inetlt(const void* a, const void* b)
{
    return (*((const double*)a) < *((const double*)b));
}

static int gbt_inetkey_cmp(const void* a, const void* b)
{
    inetKEY* ia = (inetKEY*)(((const Nsrt*)a)->t);
    inetKEY* ib = (inetKEY*)(((const Nsrt*)b)->t);

    if (ia->lower == ib->lower) {
        if (ia->upper == ib->upper)
            return 0;

        return (ia->upper > ib->upper) ? 1 : -1;
    }

    return (ia->lower > ib->lower) ? 1 : -1;
}

static const gbtree_ninfo tinfo = {
    gbt_t_inet, sizeof(double), gbt_inetgt, gbt_inetge, gbt_ineteq, gbt_inetle, gbt_inetlt, gbt_inetkey_cmp, NULL};

/**************************************************
 * inet ops
 **************************************************/

Datum gbt_inet_compress(PG_FUNCTION_ARGS)
{
    GISTENTRY* entry = (GISTENTRY*)PG_GETARG_POINTER(0);
    GISTENTRY* retval = NULL;

    if (entry->leafkey) {
        inetKEY* r = (inetKEY*)palloc(sizeof(inetKEY));

        retval = (GISTENTRY*)palloc(sizeof(GISTENTRY));
        r->lower = convert_network_to_scalar(entry->key, INETOID);
        r->upper = r->lower;
        gistentryinit(*retval, PointerGetDatum(r), entry->rel, entry->page, entry->offset, FALSE);
    } else
        retval = entry;

    PG_RETURN_POINTER(retval);
}

Datum gbt_inet_consistent(PG_FUNCTION_ARGS)
{
    GISTENTRY* entry = (GISTENTRY*)PG_GETARG_POINTER(0);
    double query = convert_network_to_scalar(PG_GETARG_DATUM(1), INETOID);
    StrategyNumber strategy = (StrategyNumber)PG_GETARG_UINT16(2);

    bool* recheck = (bool*)PG_GETARG_POINTER(4);
    inetKEY* kkk = (inetKEY*)DatumGetPointer(entry->key);
    GBT_NUMKEY_R key;

    /* All cases served by this function are inexact */
    *recheck = true;

    key.lower = (GBT_NUMKEY*)&kkk->lower;
    key.upper = (GBT_NUMKEY*)&kkk->upper;

    PG_RETURN_BOOL(gbt_num_consistent(&key, (void*)&query, &strategy, GIST_LEAF(entry), &tinfo));
}

Datum gbt_inet_union(PG_FUNCTION_ARGS)
{
    GistEntryVector* entryvec = (GistEntryVector*)PG_GETARG_POINTER(0);
    void* out = palloc(sizeof(inetKEY));

    *(int*)PG_GETARG_POINTER(1) = sizeof(inetKEY);
    PG_RETURN_POINTER(gbt_num_union((GBT_NUMKEY*)out, entryvec, &tinfo));
}

Datum gbt_inet_penalty(PG_FUNCTION_ARGS)
{
    inetKEY* origentry = (inetKEY*)DatumGetPointer(((GISTENTRY*)PG_GETARG_POINTER(0))->key);
    inetKEY* newentry = (inetKEY*)DatumGetPointer(((GISTENTRY*)PG_GETARG_POINTER(1))->key);
    float* result = (float*)PG_GETARG_POINTER(2);

    penalty_num(result, origentry->lower, origentry->upper, newentry->lower, newentry->upper);

    PG_RETURN_POINTER(result);
}

Datum gbt_inet_picksplit(PG_FUNCTION_ARGS)
{
    PG_RETURN_POINTER(
        gbt_num_picksplit((GistEntryVector*)PG_GETARG_POINTER(0), (GIST_SPLITVEC*)PG_GETARG_POINTER(1), &tinfo));
}

Datum gbt_inet_same(PG_FUNCTION_ARGS)
{
    inetKEY* b1 = (inetKEY*)PG_GETARG_POINTER(0);
    inetKEY* b2 = (inetKEY*)PG_GETARG_POINTER(1);
    bool* result = (bool*)PG_GETARG_POINTER(2);

    *result = gbt_num_same((GBT_NUMKEY*)b1, (GBT_NUMKEY*)b2, &tinfo);
    PG_RETURN_POINTER(result);
}
