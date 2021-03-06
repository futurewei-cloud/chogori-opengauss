/* -------------------------------------------------------------------------
 *
 * uuid.h
 *	  Header file for the "uuid" ADT. In C, we use the name pg_uuid_t,
 *	  to avoid conflicts with any uuid_t type that might be defined by
 *	  the system headers.
 *
 * Copyright (c) 2007-2012, PostgreSQL Global Development Group
 *
 * src/include/utils/uuid.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef UUID_H
#define UUID_H

/* guid size in bytes */
#define UUID_LEN 16

struct pg_uuid_t {
    unsigned char data[UUID_LEN];
};

/* opaque struct; defined in uuid.c */
typedef struct pg_uuid_t pg_uuid_t;

/* fmgr interface macros */
#define UUIDPGetDatum(X) PointerGetDatum(X)
#define PG_RETURN_UUID_P(X) return UUIDPGetDatum(X)
#define DatumGetUUIDP(X) ((pg_uuid_t*)DatumGetPointer(X))
#define PG_GETARG_UUID_P(X) DatumGetUUIDP(PG_GETARG_DATUM(X))

Datum uuid_hash(PG_FUNCTION_ARGS);

#define HASH32_LEN 16
struct hash32_t {
    unsigned char data[HASH32_LEN];
};

#define HASH32GetDatum(X) PointerGetDatum(X)
#define DatumGetHASH32(X) ((hash32_t *)DatumGetPointer(X))
#define PG_RETURN_HASH32_P(X) return HASH32GetDatum(X)
#define PG_GETARG_HASH32_P(X) DatumGetHASH32(PG_GETARG_DATUM(X))

#endif /* UUID_H */
