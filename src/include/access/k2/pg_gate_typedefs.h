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
#pragma once

#include <stddef.h>
#include "access/k2/data_type.h"
#include "access/k2/pg_param.h"
#include "access/k2/status.h"
#include "access/k2/pg_ids.h"
#include "access/k2/pg_session.h"
#include "access/k2/pg_expr.h"
#include "access/k2/pg_tabledesc.h"
#include "access/k2/pg_statement.h"
#include "access/k2/pg_memctx.h"

// K2 SQL data type in PG C code
typedef enum   K2SqlDataType K2PgDataType;
typedef class  k2pg::PgSession  * K2PgSession;
typedef class  k2pg::PgStatement * K2PgStatement;
typedef class  k2pg::PgExpr * K2PgExpr;
typedef class  k2pg::PgTableDesc * K2PgTableDesc;
typedef class  k2pg::PgMemctx * K2PgMemctx;
typedef struct k2pg::Status K2PgStatus;

// Postgres object identifier (OID) defined in Postgres' postgres_ext.h
typedef unsigned int K2PgOid;
#define kInvalidOid ((K2PgOid) 0)

typedef struct k2pg::PgSysColumns K2PgSysColumns;
typedef struct k2pg::PgTypeAttrs K2PgTypeAttrs;
typedef struct k2pg::PgTypeEntity K2PgTypeEntity;
typedef struct k2pg::PgPrepareParameters K2PgPrepareParameters;
typedef struct k2pg::PgExecParameters K2PgExecParameters;
typedef struct k2pg::PgTableProperties K2PgTableProperties;

// API to read type information.
const K2PgTypeEntity *K2PgFindTypeEntity(int type_oid);
K2PgDataType K2PgGetType(const K2PgTypeEntity *type_entity);
bool K2PgAllowForPrimaryKey(const K2PgTypeEntity *type_entity);

typedef struct PgCallbacks {
  void (*FetchUniqueConstraintName)(K2PgOid, char*, size_t);
  K2PgMemctx (*GetCurrentK2Memctx)();
} K2PgCallbacks;
