/*
 * Copyright (c) 2021 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * ---------------------------------------------------------------------------------------
 * 
 * gs_package.h
 *     Definition about catalog of package.
 * 
 * 
 * IDENTIFICATION
 *        src/include/catalog/gs_package.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef GS_PACKAGE_H
#define GS_PACKAGE_H

#include "catalog/genbki.h"

/* ----------------------------------------------------------------
 *        gs_package definition.
 *
 *        cpp turns this into typedef struct FormData_gs_package
 *
 *    nspname             name of the namespace
 *    nspowner            owner (creator) of the namespace
 *    nspacl              access privilege list
 * ----------------------------------------------------------------
 */
#define PackageRelationId  7815
#define PackageRelation_Rowtype_Id 9745
extern Oid PackageNameGetOid(const char* pkgname, Oid namespaceId = InvalidOid);
extern PLpgSQL_package* PackageInstantiation(Oid packageOid);
extern void PackageInit(PLpgSQL_package* pkg, bool isCreate=false);
extern void DisconnectSPI();
extern Oid saveCallFromPkgOid(Oid pkgOid);
extern void restoreCallFromPkgOid(Oid pkgOid);
extern void ConnectSPI();
extern NameData* GetPackageName(Oid packageOid);
extern Oid PackageNameListGetOid(List* pkgnameList, bool missing_ok=false);
extern bool IsExistPackageName(const char* pkgname);
CATALOG(gs_package,7815) BKI_BOOTSTRAP BKI_ROWTYPE_OID(9745) BKI_SCHEMA_MACRO
{
    Oid         pkgnamespace;   /*package name space*/
    Oid         pkgowner;       /*package owner*/
    NameData    pkgname;        /*package name*/

#ifdef CATALOG_VARLEN            /* variable-length fields start here */
    text        pkgspecsrc;     /* package specification */
    text        pkgbodydeclsrc; /* package delcare */
    text        pkgbodyinitsrc; /* package body */
    aclitem     pkgacl[1];      /* package privilege */
#endif
} FormData_gs_package;

/* ----------------
 *        FormData_gs_package corresponds to a pointer to a tuple with
 *        the format of gs_package relation.
 * ----------------
 */
typedef FormData_gs_package *Form_gs_package;

/* ----------------
 *        compiler constants for gs_package
 * ----------------
 */

#define Natts_gs_package				7
#define Anum_gs_package_pkgnamespace    1
#define Anum_gs_package_pkgowner        2
#define Anum_gs_package_pkgname         3
#define Anum_gs_package_pkgspecsrc      4
#define Anum_gs_package_pkgbodydeclsrc  5
#define Anum_gs_package_pkgbodyinitsrc  6
#define Anum_gs_package_pkgacl          7


#endif   /* PG_PACKAGRE_H */

