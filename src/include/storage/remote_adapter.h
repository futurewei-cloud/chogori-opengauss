/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
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
 * remote_adapter.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/storage/remote_adapter.h
 *
 * NOTE
 *   Don't include any of RPC or PG header file
 *   Just using simple C API interface
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef REMOTE_ADAPTER_H
#define REMOTE_ADAPTER_H

#include "c.h"

#include "storage/remote_read.h"

extern int StandbyReadCUforPrimary(uint32 spcnode, uint32 dbnode, uint32 relnode, int32 colid, uint64 offset,
    int32 size, uint64 lsn, bytea** cudata);

extern int StandbyReadPageforPrimary(uint32 spcnode, uint32 dbnode, uint32 relnode, int16 bucketnode, int32 forknum, uint32 blocknum,
    uint32 blocksize, uint64 lsn, bytea** pagedata);

#endif /* REMOTE_ADAPTER_H */
