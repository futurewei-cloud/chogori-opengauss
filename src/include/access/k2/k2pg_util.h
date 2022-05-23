// Copyright (c) YugaByte, Inc.
// Portions Copyright (c) 2021 Futurewei Cloud
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied.  See the License for the specific language governing permissions and limitations
// under the License.

// C wrappers around some K2PG utilities. Suitable for inclusion into C codebases such as our modified
// version of PostgreSQL.
#pragma once

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {

struct varlena;

#endif

typedef struct K2PgStatusStruct* K2PgStatus;
extern K2PgStatus K2PgStatusOK();
bool K2PgStatusIsOK(K2PgStatus s);
bool K2PgStatusIsNotFound(K2PgStatus s);
bool K2PgStatusIsDuplicateKey(K2PgStatus s);
uint32_t K2PgStatusPgsqlError(K2PgStatus s);
uint16_t K2PgStatusTransactionError(K2PgStatus s);
void K2PgFreeStatus(K2PgStatus s);

size_t K2PgStatusMessageLen(K2PgStatus s);
const char* K2PgStatusMessageBegin(K2PgStatus s);
const char* K2PgStatusCodeAsCString(K2PgStatus s);
char* DupK2PgStatusMessage(K2PgStatus status, bool message_only);

bool K2PgIsRestartReadError(uint16_t txn_errcode);

void K2PgResolveHostname();

#define CHECKED_K2PGSTATUS __attribute__ ((warn_unused_result)) K2PgStatus

typedef void* (*K2PgPAllocFn)(size_t size);

typedef struct varlena* (*K2PgCStringToTextWithLenFn)(const char* c, int size);

// Global initialization of the K2PG subsystem.
CHECKED_K2PGSTATUS K2PgInit(
    const char* argv0,
    K2PgPAllocFn palloc_fn,
    K2PgCStringToTextWithLenFn cstring_to_text_with_len_fn);

#ifdef __cplusplus
} // extern "C"
#endif
