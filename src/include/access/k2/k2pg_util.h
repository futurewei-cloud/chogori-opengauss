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
