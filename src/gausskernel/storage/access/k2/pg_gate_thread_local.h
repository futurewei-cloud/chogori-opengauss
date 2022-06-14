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

#include "access/k2/pg_gate_typedefs.h"

namespace k2pg {
namespace gate {

//-----------------------------------------------------------------------------
// Memory context.
//-----------------------------------------------------------------------------

/* CurrentMemoryContext (from palloc.h/mcxt.c) */
void* PgGetThreadLocalCurrentMemoryContext();
void* PgSetThreadLocalCurrentMemoryContext(void *memctx);

/*
 * Reset all variables that would be allocated within the CurrentMemoryContext.
 * These should not be used anyway, but just keeping things tidy.
 * To be used when calling MemoryContextReset() for the CurrentMemoryContext.
 */
void PgResetCurrentMemCtxThreadLocalVars();

//-----------------------------------------------------------------------------
// Error reporting.
//-----------------------------------------------------------------------------

/*
 * Jump buffer used for error reporting (with sigjmp/longjmp) in multithread
 * context.
 * TODO Currently not yet the same as the standard PG/PSQL sigjmp_buf exception
 * stack because we use simplified error reporting when multi-threaded.
 */
void* PgSetThreadLocalJumpBuffer(void* new_buffer);
void* PgGetThreadLocalJumpBuffer();

/*
 * Save/get the error message. Needs a separate function because it will be
 * generated separately in errmsg when using ereport (instead of elog).
 */
void PgSetThreadLocalErrMsg(const void* new_msg);
const void* PgGetThreadLocalErrMsg();

//-----------------------------------------------------------------------------
// Expression processing.
//-----------------------------------------------------------------------------

/*
 * pg_strtok_ptr (from read.c)
 * TODO Technically this does not need to be global but refactoring the parsing
 * code to pass it as a parameter is tedious due to the notational overhead.
 */
void* PgGetThreadLocalStrTokPtr();
void PgSetThreadLocalStrTokPtr(char *new_pg_strtok_ptr);

} // namespace gate
} // namespace k2pg
