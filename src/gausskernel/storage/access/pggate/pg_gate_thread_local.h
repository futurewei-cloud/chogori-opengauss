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
