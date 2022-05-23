#include "pg_gate_thread_local.h"

#include <setjmp.h>
#include <unordered_map>

// #include "status.h"

namespace k2pg {
namespace gate {

/*
 * This code does not need to know anything about the value internals.
 * TODO we could use opaque types instead of void* for additional type safety.
 */
thread_local void *thread_local_memory_context_ = NULL;
thread_local void *pg_strtok_ptr = NULL;
thread_local void *jump_buffer = NULL;
thread_local const void *err_msg = NULL;

//-----------------------------------------------------------------------------
// Memory context.
//-----------------------------------------------------------------------------

void* PgSetThreadLocalCurrentMemoryContext(void *memctx) {
  void *old = thread_local_memory_context_;
  thread_local_memory_context_ = memctx;
  return old;
}

void* PgGetThreadLocalCurrentMemoryContext() {
  return thread_local_memory_context_;
}

void PgResetCurrentMemCtxThreadLocalVars() {
  pg_strtok_ptr = NULL;
  jump_buffer = NULL;
  err_msg = NULL;
}

//-----------------------------------------------------------------------------
// Error reporting.
//-----------------------------------------------------------------------------

void* PgSetThreadLocalJumpBuffer(void* new_buffer) {
    void *old_buffer = jump_buffer;
    jump_buffer = new_buffer;
    return old_buffer;
}

void* PgGetThreadLocalJumpBuffer() {
    return jump_buffer;
}

void PgSetThreadLocalErrMsg(const void* new_msg) {
    err_msg = new_msg;
}

const void* PgGetThreadLocalErrMsg() {
    return err_msg;
}

//-----------------------------------------------------------------------------
// Expression processing.
//-----------------------------------------------------------------------------

void* PgGetThreadLocalStrTokPtr() {
  return pg_strtok_ptr;
}

void PgSetThreadLocalStrTokPtr(char *new_pg_strtok_ptr) {
  pg_strtok_ptr = new_pg_strtok_ptr;
}

}  // namespace gate
}  // namespace k2pg
