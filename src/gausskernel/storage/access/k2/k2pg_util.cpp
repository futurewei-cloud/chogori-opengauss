// Copyright (c) YugaByte, Inc.
// Portions Copyright (c) 2022 Futurewei Cloud
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

#include "access/k2/k2pg_util.h"

#include <stdarg.h>
#include <fstream>
#include <string>
#include <type_traits>
#include <utility>

#if defined(__APPLE__)
#include <mach-o/dyld.h>
#include <sys/sysctl.h>
#else
#include <linux/falloc.h>
#include <sys/sysinfo.h>
#endif  // defined(__APPLE__)

#include "k2pg-internal.h"
#include "access/k2/k2pg_errcodes.h"

using k2pg::K2PgErrorCode;

//extern "C" {

K2PgStatus K2PgInit(const char* argv0,
                  K2PgPAllocFn palloc_fn,
                  K2PgCStringToTextWithLenFn cstring_to_text_with_len_fn) {
  k2pg::K2PgSetPAllocFn(palloc_fn);
  if (cstring_to_text_with_len_fn) {
    k2pg::K2PgSetCStringToTextWithLenFn(cstring_to_text_with_len_fn);
  }
  // TODO: add more logic here

   K2PgStatus status {
      .pg_code = ERRCODE_SUCCESSFUL_COMPLETION,
      .k2_code = 200,
      .msg = "OK",
      .detail = "K2PgInit OK"
  };

  return status;
}

//} // extern "C"
