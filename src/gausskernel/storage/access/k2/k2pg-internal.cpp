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

#include <cstring>
#include <sstream>
#include <assert.h>
#include "k2pg-internal.h"

namespace k2pg {

namespace {
K2PgPAllocFn g_palloc_fn = nullptr;
K2PgCStringToTextWithLenFn g_cstring_to_text_with_len_fn = nullptr;
}  // anonymous namespace

void K2PgSetPAllocFn(K2PgPAllocFn palloc_fn) {
  assert(palloc_fn != NULL);
  g_palloc_fn = palloc_fn;
}

void* K2PgPAlloc(size_t size) {
  assert(g_palloc_fn != NULL);
  return g_palloc_fn(size);
}

void K2PgSetCStringToTextWithLenFn(K2PgCStringToTextWithLenFn fn) {
  assert(fn != NULL);
  g_cstring_to_text_with_len_fn = fn;
}

void* K2PgCStringToTextWithLen(const char* c, int size) {
  assert(g_cstring_to_text_with_len_fn != NULL);
  return g_cstring_to_text_with_len_fn(c, size);
}

const char* K2PgPAllocStdString(const std::string& s) {
  const size_t len = s.size();
  char* result = reinterpret_cast<char*>(K2PgPAlloc(len + 1));
  memcpy(result, s.c_str(), len);
  result[len] = 0;
  return result;
}

} // namespace k2pg
