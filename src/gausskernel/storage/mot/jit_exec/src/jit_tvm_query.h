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
 * -------------------------------------------------------------------------
 *
 * jit_tvm_query.h
 *    TVM-jitted query codegen common header.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/jit_exec/src/jit_tvm_query.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef JIT_TVM_QUERY_H
#define JIT_TVM_QUERY_H

// Be sure to include global.h before postgres.h to avoid conflict between libintl.h (included in global.h)
// and c.h (included in postgres.h).
#include "global.h"

// be careful to include gscodegen.h before anything else to avoid clash with PM definition in datetime.h
#include "codegen/gscodegen.h"
#include "postgres.h"
#include "catalog/pg_operator.h"
#include "utils/fmgroids.h"
#include "nodes/parsenodes.h"
#include "storage/ipc.h"
#include "nodes/pg_list.h"
#include "utils/elog.h"
#include "utils/numeric.h"
#include "utils/numeric_gs.h"
#include "catalog/pg_aggregate.h"

#include "mot_internal.h"
#include "storage/mot/jit_exec.h"
#include "jit_common.h"
#include "jit_tvm.h"
#include "jit_tvm_util.h"
#include "jit_util.h"
#include "jit_plan.h"

#include "mot_engine.h"
#include "utilities.h"
#include "mot_internal.h"
#include "catalog_column_types.h"
#include "mot_error.h"
#include "utilities.h"
#include "mm_session_api.h"

#include <list>
#include <string>
#include <assert.h>

namespace JitExec {
/** @struct Holds instructions that evaluate in runtime to begin and end iterators of a cursor. */
struct JitTvmRuntimeCursor {
    /** @var The iterator pointing to the beginning of the range. */
    tvm::Instruction* begin_itr;

    /** @var The iterator pointing to the end of the range. */
    tvm::Instruction* end_itr;
};

/** @struct Context used for compiling tvm-jitted functions. */
struct JitTvmCodeGenContext {
    /** @var Main table info. */
    TableInfo _table_info;

    /** @var Inner table info (in JOIN queries). */
    TableInfo m_innerTable_info;

    /** @var Sub-query table info (in COMPOUND queries). */
    TableInfo* m_subQueryTableInfo;

    /** @var Sub-query count (in COMPOUND queries). */
    uint64_t m_subQueryCount;

    /** @var The builder used for emitting code. */
    tvm::Builder* _builder;

    /** @var The resulting jitted function. */
    tvm::Function* m_jittedQuery;
};
} // namespace JitExec

#endif /* JIT_TVM_QUERY_H */
