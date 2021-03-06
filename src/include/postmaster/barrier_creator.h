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
 * barrier_creator.h
 *      Synchronize the global min csn between all nodes, head file
 *
 *
 * IDENTIFICATION
 *        /Code/src/include/pgxc/barrier_creator.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef BARRIER_CREATOR_H
#define BARRIER_CREATOR_H

#define BARRIER_FILE "hadr_barrier_id"
#define BARRIER_ARCH_INFO_LEN 100

typedef struct ArchiveBarrierLsnInfo {
    XLogRecPtr barrierLsn;
    Oid nodeoid;
} ArchiveBarrierLsnInfo;

extern void barrier_creator_main(void);
extern void barrier_creator_thread_shutdown(void);
extern uint64_t GetObsBarrierIndex(const List *archiveSlotNames);

extern void BarrierArchMain(knl_thread_arg* arg);
extern bool IsFirstCn(void);

#endif /* BARRIER_CREATOR_H */
