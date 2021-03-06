/* -------------------------------------------------------------------------
 *
 * barrier.h
 *
 *	  Definitions for the PITR barrier handling
 *
 *
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/pgxc/barrier.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef XC_BARRIER_H
#define XC_BARRIER_H

#include "access/xlogreader.h"
#include "lib/stringinfo.h"

#define CREATE_BARRIER_PREPARE 'P'
#define CREATE_BARRIER_EXECUTE 'X'
#define CREATE_SWITCHOVER_BARRIER_EXECUTE 'S'
#define CREATE_BARRIER_END 'E'
#define CREATE_BARRIER_QUERY_ARCHIVE 'W'
#define BARRIER_QUERY_ARCHIVE 'Q'


#define CREATE_BARRIER_PREPARE_DONE 'p'
#define CREATE_BARRIER_EXECUTE_DONE 'x'

#define BARRIER_LSN_FILE "barrier_lsn"
#define HADR_BARRIER_ID_FILE "hadr_barrier_id"
#define HADR_FAILOVER_BARRIER_ID_FILE "hadr_stop_barrier_id"
#define HADR_SWITCHOVER_BARRIER_ID_FILE "hadr_switchover_barrier_id"
#define HADR_MASTER_CLUSTER_STAT_FILE "master_hadr_stat"
#define HADR_STANDBY_CLUSTER_STAT_FILE "slave_hadr_stat"
#define HADR_SWITCHOVER_TO_MASTER "hadr_switchover_to_master"
#define HADR_SWITCHOVER_TO_STANDBY "hadr_switchover_to_standby"
#define HADR_IN_NORMAL "hadr_normal"
#define HADR_IN_FAILOVER "hadr_promote"
#define HADR_BARRIER_ID_HEAD "hadr"
#define HADR_KEY_CN_FILE "hadr_key_cn"
#define HADR_DELETE_CN_FILE "hadr_delete_cn"
#define BARRIER_LSN_FILE_LENGTH 17
#define MAX_BARRIER_ID_LENGTH 40
#define MAX_DEFAULT_LENGTH 255
#define WAIT_ARCHIVE_TIMEOUT 6000
#define MAX_BARRIER_SQL_LENGTH 60
#define BARRIER_LSN_LENGTH 30

#define XLOG_BARRIER_CREATE 0x00

extern void ProcessCreateBarrierPrepare(const char* id);
extern void ProcessCreateBarrierEnd(const char* id);
extern void ProcessCreateBarrierExecute(const char* id, bool isSwitchoverBarrier = false);

extern void RequestBarrier(const char* id, char* completionTag, bool isSwitchoverBarrier = false);
extern void barrier_redo(XLogReaderState* record);
extern void barrier_desc(StringInfo buf, XLogReaderState* record);
extern void DisasterRecoveryRequestBarrier(const char* id, bool isSwitchoverBarrier = false);
extern void ProcessBarrierQueryArchive(char* id);

#endif
