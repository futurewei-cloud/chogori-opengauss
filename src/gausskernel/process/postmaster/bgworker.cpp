/*--------------------------------------------------------------------
 * bgworker.cpp
 *		Pluggable background workers implementation
 *
 * Portions Copyright (c) 2021 Huawei Technologies Co.,Ltd.
 *
 * IDENTIFICATION
 *	  src/gausskernel/process/postmaster/bgworker.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include <unistd.h>
#include "miscadmin.h"
#include "libpq/pqsignal.h"
#include "postmaster/postmaster.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "tcop/tcopprot.h"
#include "utils/ps_status.h"
#include "access/transam.h"
#include "utils/postinit.h"
#include "utils/snapmgr.h"
#include "commands/dbcommands.h"

extern void StreamSaveTxnContext(StreamTxnContext* stc);
extern void StreamRestoreTxnContext(StreamTxnContext* stc);
extern Snapshot CopySnapshotByCurrentMcxt(Snapshot snapshot);
extern void SetGlobalSnapshotData(
    TransactionId xmin, TransactionId xmax, uint64 csn, GTM_Timeline timeline, bool ssNeedSyncWaitAll);

int g_max_worker_processes = 64;
/*
 * Return true if the thread is bgworker.
 */
bool IsBgWorkerProcess(void)
{
    return t_thrd.role == BGWORKER;
}

void SetUpBgWorkerTxnEnvironment()
{
    /* resotre transaction context. */
    BgWorkerContext *bwc = (BgWorkerContext *)t_thrd.bgworker_cxt.bgwcontext;

    StreamRestoreTxnContext(&bwc->transactionCxt);

    /* transaction id. */
    SetNextTransactionId(bwc->transactionCxt.txnId, false);
    StreamTxnContextSetTransactionState(&bwc->transactionCxt);

    /* snapshot. */
    Snapshot snapshot = CopySnapshotByCurrentMcxt(bwc->transactionCxt.snapshot);
    SetGlobalSnapshotData(snapshot->xmin, snapshot->xmax, snapshot->snapshotcsn, snapshot->timeline, false);
    StreamTxnContextSetSnapShot(snapshot);
    StreamTxnContextSetMyPgXactXmin(snapshot->xmin);

    /* command id. */
    SaveReceivedCommandId(bwc->transactionCxt.currentCommandId);

    /* timestamp. */
    SetCurrentGTMDeltaTimestamp();
}

static void BgWorkerSaveError()
{
    BackgroundWorker *bgw = (BackgroundWorker *)t_thrd.bgworker_cxt.bgworker;
    ErrorData *edata = &t_thrd.log_cxt.errordata[t_thrd.log_cxt.errordata_stack_depth];
    errno_t rc = EOK;
    int len;
    char *failmsg = "Worker failed during parallel build index.";
    char *nulldetail = "N/A";

    bgw->bgw_edata.elevel = edata->elevel;
    bgw->bgw_edata.sqlerrcode = edata->sqlerrcode;

    char *message = (edata->message != NULL ? edata->message : failmsg);
    len = Min(strlen(message), BGWORKER_MAX_ERROR_LEN - 1);
    rc = strncpy_s(bgw->bgw_edata.message, BGWORKER_MAX_ERROR_LEN, message, len);
    securec_check_c(rc, "", "");
    bgw->bgw_edata.message[len] = '\0';

    char *detail = (edata->detail != NULL ? edata->detail : nulldetail);
    len = Min(strlen(detail), BGWORKER_MAX_ERROR_LEN - 1);
    rc = strncpy_s(bgw->bgw_edata.detail, BGWORKER_MAX_ERROR_LEN, detail, len);
    securec_check_c(rc, "", "");
    bgw->bgw_edata.detail[len] = '\0';
}

/*
 * Called when the Bgworker thread is ending.
 */
static void BgworkerQuitAndClean(int code, Datum arg)
{
    BackgroundWorker *bgw = (BackgroundWorker *)t_thrd.bgworker_cxt.bgworker;

    if (bgw->bgw_status == BGW_STOPPED) {
        bgw->bgw_status = BGW_TERMINATED;
    } else {
        bgw->bgw_status = BGW_FAILED;
    }
}

static void BackgroundWorkerInit(void)
{
    /* we are a postmaster subprocess now */
    IsUnderPostmaster = true;
    t_thrd.role = BGWORKER;

    /* reset t_thrd.proc_cxt.MyProcPid */
    t_thrd.proc_cxt.MyProcPid = gs_thread_self();

    t_thrd.proc_cxt.MyProgName = "BgWorker";

    /* record Start Time for logging */
    t_thrd.proc_cxt.MyStartTime = time(NULL);

    init_ps_display("Bgworker process", "", "", "");

    SetProcessingMode(InitProcessing);

    on_proc_exit(BgworkerQuitAndClean, 0);

    /*
     * SIGINT is used to signal canceling the current action
     */
    (void)gspqsignal(SIGINT, StatementCancelHandler);
    (void)gspqsignal(SIGTERM, die);
    (void)gspqsignal(SIGALRM, handle_sig_alarm);
    (void)gspqsignal(SIGQUIT, SIG_IGN);
    (void)gspqsignal(SIGPIPE, SIG_IGN);
    (void)gspqsignal(SIGUSR1, SIG_IGN);
    (void)gspqsignal(SIGUSR2, SIG_IGN); /* not used */
    (void)gspqsignal(SIGHUP, SIG_IGN);

    /* Reset some signals that are accepted by postmaster but not here */
    (void)gspqsignal(SIGCHLD, SIG_DFL);
    (void)gspqsignal(SIGTTIN, SIG_DFL);
    (void)gspqsignal(SIGTTOU, SIG_DFL);
    (void)gspqsignal(SIGCONT, SIG_DFL);
    (void)gspqsignal(SIGWINCH, SIG_DFL);

    /* Early initialization */
    BaseInit();

#ifndef EXEC_BACKEND
    InitProcess();
#endif
}

/*
 * Start a new background worker
 *
 * This is the main entry point for background worker, to be called from
 * postmaster.
 */
void BackgroundWorkerMain(void)
{
    BgWorkerContext *bwc = (BgWorkerContext  *)t_thrd.bgworker_cxt.bgwcontext;
    BackgroundWorker *bgw = (BackgroundWorker *)t_thrd.bgworker_cxt.bgworker;
    MemoryContext workerContext = NULL;
    MemoryContext oldcontext = NULL;
    sigjmp_buf local_sigjmp_buf;
    int *oldTryCounter = NULL;
    int curTryCounter;

    if (pg_atomic_fetch_add_u32(&bgw->disable_count, 1) > 0) {
        /* The leader disallowed this worker to do index build due to startup time longer than 5s. */
        ereport(WARNING, (errmsg("BgWorker thread %lu was disabled for long startup time.",
            t_thrd.proc_cxt.MyProcPid)));
        /* Note that we are in the state BGW_NOT_YET_STARTED. */
        goto out;
    }

    BackgroundWorkerInit();

    workerContext = AllocSetContextCreate(t_thrd.top_mem_cxt, "BgWorker", ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);

    (void)MemoryContextSwitchTo(workerContext);

    /* Unblock signals (they were blocked when the postmaster forked us) */
    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();

    /* If an exception is encountered, processing resumes here. */
    if (sigsetjmp(local_sigjmp_buf, 1) != 0) {
        gstrace_tryblock_exit(true, oldTryCounter);

        /* Since not using PG_TRY, must reset error stack by hand */
        t_thrd.log_cxt.error_context_stack = NULL;

        /* Prevent interrupts while cleaning up */
        HOLD_INTERRUPTS();

        /* save bgworker error data for leader reporting */
        BgWorkerSaveError();

        /* Report the error to the parallel leader and the server log */
        EmitErrorReport();
        /*
         * These operations are really just a minimal subset of
         * AbortTransaction().  We don't have very many resources to worry
         * about in bgwriter, but we do have LWLocks, buffers, and temp files.
         */
        LWLockReleaseAll();
        AbortBufferIO();
        UnlockBuffers();
        /* buffer pins are released here */
        if (t_thrd.utils_cxt.CurrentResourceOwner != NULL) {
            ResourceOwnerRelease(t_thrd.utils_cxt.CurrentResourceOwner, RESOURCE_RELEASE_BEFORE_LOCKS, false, true);
        }

        /*
         * Now return to normal top-level context and clear ErrorContext for
         * next time.
         */
        (void)MemoryContextSwitchTo(workerContext);
        FlushErrorState();

        /* Flush any leaked data in the top-level context */
        MemoryContextResetAndDeleteChildren(workerContext);

        /* and go away */
        proc_exit(1);
    }
    oldTryCounter = gstrace_tryblock_entry(&curTryCounter);

    /* We can now handle ereport(ERROR) */
    t_thrd.log_cxt.PG_exception_stack = &local_sigjmp_buf;
    u_sess->proc_cxt.MyProcPort->SessionStartTime = GetCurrentTimestamp();
    bgw->bgw_status = BGW_STARTED;

    /* General initialization. */
    /* user_name and database_name in u_sess->proc_cxt.MyProcPort is under t_thrd.top_mem_cxt */
    oldcontext = MemoryContextSwitchTo(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));
    if (u_sess->proc_cxt.MyProcPort->database_name != NULL) {
        pfree_ext(u_sess->proc_cxt.MyProcPort->database_name);
    }
    if (u_sess->proc_cxt.MyProcPort->user_name != NULL) {
        pfree_ext(u_sess->proc_cxt.MyProcPort->user_name);
    }
    u_sess->proc_cxt.MyProcPort->database_name = pstrdup(bwc->databaseName);
    u_sess->proc_cxt.MyProcPort->user_name = pstrdup(bwc->userName);
    (void)MemoryContextSwitchTo(oldcontext);

    t_thrd.proc_cxt.PostInit->SetDatabaseAndUser(bwc->databaseName, InvalidOid, bwc->userName);
    t_thrd.proc_cxt.PostInit->InitBgWorker();
    t_thrd.proc_cxt.PostInit->GetDatabaseName(u_sess->proc_cxt.MyProcPort->database_name);
    ereport(LOG, (errmsg("bgworker threadId is %lu.", t_thrd.proc_cxt.MyProcPid)));

    StartTransactionCommand();
    SetUpBgWorkerTxnEnvironment();

    /*
     * Join locking group.  We must do this before anything that could try to
     * acquire a heavyweight lock, because any heavyweight locks acquired to
     * this point could block either directly against the parallel group
     * leader or against some process which in turn waits for a lock that
     * conflicts with the parallel group leader, causing an undetected
     * deadlock.  (If we can't join the lock group, the leader has gone away,
     * so just exit quietly.)
     */
    BecomeLockGroupMember(bwc->leader);

    u_sess->attr.attr_sql.enable_cluster_resize = bwc->enable_cluster_resize;

    /*
     * Now invoke the user-defined worker code
     */
    bwc->main_entry(bwc);
    EndParallelWorkerTransaction();
    ResetTransactionInfo();
    /* ... and if it returns, we're done */
    bgw->bgw_status = BGW_STOPPED;

out:
    proc_exit(0);
}

/*
 * Register a new background worker while processing shared_preload_libraries.
 *
 * This can only be called in the _PG_init function of a module library
 * that's loaded by shared_preload_libraries; otherwise it has no effect.
 */
void RegisterBackgroundWorker(BgWorkerContext *bwc)
{
    BackgroundWorker *bgw;
    BackgroundWorkerArgs *bwa;

    bgw = (BackgroundWorker*)MemoryContextAllocZero(
        INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), sizeof(BackgroundWorker));
    bgw->bgw_status = BGW_NOT_YET_STARTED;
    bgw->bgw_status_dur = 0;
    bgw->disable_count = 0;

    /* Construct bgworker thread args */
    bwa = (BackgroundWorkerArgs*)palloc(sizeof(BackgroundWorkerArgs));
    bwa->bgwcontext = bwc;
    bwa->bgworker = bgw;

    /* Fork a new worker thread */
    bgw->bgw_notify_pid = initialize_util_thread(BGWORKER, bwa);

    /* Copy the registration data into the registered workers list. */
    slist_push_head(&t_thrd.bgworker_cxt.bgwlist, &bgw->rw_lnode);
}

static void BgworkerCleanupSharedContext()
{
    Assert(!IsBgWorkerProcess());
    /* clean up backgroud shared context */
    if (t_thrd.bgworker_cxt.bgwcontext) {
        BgWorkerContext *bwc = (BgWorkerContext*)t_thrd.bgworker_cxt.bgwcontext;

        if (bwc->exit_entry) {
            bwc->exit_entry(bwc);
        }
        pfree_ext(bwc->bgshared);
        pfree_ext(t_thrd.bgworker_cxt.bgwcontext);
    }
    slist_init(&t_thrd.bgworker_cxt.bgwlist);
}

void BgworkerListSyncQuit()
{
    slist_mutable_iter iter;
    bool alldone = false;
    bool sigsent = false;

    if (slist_is_empty(&t_thrd.bgworker_cxt.bgwlist)) {
        return;
    }

loop:
    alldone = true;
    slist_foreach_modify(iter, &t_thrd.bgworker_cxt.bgwlist) {
        BackgroundWorker *bgw = slist_container(BackgroundWorker, rw_lnode, iter.cur);
        if (bgw->bgw_status != BGW_FAILED && bgw->bgw_status != BGW_TERMINATED) {
            if (!sigsent && gs_signal_send(bgw->bgw_notify_pid, SIGINT) != 0) {
                ereport(WARNING, (errmsg("BgworkerListSyncQuit kill(pid %lu, stat %d) failed: %m",
                    bgw->bgw_notify_pid, bgw->bgw_status)));
            }

            if (bgw->bgw_status == BGW_NOT_YET_STARTED && (pg_atomic_fetch_add_u32(&bgw->disable_count, 1) == 0)) {
                ereport(WARNING, (errmsg("The bgworker thread %lu hasn't started at the moment "
                                         "when the leader quits, disable it.", bgw->bgw_notify_pid)));
                bgw->bgw_status = BGW_FAILED;
            }
            alldone = false;
        } else {
            slist_delete_current(&iter);
            pfree(bgw);
        }
    }

    if (!alldone) {
        usleep(BGWORKER_LOOP_SLEEP_TIME);
        sigsent = true;
        goto loop;
    }
    BgworkerCleanupSharedContext();
}

static inline void CleanupUnstartBgworkers(int nunstarts)
{
    slist_mutable_iter iter;

    if (nunstarts > 0) {
        slist_foreach_modify(iter, &t_thrd.bgworker_cxt.bgwlist) {
            BackgroundWorker *bgw = slist_container(BackgroundWorker, rw_lnode, iter.cur);
            if (bgw->bgw_status == BGW_NOT_YET_STARTED) {
                /* the bgworker thread is unable to start, remove it from the waiting list */
                slist_delete_current(&iter);
                pfree(bgw);
            }
        }
    }
}

void BgworkerListWaitFinish(int *nparticipants)
{
    slist_iter iter;
    bool alldone = false;
    uint32 disable_count;
    int nfinished;
    int nunstarts = 0; 

    Assert(nparticipants != NULL);

    while (!alldone) {
        nfinished = 0;
        slist_foreach(iter, &t_thrd.bgworker_cxt.bgwlist) {
            BackgroundWorker *bgw = slist_container(BackgroundWorker, rw_lnode, iter.cur);
            if (bgw->bgw_status == BGW_NOT_YET_STARTED && ++bgw->bgw_status_dur > BGWORKER_STATUS_DURLIMIT) {
                disable_count = pg_atomic_fetch_add_u32(&bgw->disable_count, 1);
                if (disable_count == 0) {
                    ereport(WARNING, (errmsg("The bgworker thread %lu hasn't started in 5 seconds, disable it.",
                        bgw->bgw_notify_pid)));
                    (*nparticipants)--;
                    nunstarts++;
                }
            } else if (bgw->bgw_status == BGW_FAILED) {
                if (bgw->bgw_edata.elevel >= ERROR) {
                    ereport(bgw->bgw_edata.elevel, (errcode(bgw->bgw_edata.sqlerrcode), errmsg("%s",
                        bgw->bgw_edata.message), errdetail("%s", bgw->bgw_edata.detail)));
                } else {
                    ereport(ERROR, (errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION),
                        errmsg("Background worker failed during parallel index building.")));
                }
            } else if (bgw->bgw_status == BGW_TERMINATED) {
                nfinished++;
            }
        }

        alldone = (nfinished >= *nparticipants);

        if (alldone) {
            CleanupUnstartBgworkers(nunstarts);
        } else {
            CHECK_FOR_INTERRUPTS();
            usleep(BGWORKER_LOOP_SLEEP_TIME);
        }
    }
}

void LaunchBackgroundWorkers(int nworkers, void *bgshared, bgworker_main bgmain, bgworker_exit bgexit)
{
    MemoryContext oldcontext;
    BgWorkerContext *bwc;

    Assert(nworkers > 0);
    /* We need to be a lock group leader. */
    BecomeLockGroupLeader();

    /* We might be running in a short-lived memory context. */
    oldcontext = MemoryContextSwitchTo(u_sess->top_transaction_mem_cxt);

    /*
     * Start workers.
     *
     * The caller must be able to tolerate ending up with fewer workers than
     * expected, so there is no need to throw an error here if registration
     * fails.  It wouldn't help much anyway, because registering the worker in
     * no way guarantees that it will start up and initialize successfully.
     */
    bwc = (BgWorkerContext*)MemoryContextAllocZero(
        INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), sizeof(BgWorkerContext));
    bwc->transactionCxt.txnId = GetCurrentTransactionIdIfAny();
    bwc->transactionCxt.snapshot = GetActiveSnapshot();
    bwc->bgshared = bgshared;
    bwc->databaseName = get_database_name(u_sess->proc_cxt.MyDatabaseId);
    bwc->userName = u_sess->proc_cxt.MyProcPort->user_name;
    /* pass enable_cluster_resize to bgwokers to optimize parallel index building performance during redistribution */
    bwc->enable_cluster_resize = u_sess->attr.attr_sql.enable_cluster_resize;
    bwc->leader = t_thrd.proc;
    bwc->main_entry = bgmain;
    bwc->exit_entry = bgexit;

    t_thrd.bgworker_cxt.bgwcontext = bwc;

    StreamSaveTxnContext(&bwc->transactionCxt);

    for (int i = 0; i < nworkers; ++i) {
        RegisterBackgroundWorker(bwc);
    }

    /* Restore previous memory context. */
    MemoryContextSwitchTo(oldcontext);
}
