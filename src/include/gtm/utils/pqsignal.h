/* -------------------------------------------------------------------------
 *
 * pqsignal.h
 *	  prototypes for the reliable BSD-style signal(2) routine.
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd. 
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * $PostgreSQL: pgsql/src/include/libpq/pqsignal.h,v 1.32 2008/01/01 19:45:58 momjian Exp $
 *
 * NOTES
 *	  This shouldn't be in libpq, but the monitor and some other
 *	  things need it...
 *
 * -------------------------------------------------------------------------
 */
#ifndef PQSIGNAL_H
#define PQSIGNAL_H

#include <signal.h>

#ifdef HAVE_SIGPROCMASK
extern THR_LOCAL sigset_t UnBlockSig, BlockSig, AuthBlockSig;

#define PG_SETMASK(mask) pthread_sigmask(SIG_SETMASK, mask, NULL)
#else
extern THR_LOCAL int UnBlockSig, BlockSig, AuthBlockSig;

#ifndef WIN32
#define PG_SETMASK(mask) sigsetmask(*((int*)(mask)))
#else
#define PG_SETMASK(mask) pqsigsetmask(*((int*)(mask)))
int pqsigsetmask(int mask);
#endif
#endif

typedef void (*pqsigfunc)(int);

extern void pqinitmask(void);

extern pqsigfunc pqsignal(int signo, pqsigfunc func);

#endif /* PQSIGNAL_H */
