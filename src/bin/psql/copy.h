/*
 * psql - the openGauss interactive terminal
 *
 * Copyright (c) 2000-2012, PostgreSQL Global Development Group
 *
 * src/bin/psql/copy.h
 */
#ifndef COPY_H
#define COPY_H

#include "libpq/libpq-fe.h"

typedef struct CopyInArgs {
    const char* query;
    PGconn* conn;         /* connection to backend */
    pthread_t	thread;
    pthread_mutex_t *stream_mutex;
    bool	result;
} CopyInArgs;

/* handler for \copy */
bool do_copy(const char* args);

/* lower level processors for copy in/out streams */

bool handleCopyOut(PGconn* conn, FILE* copystream);
bool handleCopyIn(PGconn* conn, FILE* copystream, bool isbinary);
bool ParallelCopyIn(const CopyInArgs* copyarg, const char** errMsg);

#endif

