/*
 * psql - the openGauss interactive terminal
 *
 * Copyright (c) 2000-2012, PostgreSQL Global Development Group
 *
 * src/bin/psql/mainloop.h
 */
#ifndef MAINLOOP_H
#define MAINLOOP_H

#include "postgres_fe.h"

int MainLoop(FILE* source, char* querystring = NULL);

#endif /* MAINLOOP_H */

