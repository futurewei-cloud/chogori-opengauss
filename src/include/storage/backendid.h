/* -------------------------------------------------------------------------
 *
 * backendid.h
 *	  openGauss backend id communication definitions
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/backendid.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef BACKENDID_H
#define BACKENDID_H

/* ----------------
 *		-cim 8/17/90
 * ----------------
 */
typedef int BackendId; /* unique currently active backend identifier */

#define InvalidBackendId (-1)

#endif /* BACKENDID_H */
