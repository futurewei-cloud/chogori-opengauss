/* -------------------------------------------------------------------------
 *
 * win32_crashdump.c
 *       Automatic crash dump creation for openGauss on Windows
 *
 * The crashdump feature traps unhandled win32 exceptions produced by the
 * backend, and tries to produce a Windows MiniDump crash
 * dump for later debugging and analysis. The machine performing the dump
 * doesn't need any special debugging tools; the user only needs to send
 * the dump to somebody who has the same version of openGauss and has debugging
 * tools.
 *
 * crashdump module originally by Craig Ringer <ringerc@ringerc.id.au>
 *
 * LIMITATIONS
 * ===========
 * This *won't* work in hard OOM situations or stack overflows.
 *
 * For those, it'd be necessary to take a much more complicated approach where
 * the handler switches to a new stack (if it can) and forks a helper process
 * to debug it self.
 *
 * POSSIBLE FUTURE WORK
 * ====================
 * For bonus points, the crash dump format permits embedding of user-supplied
 * data. If there's anything else that should always be supplied with a crash
 * dump (postgresql.conf? Last few lines of a log file?), it could potentially
 * be added, though at the cost of a greater chance of the crash dump failing.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *    src/common/backend/port/win32/crashdump.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <string.h>
#include <dbghelp.h>
#include "securec.h"

/*
 * Much of the following code is based on CodeProject and MSDN examples,
 * particularly
 * http://www.codeproject.com/KB/debug/postmortemdebug_standalone1.aspx
 *
 * Useful MSDN articles:
 *
 * http://msdn.microsoft.com/en-us/library/ff805116(v=VS.85).aspx
 * http://msdn.microsoft.com/en-us/library/ms679294(VS.85).aspx
 *
 * Other useful articles on working with minidumps:
 * http://www.debuginfo.com/articles/effminidumps.html
 */

typedef BOOL(WINAPI* MINIDUMPWRITEDUMP)(HANDLE hProcess, DWORD dwPid, HANDLE hFile, MINIDUMP_TYPE DumpType,
    CONST PMINIDUMP_EXCEPTION_INFORMATION ExceptionParam, CONST PMINIDUMP_USER_STREAM_INFORMATION UserStreamParam,
    CONST PMINIDUMP_CALLBACK_INFORMATION CallbackParam);

/*
 * This function is the exception handler passed to SetUnhandledExceptionFilter.
 * It's invoked only if there's an unhandled exception. The handler will use
 * dbghelp.dll to generate a crash dump, then resume the normal unhandled
 * exception process, which will generally exit with an error message from
 * the runtime.
 *
 * This function is run under the unhandled exception handler, effectively
 * in a crash context, so it should be careful with memory and avoid using
 * any openGauss functions.
 */
static LONG WINAPI crashDumpHandler(struct _EXCEPTION_POINTERS* pExceptionInfo)
{
    /*
     * We only write crash dumps if the "crashdumps" directory within the
     * openGauss data directory exists.
     */
    DWORD attribs = GetFileAttributesA("crashdumps");

    if (attribs != INVALID_FILE_ATTRIBUTES && (attribs & FILE_ATTRIBUTE_DIRECTORY)) {
        /* 'crashdumps' exists and is a directory. Try to write a dump' */
        HMODULE hDll = NULL;
        MINIDUMPWRITEDUMP pDump = NULL;
        MINIDUMP_TYPE dumpType;
        char dumpPath[_MAX_PATH];
        HANDLE selfProcHandle = GetCurrentProcess();
        DWORD selfPid = GetProcessId(selfProcHandle);
        HANDLE dumpFile = NULL;
        DWORD systemTicks;
        struct _MINIDUMP_EXCEPTION_INFORMATION ExInfo;

        ExInfo.ThreadId = GetCurrentThreadId();
        ExInfo.ExceptionPointers = pExceptionInfo;
        ExInfo.ClientPointers = FALSE;

        /* Load the dbghelp.dll library and functions */
        hDll = LoadLibrary("dbghelp.dll");
        if (hDll == NULL) {
            write_stderr("could not load dbghelp.dll, cannot write crash dump\n");
            return EXCEPTION_CONTINUE_SEARCH;
        }

        pDump = (MINIDUMPWRITEDUMP)GetProcAddress(hDll, "MiniDumpWriteDump");

        if (pDump == NULL) {
            write_stderr("could not load required functions in dbghelp.dll, cannot write crash dump\n");
            return EXCEPTION_CONTINUE_SEARCH;
        }

        /*
         * Dump as much as we can, except shared memory, code segments, and
         * memory mapped files. Exactly what we can dump depends on the
         * version of dbghelp.dll, see:
         * http://msdn.microsoft.com/en-us/library/ms680519(v=VS.85).aspx
         */
        dumpType = MiniDumpNormal | MiniDumpWithHandleData | MiniDumpWithDataSegs;

        if (GetProcAddress(hDll, "EnumDirTree") != NULL) {
            /* If this function exists, we have version 5.2 or newer */
            dumpType |= MiniDumpWithIndirectlyReferencedMemory | MiniDumpWithPrivateReadWriteMemory;
        }

        systemTicks = GetTickCount();
        int rc = snprintf_s(dumpPath,
            sizeof(dumpPath),
            sizeof(dumpPath) - 1,
            "crashdumps\\postgres-pid%0i-%0i.mdmp",
            (int)selfPid,
            (int)systemTicks);
        securec_check_ss(rc, "\0", "\0");
        dumpPath[_MAX_PATH - 1] = '\0';

        dumpFile =
            CreateFile(dumpPath, GENERIC_WRITE, FILE_SHARE_WRITE, NULL, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);
        if (dumpFile == INVALID_HANDLE_VALUE) {
            write_stderr(
                "could not open crash dump file \"%s\" for writing: error code %lu\n", dumpPath, GetLastError());
            return EXCEPTION_CONTINUE_SEARCH;
        }

        if ((*pDump)(selfProcHandle, selfPid, dumpFile, dumpType, &ExInfo, NULL, NULL))
            write_stderr("wrote crash dump to file \"%s\"\n", dumpPath);
        else
            write_stderr("could not write crash dump to file \"%s\": error code %lu\n", dumpPath, GetLastError());

        CloseHandle(dumpFile);
    }

    return EXCEPTION_CONTINUE_SEARCH;
}

void pgwin32_install_crashdump_handler(void)
{
    SetUnhandledExceptionFilter(crashDumpHandler);
}
