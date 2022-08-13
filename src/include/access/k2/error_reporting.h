/*
MIT License

Copyright(c) 2022 Futurewei Cloud

    Permission is hereby granted,
    free of charge, to any person obtaining a copy of this software and associated documentation files(the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and / or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :

    The above copyright notice and this permission notice shall be included in all copies
    or
    substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS",
    WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
    DAMAGES OR OTHER
    LIABILITY,
    WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
*/
#pragma once
#include "utils/elog.h"
#include <string>

/*
Important guidelines for errors
https://www.postgresql.org/docs/current/error-style-guide.html
https://www.postgresql.org/docs/current/error-message-reporting.html

use report_pg_error to report errors in the PG environment.
Actual errors are intercepted by PG and query processing is stopped.
The error message is then returned to the user

Error code definitions available here:
/src/common/backend/utils/errcodes.txt
*/
namespace k2pg {
struct RCode {
    const int err;
    const char* msg;
    const char* detail;
};

#define K2_DEF_RC(__RC_NAME, __RC_MSG, __RC_ERRCODE, __RC_DETAIL) static const inline RCode __RC_NAME {.err=__RC_ERRCODE, .msg=__RC_MSG, .detail=__RC_DETAIL};

struct RCStatus {
    /** @var Denotes success. */
    K2_DEF_RC(RC_OK, "OK", ERRCODE_SUCCESSFUL_COMPLETION, "");
    /** @var Denotes failure. */
    K2_DEF_RC(RC_ERROR, "Error", ERRCODE_FDW_ERROR, "Unknown error has occurred");
    /** @var Denotes operation aborted due to serialization error */
    K2_DEF_RC(RC_SERIALIZATION_FAILURE, "Serialization failure", ERRCODE_T_R_SERIALIZATION_FAILURE, "Could not serialize access due to concurrent access");
    K2_DEF_RC(RC_UNSUPPORTED_COL_TYPE, "Unsupported column type", ERRCODE_INVALID_COLUMN_DEFINITION, "Column definition is not supported");
    K2_DEF_RC(RC_UNSUPPORTED_COL_TYPE_ARR, "Unsupported column type array", ERRCODE_INVALID_COLUMN_DEFINITION, "Column definition Array is not supported");
    K2_DEF_RC(RC_COL_SIZE_INVALID, "Invalid column size", ERRCODE_INVALID_COLUMN_DEFINITION, "Column size is invalid");
    /** @var Unique constraint violation */
    K2_DEF_RC(RC_UNIQUE_VIOLATION, "Unique constraint violation", ERRCODE_UNIQUE_VIOLATION, "Duplicate key value violates unique constraint");
    K2_DEF_RC(RC_TABLE_NOT_FOUND, "Table not found", ERRCODE_UNDEFINED_TABLE, "Table doesn't exist");
    K2_DEF_RC(RC_INDEX_NOT_FOUND, "Index not found", ERRCODE_UNDEFINED_TABLE, "Index doesn't exist");
    K2_DEF_RC(RC_INSERT_ON_EXIST, "Insert on exist", ERRCODE_FDW_ERROR, "Attempt to insert but record already exists");
    K2_DEF_RC(RC_INDEX_DELETE, "Insert to index failed: row deleted", ERRCODE_FDW_ERROR, "Insert to index failed due to row deleted");
    K2_DEF_RC(RC_MEMORY_ALLOCATION_ERROR, "Out of memory", ERRCODE_OUT_OF_LOGICAL_MEMORY, "Memory is temporarily unavailable");
    K2_DEF_RC(RC_NULL_VIOLATION, "Violated NULL constraint", ERRCODE_FDW_ERROR, "NULL value cannot be inserted into non-null column");
    K2_DEF_RC(RC_PANIC, "Internal panic", ERRCODE_FDW_ERROR, "Critical error");
    /** @var operation currently n.a. */
    K2_DEF_RC(RC_NA, "Operation not supported", ERRCODE_FDW_OPERATION_NOT_SUPPORTED, "Operation not supported");
    /** @var The maximum value for return codes. */
    K2_DEF_RC(RC_MAX_VALUE, "MAX_RC (internal error)", ERRCODE_FDW_ERROR, "Unknown error has occurred");
};

/*
 * @brief This method processes the given return code, issuing the necessary warnings or errors as needed
 * or even signaling that transaction has to be aborted
 * @param rc The return code to process
 * @param detail This gets appended to the message detail
*/
void reportRC(const RCode& rc, const std::string& detail = "");

} // ns
