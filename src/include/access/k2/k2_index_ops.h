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


#ifndef K2_INDEX_OPS_H
#define K2_INDEX_OPS_H

#include "access/tableam.h"

/*
 * external entry points for K2PG indexes
 */
extern Datum k2inbuild(PG_FUNCTION_ARGS);
extern Datum k2inbuildempty(PG_FUNCTION_ARGS);
extern Datum k2ininsert(PG_FUNCTION_ARGS);
extern Datum k2inbeginscan(PG_FUNCTION_ARGS);
extern Datum k2ingettuple(PG_FUNCTION_ARGS);
extern Datum k2inrescan(PG_FUNCTION_ARGS);
extern Datum k2inendscan(PG_FUNCTION_ARGS);
extern Datum k2indelete(PG_FUNCTION_ARGS);
extern Datum k2inbulkdelete(PG_FUNCTION_ARGS);
extern Datum k2invacuumcleanup(PG_FUNCTION_ARGS);
extern Datum k2incanreturn(PG_FUNCTION_ARGS);
extern Datum k2inoptions(PG_FUNCTION_ARGS);
extern Datum k2incostestimate(PG_FUNCTION_ARGS);

extern bool k2invalidate(Oid opclassoid);

#endif							/* K2_INDEX_OPS_H */
