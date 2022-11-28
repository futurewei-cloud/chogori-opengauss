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
#include <skvhttp/common/Status.h>
#include "access/k2/status.h"

#include "postgres.h"
#include "c.h"
#include "fmgr/fmgr_comp.h"

namespace k2pg {
// A class for untoasted datums so that freeing data can be done by the destructor and exception-safe
class UntoastedDatum {
public:
    bytea* untoasted;
    Datum datum;
    UntoastedDatum(Datum d) : datum(d) {
        untoasted = DatumGetByteaP(datum);
    }

    ~UntoastedDatum() {
        if ((char*)datum != (char*)untoasted) {
            pfree(untoasted);
        }
    }
};

Status K2StatusToK2PgStatus(skv::http::Status&& status);

}  // namespace k2pg
