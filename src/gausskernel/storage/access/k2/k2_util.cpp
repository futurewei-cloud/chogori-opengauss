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
#include "k2_util.h"

namespace k2pg {

int K2CodeToPGCode(int k2code) {
    switch (k2code) {
        case 200: // OK Codes
        case 201:
        case 202:
            return ERRCODE_SUCCESSFUL_COMPLETION;
        case 400: // Bad request
            return ERRCODE_INTERNAL_ERROR;
        case 403: // Forbidden, used to indicate AbortRequestTooOld in K23SI
            return ERRCODE_SNAPSHOT_INVALID;
        case 404: // Not found
            return ERRCODE_SUCCESSFUL_COMPLETION;
        case 405: // Not allowed, indicates a bug in K2 usage or operation
        case 406: // Not acceptable, used to indicate BadFilterExpression
        case 408: // Timeout
            return ERRCODE_INTERNAL_ERROR;
        case 409: // Conflict, used to indicate K23SI transaction conflicts
            return ERRCODE_T_R_SERIALIZATION_FAILURE;
        case 410: // Gone, indicates a partition map error
            return ERRCODE_INTERNAL_ERROR;
        case 412: // Precondition failed, indicates a failed K2 insert operation
            return ERRCODE_UNIQUE_VIOLATION;
        case 422: // Unprocessable entity, BadParameter in K23SI, indicates a bug in usage or operation
        case 500: // Internal error, indicates a bug in K2 code
        case 503: // Service unavailable, indicates a partition is not assigned
        default:
            return ERRCODE_INTERNAL_ERROR;
    }

    return ERRCODE_INTERNAL_ERROR;
}

Status K2StatusToK2PgStatus(skv::http::Status&& status) {
    if(!status.is2xxOK()) {
        K2PgStatus out_status{
            .pg_code = K2CodeToPGCode(status.code),
            .k2_code = status.code,
            .msg = std::move(status.message),
            .detail = ""
        };

        return out_status;
    }

    return Status::OK;
}

}  // namespace k2pg
