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

// When we mix certain C++ standard lib code and pg code there seems to be a macro conflict that
// will cause compiler errors in libintl.h. Including as the first thing fixes this.
#include <libintl.h>
#include "postgres.h"
#include "access/k2/pg_gate_api.h"
#include "access/k2/k2pg_aux.h"
#include "catalog/pg_type.h"
#include "fmgr/fmgr_comp.h"

#include <skvhttp/dto/SKVRecord.h>
#include <skvhttp/common/Status.h>

namespace k2pg {
namespace gate {
    constexpr int K2_FIELD_OFFSET = 2;
    void serializePGConstToK2SKV(skv::http::dto::SKVRecordBuilder& builder, K2PgConstant constant);
    K2PgStatus K2StatusToK2PgStatus(skv::http::Status&& status);
    K2PgStatus serializePgAttributesToSKV(skv::http::dto::SKVRecordBuilder& builder, std::shared_ptr<skv::http::dto::Schema> schema, int32_t table_id, int32_t index_id,
                                          const std::vector<K2PgAttributeDef>& attrs, const std::unordered_map<int, uint32_t>& attr_num_to_index);
} // k2pg ns
} // gate ns
