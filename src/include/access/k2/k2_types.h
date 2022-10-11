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

#include <libintl.h>
#include "postgres.h"
#include "catalog/pg_type.h"

#include <skvhttp/dto/FieldTypes.h>

// This file contains stateless helper methods for converting between PG and SKV types

namespace k2pg {

// These are types that we can push down filter operations to K2, so when we convert them we want to
// strip out the Datum headers
inline bool isStringType(Oid oid) {
    return (oid == VARCHAROID || oid == BPCHAROID || oid == TEXTOID || oid == CLOBOID || oid == CSTRINGOID);
}

// Type to size association taken from MOT column.cpp. Note that this does not determine whether we can use the type as a key or for pushdown, only that it will fit in a K2 native type
inline bool is1ByteIntType(Oid oid) {
    return (oid == CHAROID || oid == INT1OID);
}

inline bool is2ByteIntType(Oid oid) {
    return (oid == INT2OID);
}

inline bool is4ByteIntType(Oid oid) {
    return (oid == INT4OID || oid == DATEOID);
}

// These are types that are uint32_t for PG, but for SKV we promote them to INT64 since SKV does not support unsigned types
inline bool isUnsignedPromotedType(Oid oid) {
    return (oid == OIDOID || oid == CIDOID || XIDOID);
}

inline bool is8ByteIntType(Oid oid) {
    return (oid == TIDOID || oid == INT8OID || oid == TIMESTAMPOID || oid == TIMESTAMPTZOID || oid == TIMEOID || oid == INTERVALOID || oid == TINTERVALOID || oid == TIMETZOID);
}

inline bool isPushdownType(Oid oid) {
    return isStringType(oid) || isUnsignedPromotedType(oid) || (oid == CHAROID || oid == INT1OID || oid == INT2OID || oid == INT4OID ||
        oid == DATEOID || oid == TIMEOID || oid == INT8OID || oid == FLOAT4OID || oid == FLOAT8OID || oid == BOOLOID || oid == NAMEOID);
}

inline skv::http::dto::FieldType OidToK2Type(Oid type_oid) {
    using namespace skv::http::dto;

    if (is1ByteIntType(type_oid) || is2ByteIntType(type_oid)) {
        return FieldType::INT16T;
    }
    else if (is4ByteIntType(type_oid)) {
        return FieldType::INT32T;
    }
    else if (is8ByteIntType(type_oid) || isUnsignedPromotedType(type_oid)) {
        return FieldType::INT64T;
    }
    else if (type_oid == FLOAT4OID) {
        return FieldType::FLOAT;
    }
    else if (type_oid == FLOAT8OID) {
        return FieldType::DOUBLE;
    }
    else if (type_oid == BOOLOID) {
        return FieldType::BOOL;
    }

    return FieldType::STRING;
}


} // ns k2pg
