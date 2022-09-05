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

#include <vector>
#include <assert.h>

#include "utils/elog.h"

#include "access/k2/pg_type.h"

namespace k2pg {

    // The following functions are to construct QLType objects.
    std::shared_ptr<SQLType> SQLType::Create(DataType data_type, const std::vector<std::shared_ptr<SQLType>>& params) {
        switch (data_type) {
            case DataType::K2SQL_DATA_TYPE_LIST:
                assert(params.size() == 1);
                return CreateCollectionType<DataType::K2SQL_DATA_TYPE_LIST>(params);
            case DataType::K2SQL_DATA_TYPE_MAP:
                assert(params.size() == 2);
                return CreateCollectionType<DataType::K2SQL_DATA_TYPE_MAP>(params);
            case DataType::K2SQL_DATA_TYPE_SET:
                assert(params.size() == 1);
                return CreateCollectionType<DataType::K2SQL_DATA_TYPE_SET>(params);
            default:
                assert(params.size() == 0);
                return Create(data_type);
        }
    }

    std::shared_ptr<SQLType> SQLType::Create(DataType data_type) {
        switch (data_type) {
            case DataType::K2SQL_DATA_TYPE_UNKNOWN_DATA:
                return CreatePrimitiveType<DataType::K2SQL_DATA_TYPE_UNKNOWN_DATA>();
            case DataType::K2SQL_DATA_TYPE_NULL_VALUE_TYPE:
                return CreatePrimitiveType<DataType::K2SQL_DATA_TYPE_NULL_VALUE_TYPE>();
            case DataType::K2SQL_DATA_TYPE_UINT8:
                return CreatePrimitiveType<DataType::K2SQL_DATA_TYPE_UINT8>();
            case DataType::K2SQL_DATA_TYPE_INT8:
                return CreatePrimitiveType<DataType::K2SQL_DATA_TYPE_INT8>();
            case DataType::K2SQL_DATA_TYPE_UINT16:
                return CreatePrimitiveType<DataType::K2SQL_DATA_TYPE_UINT16>();
            case DataType::K2SQL_DATA_TYPE_INT16:
                return CreatePrimitiveType<DataType::K2SQL_DATA_TYPE_INT16>();
            case DataType::K2SQL_DATA_TYPE_UINT32:
                return CreatePrimitiveType<DataType::K2SQL_DATA_TYPE_UINT32>();
            case DataType::K2SQL_DATA_TYPE_INT32:
                return CreatePrimitiveType<DataType::K2SQL_DATA_TYPE_INT32>();
            case DataType::K2SQL_DATA_TYPE_UINT64:
                return CreatePrimitiveType<DataType::K2SQL_DATA_TYPE_UINT64>();
            case DataType::K2SQL_DATA_TYPE_INT64:
                return CreatePrimitiveType<DataType::K2SQL_DATA_TYPE_INT64>();
            case DataType::K2SQL_DATA_TYPE_STRING:
                return CreatePrimitiveType<DataType::K2SQL_DATA_TYPE_STRING>();
            case DataType::K2SQL_DATA_TYPE_BOOL:
                return CreatePrimitiveType<DataType::K2SQL_DATA_TYPE_BOOL>();
            case DataType::K2SQL_DATA_TYPE_FLOAT:
                return CreatePrimitiveType<DataType::K2SQL_DATA_TYPE_FLOAT>();
            case DataType::K2SQL_DATA_TYPE_DOUBLE:
                return CreatePrimitiveType<DataType::K2SQL_DATA_TYPE_DOUBLE>();
            case DataType::K2SQL_DATA_TYPE_BINARY:
                return CreatePrimitiveType<DataType::K2SQL_DATA_TYPE_BINARY>();
            case DataType::K2SQL_DATA_TYPE_TIMESTAMP:
                return CreatePrimitiveType<DataType::K2SQL_DATA_TYPE_TIMESTAMP>();
            case DataType::K2SQL_DATA_TYPE_DECIMAL:
                return CreatePrimitiveType<DataType::K2SQL_DATA_TYPE_DECIMAL>();
            case DataType::K2SQL_DATA_TYPE_DATE:
                return CreatePrimitiveType<DataType::K2SQL_DATA_TYPE_DATE>();
            case DataType::K2SQL_DATA_TYPE_TIME:
                return CreatePrimitiveType<DataType::K2SQL_DATA_TYPE_TIME>();

                // Create empty parametric types and raise error during semantic check.
            case DataType::K2SQL_DATA_TYPE_LIST:
                return CreateTypeList();
            case DataType::K2SQL_DATA_TYPE_MAP:
                return CreateTypeMap();
            case DataType::K2SQL_DATA_TYPE_SET:
                return CreateTypeSet();

            default:
                elog(FATAL, "Not supported datatype %d", data_type);
                return nullptr;
        }
    }

    bool SQLType::IsValidPrimaryType(DataType type) {
        switch (type) {
            case DataType::K2SQL_DATA_TYPE_MAP: [[fallthrough]];
            case DataType::K2SQL_DATA_TYPE_SET: [[fallthrough]];
            case DataType::K2SQL_DATA_TYPE_LIST:
                return false;

            default:
                // Let all other types go. Because we already process column datatype before getting here,
                // just assume that they are all valid types.
                return true;
        }
    }

    std::shared_ptr<SQLType> SQLType::CreateTypeMap(std::shared_ptr<SQLType> key_type,
                                             std::shared_ptr<SQLType> value_type) {
        std::vector<std::shared_ptr<SQLType>> params = {key_type, value_type};
        return CreateCollectionType<DataType::K2SQL_DATA_TYPE_MAP>(params);
    }

    std::shared_ptr<SQLType> SQLType::CreateTypeMap(DataType key_type, DataType value_type) {
        return CreateTypeMap(SQLType::Create(key_type), SQLType::Create(value_type));
    }

    std::shared_ptr<SQLType> SQLType::CreateTypeList(std::shared_ptr<SQLType> value_type) {
        std::vector<std::shared_ptr<SQLType>> params(1, value_type);
        return CreateCollectionType<DataType::K2SQL_DATA_TYPE_LIST>(params);
    }

    std::shared_ptr<SQLType> SQLType::CreateTypeList(DataType value_type) {
        return CreateTypeList(SQLType::Create(value_type));
    }

    std::shared_ptr<SQLType> SQLType::CreateTypeSet(std::shared_ptr<SQLType> value_type) {
        std::vector<std::shared_ptr<SQLType>> params(1, value_type);
        return CreateCollectionType<DataType::K2SQL_DATA_TYPE_SET>(params);
    }

    std::shared_ptr<SQLType> SQLType::CreateTypeSet(DataType value_type) {
        return CreateTypeSet(SQLType::Create(value_type));
    }

}  // namespace k2pg
