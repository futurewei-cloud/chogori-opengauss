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

#include "access/k2/data_type.h"

namespace k2pg {

    class SQLType {
        public:
        typedef std::shared_ptr<SQLType> SharedPtr;

        // Constructor for elementary types
        explicit SQLType(DataType sql_typeid) : id_(sql_typeid), params_(0) {
        }

        // Constructor for collection types
        SQLType(DataType sql_typeid, const std::vector<std::shared_ptr<SQLType>>& params)
            : id_(sql_typeid), params_(params) {
        }

        virtual ~SQLType() {
        }

        const DataType id() const {
            return id_;
        }

        const std::vector<std::shared_ptr<SQLType>>& params() const {
            return params_;
        }

        const SQLType::SharedPtr& param_type(int member_index = 0) const {
            // TODO: add index validation
            return params_[member_index];
        }

        bool IsCollection() const {
            return id_ == K2SQL_DATA_TYPE_MAP || id_ == K2SQL_DATA_TYPE_SET || id_ == K2SQL_DATA_TYPE_LIST;
        }

        bool IsUnknown() const {
            return IsUnknown(id_);
        }

        bool IsAnyType() const {
            return IsNull(id_);
        }

        bool IsInteger() const {
            return IsInteger(id_);
        }

        bool IsElementary() const {
            return !IsCollection();
        }

        bool IsValid() const {
            if (IsElementary()) {
                return params_.empty();
            } else {
                // checking number of params
                if (id_ == K2SQL_DATA_TYPE_MAP && params_.size() != 2) {
                    return false; // expect two type parameters for maps
                } else if ((id_ == K2SQL_DATA_TYPE_SET || id_ == K2SQL_DATA_TYPE_LIST) && params_.size() != 1) {
                    return false; // expect one type parameter for set and list
                }
                // recursively checking params
                for (const auto &param : params_) {
                    if (!param->IsValid()) return false;
                }
                return true;
            }
        }

        bool Contains(DataType id) const {
            for (const std::shared_ptr<SQLType>& param : params_) {
                if (param->Contains(id)) {
                    return true;
                }
            }
            return id_ == id;
        }

        bool operator ==(const SQLType& other) const {
            if (id_ == other.id_ && params_.size() == other.params_.size()) {
                for (size_t i = 0; i < params_.size(); i++) {
                    if (*params_[i] == *other.params_[i]) {
                        continue;
                    }
                    return false;
                }
                return true;
            }

            return false;
        }

        bool operator !=(const SQLType& other) const {
            return !(*this == other);
        }

        // static methods
        // Create all builtin types including collection.
        static std::shared_ptr<SQLType> Create(DataType data_type, const std::vector<std::shared_ptr<SQLType>>& params);

        // Create primitive types, all builtin types except collection.
        static std::shared_ptr<SQLType> Create(DataType data_type);

        // Check type methods.
        static bool IsValidPrimaryType(DataType type);

        static bool IsInteger(DataType t) {
            return (t >= K2SQL_DATA_TYPE_INT8 && t <= K2SQL_DATA_TYPE_INT64); // || t == K2SQL_DATA_TYPE_VARINT;
        }

        static bool IsNumeric(DataType t) {
            return IsInteger(t) || t == K2SQL_DATA_TYPE_FLOAT || t == K2SQL_DATA_TYPE_DOUBLE || t == K2SQL_DATA_TYPE_DECIMAL;
        }

        // NULL_VALUE_TYPE represents type of a null value.
        static bool IsNull(DataType t) {
            return t == K2SQL_DATA_TYPE_NULL_VALUE_TYPE;
        }

        // Type is not yet set (VOID).
        static bool IsUnknown(DataType t) {
            return t == DataType::K2SQL_DATA_TYPE_UNKNOWN_DATA;
        }

        private:
        // Data members.
        DataType id_;

        std::vector<std::shared_ptr<SQLType>> params_;
    };

}  // namespace k2pg
