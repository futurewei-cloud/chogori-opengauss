// Copyright(c) 2022 Futurewei Cloud
//
// Permission is hereby granted,
//        free of charge, to any person obtaining a copy of this software and associated documentation files(the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and / or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :
//
// The above copyright notice and this permission notice shall be included in all copies
// or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS",
// WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE
//        AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
//        DAMAGES OR OTHER LIABILITY,
// WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

#pragma once

#include <nlohmann/json.hpp>
#include <string>
#include <k2/logging/Log.h>

namespace k2fdw {
namespace log {
inline thread_local k2::logging::Logger k2fdw("k2::fdw");
}

class Config {
public:
    Config();
    ~Config();
    nlohmann::json& operator()() {
        return _config;
    }

    template<typename T>
    T get(const std::string& key, T defaultV) {
        auto iter = _config.find(key);
        if (iter != _config.end()) {
            return iter.value();
        }
        else {
            return std::move(defaultV);
        }
    }

    k2::Duration getDurationMillis(const std::string& key, k2::Duration defaultV) {
        return k2::Duration(get<uint64_t>(key, k2::msec(defaultV).count())*1'000'000);
    }
private:
    nlohmann::json _config;
};

} // ns
