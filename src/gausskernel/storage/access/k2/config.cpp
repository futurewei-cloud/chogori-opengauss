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

#include "config.h"

#include <cstdlib>
#include <fstream>

namespace k2pg {

Config::Config(nlohmann::json conf) : _config(std::move(conf)) {
}

Config::Config() {
    const char* configFileName = getenv("K2_CONFIG_FILE");
    if (NULL == configFileName) {
        K2LOG_W(log::k2pg, "No config file given for K2 configuration in the K2_CONFIG_FILE env variable");
        return;
    }

    // read the config file
    K2LOG_I(log::k2pg, "{}", configFileName);
    std::ifstream ifile(configFileName);
    ifile >> _config;
}

Config::~Config(){
}

Config Config::sub(const std::string& key) {
    auto iter = _config.find(key);
    if (iter != _config.end() && iter.value().is_object()) {
        return Config(iter.value());
    }
    return Config(nlohmann::json{});
}

Config Config::sub(size_t pos) {
    auto obj = _config.at(pos);
    if (obj.is_object()) {
        return Config(std::move(obj));
    }
    return Config(nlohmann::json{});
}
}
