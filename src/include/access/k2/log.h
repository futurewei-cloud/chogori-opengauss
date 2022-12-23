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
#include <cxxabi.h>
#include <dlfcn.h>
#include <execinfo.h>
#include <k2/logging/Log.h>

namespace k2log {
inline thread_local k2::logging::Logger k2pg("k2::pg");

inline bool trace_read_ops = false;
inline bool trace_write_ops = false;
inline bool trace_control_ops = false;

class trace {
    friend std::ostream& operator<<(std::ostream& os, const trace&) {
        const size_t MAX_DEPTH = 25;
        void* traces[MAX_DEPTH];
        int numFrames = ::backtrace(traces, MAX_DEPTH);
        char** symbols = ::backtrace_symbols(traces, numFrames);
        // start at 1 to skip the first frame (this operator<<)
        for (int i = 1; i < numFrames; i++) {
            os << (i > 1 ? ", [#" : "[#") << i << " " << ((void*)traces[i]) << " ";
            Dl_info info;
            if (::dladdr(traces[i], &info)) {
                int status = 0;
                char* demangled = abi::__cxa_demangle(info.dli_sname, NULL, 0, &status);
                if (demangled && status == 0) {
                    os << demangled;
                } else if (info.dli_sname) {
                    os << info.dli_sname;
                } else {
                    os << symbols[i];
                }
                if (demangled) free(demangled);
            } else {
                os << "sym: " << symbols[i];
            }
            os << "]";
        }

        if (symbols) free(symbols);
        return os;
    }
};
static inline const trace TR;

static inline void initialize(const std::string& mainLevel, const std::string& tracedOps, const std::map<std::string, std::string>& moduleOverrides) {
    k2::logging::Logger::threadLocalLogLevel = k2::logging::LogLevelFromStr(mainLevel);
    K2LOG_I(k2log::k2pg, "global log level: {}", mainLevel);
    for (auto& [module, levelstr] : moduleOverrides) {
        K2LOG_I(k2log::k2pg, "level override {} -- {}", module, levelstr);
        auto level = k2::logging::LogLevelFromStr(levelstr);
        k2::logging::Logger::moduleLevels[module] = level;
        // ... and if the logger for this module is already created, update the live object with the override level
        auto it = k2::logging::Logger::moduleLoggers.find(module);
        if (it != k2::logging::Logger::moduleLoggers.end()) {
            it->second->moduleLevel = level;
        }
    }

    // configure traced groups
    k2log::trace_read_ops = tracedOps.find('r') != std::string::npos || tracedOps.find('R') != std::string::npos;
    k2log::trace_write_ops = tracedOps.find('w') != std::string::npos || tracedOps.find('W') != std::string::npos;
    k2log::trace_control_ops = tracedOps.find('c') != std::string::npos || tracedOps.find('C') != std::string::npos;
    K2LOG_I(k2log::k2pg, "tracedOps: {}, r={}, w={}, c={}", tracedOps, k2log::trace_read_ops, k2log::trace_write_ops, k2log::trace_control_ops);
}

} // ns

#define K2TRACE_D(enabled, logger, fmtstr, ...)                             \
    if ((enabled)) {                                                        \
        K2LOG_D(logger, fmtstr " -- TRACE : {}", ##__VA_ARGS__, k2log::TR); \
    } else {                                                                \
        K2LOG_D(logger, fmtstr, ##__VA_ARGS__);                             \
    }
#define K2TRACE_I(enabled, logger, fmtstr, ...)                             \
    if ((enabled)) {                                                        \
        K2LOG_I(logger, fmtstr " -- TRACE : {}", ##__VA_ARGS__, k2log::TR); \
    } else {                                                                \
        K2LOG_I(logger, fmtstr, ##__VA_ARGS__);                             \
    }
#define K2TRACE_W(enabled, logger, fmtstr, ...)                             \
    if ((enabled)) {                                                        \
        K2LOG_W(logger, fmtstr " -- TRACE : {}", ##__VA_ARGS__, k2log::TR); \
    } else {                                                                \
        K2LOG_W(logger, fmtstr, ##__VA_ARGS__);                             \
    }
#define K2TRACE_E(enabled, logger, fmtstr, ...)                             \
    if ((enabled)) {                                                        \
        K2LOG_E(logger, fmtstr " -- TRACE : {}", ##__VA_ARGS__, k2log::TR); \
    } else {                                                                \
        K2LOG_E(logger, fmtstr, ##__VA_ARGS__);                             \
    }

// various macros to allow logging with trace per group.
// the groups are R(read), W(write), C(control)
// Debug for READ group with trace
#define K2LOG_DRT(logger, fmtstr, ...) K2TRACE_D(k2log::trace_read_ops, logger, fmtstr, ##__VA_ARGS__);
// Debug for WRITE group with trace
#define K2LOG_DWT(logger, fmtstr, ...) K2TRACE_D(k2log::trace_write_ops, logger, fmtstr, ##__VA_ARGS__);
// Debug for CONTROL group with trace
#define K2LOG_DCT(logger, fmtstr, ...) K2TRACE_D(k2log::trace_control_ops, logger, fmtstr, ##__VA_ARGS__);

#define K2LOG_IRT(logger, fmtstr, ...) K2TRACE_I(k2log::trace_read_ops, logger, fmtstr, ##__VA_ARGS__);
#define K2LOG_IWT(logger, fmtstr, ...) K2TRACE_I(k2log::trace_write_ops, logger, fmtstr, ##__VA_ARGS__);
#define K2LOG_ICT(logger, fmtstr, ...) K2TRACE_I(k2log::trace_control_ops, logger, fmtstr, ##__VA_ARGS__);

#define K2LOG_WRT(logger, fmtstr, ...) K2TRACE_W(k2log::trace_read_ops, logger, fmtstr, ##__VA_ARGS__);
#define K2LOG_WWT(logger, fmtstr, ...) K2TRACE_W(k2log::trace_write_ops, logger, fmtstr, ##__VA_ARGS__);
#define K2LOG_WCT(logger, fmtstr, ...) K2TRACE_W(k2log::trace_control_ops, logger, fmtstr, ##__VA_ARGS__);

#define K2LOG_ERT(logger, fmtstr, ...) K2TRACE_E(k2log::trace_read_ops, logger, fmtstr, ##__VA_ARGS__);
#define K2LOG_EWT(logger, fmtstr, ...) K2TRACE_E(k2log::trace_write_ops, logger, fmtstr, ##__VA_ARGS__);
#define K2LOG_ECT(logger, fmtstr, ...) K2TRACE_E(k2log::trace_control_ops, logger, fmtstr, ##__VA_ARGS__);


namespace k2pg {
class Metric {
   public:
    Metric(const char* name, k2::Duration warnThr) : _name(name), _warnThr(warnThr), _start(k2::Clock::now()) {}
    Metric() {}
    Metric(Metric&& o) {
        _name = o._name;
        _warnThr = o._warnThr;
        _start = o._start;
        o._name = "";
        o._warnThr = k2::Duration{};
        o._start = k2::TimePoint{};
    }
    Metric& operator=(Metric&& o) {
        _name = o._name;
        _warnThr = o._warnThr;
        _start = o._start;
        o._name = "";
        o._warnThr = k2::Duration{};
        o._start = k2::TimePoint{};
        return *this;
    }
    void report() const {
        // if (_warnThr > 0ns) {
        //     auto elapsed = k2::Clock::now() - _start;
        //     if (elapsed >= _warnThr) {
        //         // K2LOG_WCT(k2log::k2pg, "Metric {} exceeded threshold {}: {}", _name, _warnThr, elapsed);
        //     } else {
        //         // K2LOG_DCT(k2log::k2pg, "Metric {} with threshold {}: {}", _name, _warnThr, elapsed);
        //     }
        // }
    }

   private:
    const char* _name{""};
    k2::Duration _warnThr;
    k2::TimePoint _start;
};
}
