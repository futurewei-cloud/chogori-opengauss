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
#include <skvhttp/client/SKVClient.h>
#include "config.h"

namespace k2fdw {
namespace sh=skv::http;

class TxnManager {
public:
    // this method returns the current active transaction in this manager, creating a new one if needed
    sh::Response<std::shared_ptr<sh::TxnHandle>> BeginTxn(sh::dto::TxnOptions opts);

    // this method returns the current active transaction in this manager, or null ptr if one doesn't exist
    std::shared_ptr<sh::TxnHandle> GetTxn();

    // transactions should be ended via this call to ensure the thread-local state is maintained
    sh::Response<> EndTxn(sh::dto::EndAction endAction);

private:
    // Helper used to initialize the manager.
    void _Init();

    // this txn is managed by this manager.
    std::shared_ptr<sh::TxnHandle> _txn;

    // share the client among all threads
    static inline std::shared_ptr<sh::Client> _client;
    static inline thread_local Config _config;
};

// the thread-local TxnManager. It allows access to k2 from any thread in opengauss,
// in particular, non-fdw threads of execution.
// The general execution model is that we can have at most one active transaction per thread.
inline thread_local TxnManager TXMgr;
} // ns
