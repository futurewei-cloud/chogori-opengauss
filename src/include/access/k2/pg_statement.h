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

#include <memory>
#include <string>

#include "access/k2/pg_session.h"

namespace k2pg {

// Statement types.
enum StmtOp {
  STMT_NOOP = 0,
  STMT_CREATE_DATABASE,
  STMT_DROP_DATABASE,
  STMT_CREATE_SCHEMA,
  STMT_DROP_SCHEMA,
  STMT_CREATE_TABLE,
  STMT_DROP_TABLE,
  STMT_TRUNCATE_TABLE,
  STMT_CREATE_INDEX,
  STMT_DROP_INDEX,
  STMT_ALTER_TABLE,
  STMT_INSERT,
  STMT_UPDATE,
  STMT_DELETE,
  STMT_TRUNCATE,
  STMT_SELECT,
  STMT_ALTER_DATABASE,
};

class PgStatement {
 public:

  //------------------------------------------------------------------------------------------------
  // Constructors.
  // pg_session is the session that this statement belongs to. If PostgreSQL cancels the session
  // while statement is running, pg_session::sharedptr can still be accessed without crashing.
  explicit PgStatement(std::shared_ptr<PgSession> pg_session);
  virtual ~PgStatement();

  const std::shared_ptr<PgSession>& pg_session() {
    return pg_session_;
  }

  // Statement type.
  virtual StmtOp stmt_op() const = 0;

  //------------------------------------------------------------------------------------------------
  static bool IsValidStmt(PgStatement* stmt, StmtOp op) {
    return (stmt != nullptr && stmt->stmt_op() == op);
  }

  int64_t stmt_id() const {
    return stmt_id_;
  }

 protected:
  // PgSession that this statement belongs to.
  std::shared_ptr<PgSession> pg_session_;

  std::string client_id_;

  int64_t stmt_id_;
};

}  // namespace k2pg
