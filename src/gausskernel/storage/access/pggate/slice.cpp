// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// Portions Copyright (c) 2021 Futurewei Cloud
//
// The following only applies to changes made to this file as part of YugaByte development.
//
// Portions Copyright (c) YugaByte, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied.  See the License for the specific language governing permissions and limitations
// under the License.
//

#include <sstream>
#include "slice.h"

#include "status.h"

namespace k2pg {
int32_t non_graph_characters_percentage_to_use_hexadecimal_rendering = 10;

Status Slice::check_size(size_t expected_size) const {
  if (size() != expected_size) {
    std::stringstream oss;
    oss << "Unexpected Slice size, expected " << expected_size << " but got " << size();
    return STATUS(Corruption, oss.str());
  }
  return Status::OK();
}

void Slice::CopyToBuffer(std::string* buffer) const {
  buffer->assign(cdata(), size());
}

// Return a string that contains the copy of the referenced data.
std::string Slice::ToBuffer() const {
  return std::string(cdata(), size());
}

std::string Slice::ToString(bool hex) const {
//  return hex ? ToDebugHexString() : ToString();
  return hex ? ToDebugHexString() : ToBuffer();
}

std::string Slice::ToDebugHexString() const {
  std::string result;
  char buf[10];
  for (auto i = begin_; i != end_; ++i) {
    snprintf(buf, sizeof(buf), "%02X", *i);
    result += buf;
  }
  return result;
}

std::string Slice::ToDebugString(size_t max_len) const {
  size_t bytes_to_print = size();
  bool abbreviated = false;
  if (max_len != 0 && bytes_to_print > max_len) {
    bytes_to_print = max_len;
    abbreviated = true;
  }

  int num_not_graph = 0;
  for (size_t i = 0; i < bytes_to_print; i++) {
    if (!isgraph(begin_[i])) {
      ++num_not_graph;
    }
  }

  if (num_not_graph * 100 >

      bytes_to_print * non_graph_characters_percentage_to_use_hexadecimal_rendering) {
    return ToDebugHexString();
  }

  std::ostringstream oss;
  for (int i = 0; i < bytes_to_print; i++) {
    auto ch = begin_[i];
    if (!isgraph(ch)) {
      if (ch == '\r') {
        oss << "\\r";
      } else if (ch == '\n') {
        oss << "\\n";
      } else if (ch == ' ') {
        oss << ' ';
      } else {
        oss << std::hex << (ch & 0xff);
      }
    } else {
      oss << ch;
    }
  }
  if (abbreviated) {
    oss << "...<" << this->size() << " bytes total>";
  }
  return oss.str();
}

Slice::Slice(const SliceParts& parts, std::string* buf) {
  size_t length = 0;
  for (int i = 0; i < parts.num_parts; ++i) {
    length += parts.parts[i].size();
  }
  buf->reserve(length);

  for (int i = 0; i < parts.num_parts; ++i) {
    buf->append(parts.parts[i].cdata(), parts.parts[i].size());
  }
  *this = Slice(*buf);
}

Status Slice::consume_byte(char c) {
  char consumed = consume_byte();
  if (consumed != c) {
    std::stringstream oss;
    oss << "Wrong first byte, expected " << static_cast<int>(c) << " but found " << static_cast<int>(consumed);
    return STATUS(Corruption, oss.str());
  }

  return Status::OK();
}

}  // namespace k2pg
