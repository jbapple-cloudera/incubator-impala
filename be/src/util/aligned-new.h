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

#ifndef IMPALA_UTIL_ALIGNED_NEW_H_
#define IMPALA_UTIL_ALIGNED_NEW_H_

#include <memory>

namespace impala {

// Objects that must be allocated at alignment greater than that promised by the global
// new (16) can inherit publicly from AlignedNew.
template <size_t ALIGNMENT>
struct AlignedNew {
  static_assert(ALIGNMENT > 0, "ALIGNMENT must be positive");
  static_assert((ALIGNMENT & (ALIGNMENT - 1)) == 0, "ALIGNMENT must be a power of 2");
  static_assert(
      (ALIGNMENT % sizeof(void*)) == 0, "ALIGNMENT must be a multiple of sizeof(void *)");
  static void* operator new(std::size_t count) { return Allocate(count); }
  static void* operator new[](std::size_t count) { return Allocate(count); }
  static void operator delete(void* ptr) { free(ptr); }
  static void operator delete[](void* ptr) { free(ptr); }

 private:
  static void* Allocate(std::size_t count) {
    void* result = nullptr;
    if (posix_memalign(&result, ALIGNMENT, count)) {
      throw std::bad_alloc();
    }
    return result;
  }
};

using CacheLineAligned = AlignedNew<64>;
}

#endif
