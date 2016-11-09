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

#ifndef IMPALA_UTIL_OVERFLOW_H_
#define IMPALA_UTIL_OVERFLOW_H_

#include <climits>
#include <cstdint>
#include <cstdlib>
#include <type_traits>

#include "common/logging.h"

/// Arithmetic operations that prevent or check overflow of integer operations. These are
/// especially useful for signed integers, since signed integer overflow has undefined
/// behavior in C++.
namespace impala {

class Overflow {
 private:
  enum struct Signedness { UNSIGNED, SIGNED };

  template <Signedness SIGNEDNESS, int WIDTH>
  struct Int;

  template <typename T>
  static constexpr bool IsInteger() {
    return std::is_integral<T>::value || std::is_same<T, __int128>::value
        || std::is_same<T, unsigned __int128>::value;
  }

  template <typename T, typename U>
  static constexpr int MaxWidth() {
    return CHAR_BIT * ((sizeof(T) < sizeof(U)) ? sizeof(U) : sizeof(T));
  }

  template <typename T>
  static constexpr Signedness GetSignedness() {
    static_assert(IsInteger<T>(), "Non-integers don't have signedness");
    return std::is_same<typename Int<Signedness::UNSIGNED, CHAR_BIT * sizeof(T)>::Type,
               T>::value ?
        Signedness::UNSIGNED :
        Signedness::SIGNED;
  }

  /// A method for operating on integers as if they were unsigned. This forces two's
  /// complement overflow for operations like signed integer overflow that are technically
  /// undefined behavior according to the standard. Example:
  ///
  ///     int x = argc;
  ///     long y = atol(argv[1]);
  ///     long z = AsUnsigned<std::plus<>>(x, y);
  template <typename Operator, typename T, typename U>
  [[gnu::always_inline]] static auto AsUnsigned(const T x, const U y) {
    static_assert(IsInteger<T>() && IsInteger<U>(), "AsUnsigned only works for integers");
    using ResultType = decltype(Operator()(x, y));
    const auto unsigned_x = CastToUnsigned(x);
    const auto unsigned_y = CastToUnsigned(y);
    const auto result = Operator()(unsigned_x, unsigned_y);
    return SignCastTo<ResultType>(result);
  }

 public:
  /// A method for casting from signed to unsigned integers or unsigned to signed integers
  /// that catches some common mistakes by being stricter than reinterpret_cast or
  /// static_cast via the use of static_asserts. Example usage:
  ///
  ///     long x = atol(argc[1]);
  ///     unsigned long y = Overflow::SignCastTo<unsigned long>(x);
  template <typename T, typename U>
  [[gnu::always_inline]] static T SignCastTo(U x) {
    static_assert(IsInteger<T>() && IsInteger<U>(), "SignCastTo only works on integers");
    static_assert(sizeof(T) >= sizeof(U), "SignCastTo cannot do narrowing conversions");
    return static_cast<T>(x);
  }

  /// CastToUnsigned is a specialization of SignCastTo that always casts to the unsigned
  /// type of the same width as its argument. This means that the caller does not have to
  /// specify the result type.
  template <typename T>
  [[gnu::always_inline]] static auto CastToUnsigned(T x) {
    return SignCastTo<typename Int<Signedness::UNSIGNED, sizeof(x) * CHAR_BIT>::Type>(x);
  }

  /// UnsignedSum, UnsignedDifference, and UnsignedProduct all use AsUnsigned to perform
  /// addition, subtraction, and multiplication on their arguments, respectively, without
  /// performing any underfined operations.
  template <typename T, typename U>
  [[gnu::always_inline]] static auto UnsignedSum(const T x, const U y) {
    return AsUnsigned<std::plus<>>(x, y);
  }

  template <typename T, typename U>
  [[gnu::always_inline]] static auto UnsignedDifference(const T x, const U y) {
    return AsUnsigned<std::minus<>>(x, y);
  }

  template <typename T, typename U>
  [[gnu::always_inline]] static auto UnsignedProduct(const T x, const U y) {
    return AsUnsigned<std::multiplies<>>(x, y);
  }

  /// CheckedSum(x, y, &overflow) returns the sum of x and y without performing any
  /// undefined operations. It sets overflow to true if the addition overflowed. x and y
  /// must be share an unsigned type.
  template<typename T>
  static auto CheckedSum(T x, T y, bool* overflow) {
    static_assert(Signedness::UNSIGNED == GetSignedness<T>(),
        "CheckedSum only works for unsigned integers");
    const T result = x + y;
    *overflow = result < x;
    return result;
  }

  /// CheckedProduct(x, y, &overflow) returns the product of x and y without performing
  /// any undefined operations. It sets overflow to true if the multiplication overflowed.
  template <typename T, typename U>
  static auto CheckedProduct(T x, U y, bool * overflow) {
    static_assert(IsInteger<T>() && IsInteger<U>(), "CheckedProduct only works for ints");
    constexpr Signedness SIGN = (std::is_signed<T>::value || std::is_signed<U>::value) ?
        Signedness::SIGNED : Signedness::UNSIGNED;
    using ResultType = decltype(x * y);
    using DoubleWide = typename Int<SIGN, 2 * MaxWidth<T, U>()>::Type;
    const DoubleWide wide_result =
        static_cast<DoubleWide>(x) * static_cast<DoubleWide>(y);
    // Casting result to a smaller type is implementation-defined behavior, not undefined
    // behavior.
    const ResultType result = static_cast<ResultType>(wide_result);
    *overflow = (result != wide_result);
    return result;
  }

  /// std::abs() has undefined behavior when its argument is negative and the negation of
  /// its argument is not representable. UnsignedAbs() returns the absolute value as an
  /// unsigned type to avoid that undefined behavior.
  template <typename T>
  [[gnu::always_inline]] static auto UnsignedAbs(const T x) {
    if (std::is_signed<T>::value && x == std::numeric_limits<T>::min()) {
      return 1 + CastToUnsigned(std::abs(x + 1));
    } else {
      return CastToUnsigned(std::abs(x));
    }
  }
};

#pragma push_macro("DECL_SIZED_INT")

#define DECL_SIZED_INT(N)                                   \
  template <>                                               \
  struct Overflow::Int<Overflow::Signedness::UNSIGNED, N> { \
    using Type = uint##N##_t;                               \
  };                                                        \
  template <>                                               \
  struct Overflow::Int<Overflow::Signedness::SIGNED, N> {   \
    using Type = int##N##_t;                                \
  }

DECL_SIZED_INT(8);
DECL_SIZED_INT(16);
DECL_SIZED_INT(32);
DECL_SIZED_INT(64);

#pragma pop_macro("DECL_SIZED_INT")

template <>
struct Overflow::Int<Overflow::Signedness::UNSIGNED, 128> {
  using Type = unsigned __int128;
};
template <>
struct Overflow::Int<Overflow::Signedness::SIGNED, 128> {
  using Type = __int128;
};

} // namespace impala

#endif // IMPALA_UTIL_OVERFLOW_H_
