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

#include <cstdint>
#include <limits>

using namespace std;

#include "testutil/gtest-util.h"
#include "util/overflow.h"

namespace impala {

namespace {

// Convert signed two's complement number to the unsigned equivalent. This should mimic
// SignCastTo.
template <typename Signed, typename Unsigned>
Unsigned ArithmeticCastToUnsigned(Signed x) {
  Unsigned result;
  if (x >= 0) {
    result = x;
  } else {
    ++x;
    x = -x;
    result = x;
    result = ~result;
  }
  return result;
}

// Convert unsigned number to signed two's complement equivalent. This should mimic
// SignCastTo.
template <typename Signed, typename Unsigned>
Signed ArithmeticCastToSigned(Unsigned x) {
  Signed result;
  if (x < (static_cast<Unsigned>(1) << (CHAR_BIT * sizeof(x) - 1))) {
    result = x;
  } else {
    --x;
    x = ~x;
    --x;
    result = x;
    result = -result;
    --result;
  }
  return result;
}

// Convert back and forth between signed two's complement and unsigned numbers, calling
// EXPECT_EQ on each conversion.
template <typename Signed, typename Unsigned>
void ExpectConversionEquality(Signed sign, Unsigned unsign) {
  static_assert(sizeof(Signed) == sizeof(Unsigned), "Attempt to cast across bitwidths");
  EXPECT_EQ(unsign, Overflow::SignCastTo<Unsigned>(sign)) << "Bit width: "
                                                          << CHAR_BIT * sizeof(Signed);
  EXPECT_EQ(sign, Overflow::SignCastTo<Signed>(unsign)) << "Bit width: "
                                                        << CHAR_BIT * sizeof(Signed);
  EXPECT_EQ(unsign, (ArithmeticCastToUnsigned<Signed, Unsigned>(sign)))
      << "Bit width: " << CHAR_BIT * sizeof(Signed);
  EXPECT_EQ(sign, (ArithmeticCastToSigned<Signed, Unsigned>(unsign)))
      << "Bit width: " << CHAR_BIT * sizeof(Signed);
}

constexpr unsigned __int128 UINT128_MAX = ~static_cast<unsigned __int128>(0);
constexpr __int128 INT128_MAX = UINT128_MAX >> 1;
constexpr __int128 INT128_MIN = -INT128_MAX - 1;

} // anonymous namespace

TEST(Overflow, SignCastTo) {
  // unsigned 0b1111.... (aka numeric_limits<...>::max(), aka ~0) is -1 in two's
  // complement:
  ExpectConversionEquality<int8_t>(-1, numeric_limits<uint8_t>::max());
  ExpectConversionEquality<int16_t>(-1, numeric_limits<uint16_t>::max());
  ExpectConversionEquality<int32_t>(-1, numeric_limits<uint32_t>::max());
  ExpectConversionEquality<int64_t>(-1, numeric_limits<uint64_t>::max());
  ExpectConversionEquality<__int128>(-1, ~static_cast<unsigned __int128>(0));

  // unsigned 0b010000.... is the lowest negative number in two's complement:
  ExpectConversionEquality<int8_t, uint8_t>(
      numeric_limits<int8_t>::min(), 1 + numeric_limits<uint8_t>::max() / 2);
  ExpectConversionEquality<int16_t, uint16_t>(
      numeric_limits<int16_t>::min(), 1 + numeric_limits<uint16_t>::max() / 2);
  ExpectConversionEquality<int32_t, uint32_t>(
      numeric_limits<int32_t>::min(), 1 + numeric_limits<uint32_t>::max() / 2);
  ExpectConversionEquality<int64_t, uint64_t>(
      numeric_limits<int64_t>::min(), 1 + numeric_limits<uint64_t>::max() / 2);
  ExpectConversionEquality(INT128_MIN, 1 + UINT128_MAX / 2);
}

TEST(Overflow, AsUnsigned) {
  // Addition and subtraction wrap in unsigned arithmetic. In these tests, the result of
  // Overflow::Unsigned_() are sometimes int when the arguments are shorter than int,
  // because in C++ the type of x + y is int if x and y can be converted to int
  // losslessly.
  EXPECT_EQ(-128, static_cast<int8_t>(Overflow::UnsignedSum<int8_t, int8_t>(-127, -1)));
  EXPECT_EQ(1, Overflow::UnsignedSum(-0x7fffffff, numeric_limits<int32_t>::min()));
  EXPECT_EQ(numeric_limits<int16_t>::min(),
      static_cast<int16_t>(Overflow::UnsignedSum<int16_t, int16_t>(
          numeric_limits<int16_t>::max(), 1)));
  EXPECT_EQ(numeric_limits<int64_t>::min(),
      Overflow::UnsignedDifference(numeric_limits<int64_t>::max(), -1L));
  EXPECT_EQ(
      INT128_MIN, Overflow::UnsignedDifference(INT128_MAX, static_cast<__int128>(-1)));

  // Overflowing unsigned 128-bit multiplication:
  constexpr __int128 POW64 = static_cast<__int128>(1) << 64;
  EXPECT_EQ(0, (Overflow::UnsignedProduct(POW64, POW64)));

  // In unsigned 128-bit integers, POW64 * POW64 / 2 is positive. In two's complement,
  // it is negative:
  EXPECT_EQ(INT128_MIN, (Overflow::UnsignedProduct(POW64, POW64 / 2)));
}

TEST(Overflow, Check) {
  bool overflow;
  EXPECT_EQ(0, Overflow::CheckedSum(~0ul, 1ul, &overflow));
  EXPECT_TRUE(overflow);
  EXPECT_EQ(1, Overflow::CheckedSum(~0ul - 1, 3ul, &overflow));
  EXPECT_TRUE(overflow);
  EXPECT_EQ(~0ul - 1, Overflow::CheckedSum(~0ul, ~0ul, &overflow));
  EXPECT_TRUE(overflow);

  EXPECT_EQ(1ul << 63, Overflow::CheckedProduct(1ul << 32, 1ul << 31, &overflow));
  EXPECT_FALSE(overflow);
  EXPECT_EQ(0, Overflow::CheckedProduct(1ul << 32, 1ul << 32, &overflow));
  EXPECT_TRUE(overflow);
  EXPECT_EQ(
      ~0ul << 32, Overflow::CheckedProduct(1ul << 32, ((1ul << 32) - 1), &overflow));
  EXPECT_FALSE(overflow);
  EXPECT_EQ(numeric_limits<long>::min(),
      Overflow::CheckedProduct(numeric_limits<long>::min(), -1, &overflow));
  EXPECT_TRUE(overflow);
  EXPECT_EQ(numeric_limits<long>::max(),
      Overflow::CheckedProduct(numeric_limits<long>::min() + 1, -1, &overflow));
}

} // namespace impala

IMPALA_TEST_MAIN();
