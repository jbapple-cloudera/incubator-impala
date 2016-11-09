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

#include "exprs/operators.h"

#include <boost/cstdint.hpp>

#include "exprs/anyval-util.h"
#include "gutil/strings/substitute.h"
#include "runtime/string-value.inline.h"
#include "runtime/timestamp-value.h"
#include "util/overflow.h"

#include "common/names.h"

#define BINARY_OP_FN(NAME, TYPE, OP) \
  TYPE Operators::NAME##_##TYPE##_##TYPE(\
      FunctionContext* c, const TYPE& v1, const TYPE& v2) {\
    if (v1.is_null || v2.is_null) return TYPE::null();\
    return TYPE(v1.val OP v2.val);\
  }

#define BINARY_OP_FN_AS_UNSIGNED(NAME, TYPE, UNSIGNED_OP)   \
  TYPE Operators::NAME##_##TYPE##_##TYPE(                   \
      FunctionContext* c, const TYPE& v1, const TYPE& v2) { \
    if (v1.is_null || v2.is_null) return TYPE::null();      \
    return TYPE(UNSIGNED_OP(v1.val, v2.val));               \
  }

#define BINARY_OP_CHECK_ZERO_FN(NAME, TYPE, OP) \
  TYPE Operators::NAME##_##TYPE##_##TYPE(\
      FunctionContext* c, const TYPE& v1, const TYPE& v2) {\
    if (v1.is_null || v2.is_null || v2.val == 0) return TYPE::null();\
    return TYPE(v1.val OP v2.val);\
  }

#define BITNOT_FN(TYPE)\
  TYPE Operators::Bitnot_##TYPE(FunctionContext* c, const TYPE& v) {\
    if (v.is_null) return TYPE::null();\
    return TYPE(~v.val);\
  }

// Return infinity if overflow.
#define FACTORIAL_FN(TYPE)\
  BigIntVal Operators::Factorial_##TYPE(FunctionContext* c, const TYPE& v) {\
    if (v.is_null) return BigIntVal::null();\
    int64_t fact = ComputeFactorial(v.val); \
    if (fact < 0) { \
      c->SetError(Substitute("factorial $0! too large for BIGINT", v.val).c_str()); \
      return BigIntVal::null(); \
    } \
    return BigIntVal(fact); \
  }

#define BINARY_PREDICATE_NUMERIC_NONNULL(OP, V1, V2) \
  return BooleanVal(V1.val OP V2.val)

#define BINARY_PREDICATE_NONNUMERIC_NONNULL(TYPE, IMPALA_TYPE, OP, V1, V2) \
  IMPALA_TYPE iv1 = IMPALA_TYPE::From##TYPE(V1);\
  IMPALA_TYPE iv2 = IMPALA_TYPE::From##TYPE(V2);\
  return BooleanVal(iv1 OP iv2)

#define BINARY_PREDICATE_CHAR_NONNULL(OP, V1, V2) \
  StringValue iv1 = StringValue::FromStringVal(V1);\
  StringValue iv2 = StringValue::FromStringVal(V2);\
  iv1.len = StringValue::UnpaddedCharLength(iv1.ptr, c->GetArgType(0)->len); \
  iv2.len = StringValue::UnpaddedCharLength(iv2.ptr, c->GetArgType(1)->len); \
  return BooleanVal(iv1 OP iv2)

#define BINARY_PREDICATE_NUMERIC_FN(NAME, TYPE, OP) \
  BooleanVal Operators::NAME##_##TYPE##_##TYPE(\
      FunctionContext* c, const TYPE& v1, const TYPE& v2) {\
    if (v1.is_null || v2.is_null) return BooleanVal::null();\
    BINARY_PREDICATE_NUMERIC_NONNULL(OP, v1, v2);\
  }

#define BINARY_PREDICATE_NONNUMERIC_FN(NAME, TYPE, IMPALA_TYPE, OP) \
  BooleanVal Operators::NAME##_##TYPE##_##TYPE(\
      FunctionContext* c, const TYPE& v1, const TYPE& v2) {\
    if (v1.is_null || v2.is_null) return BooleanVal::null();\
    BINARY_PREDICATE_NONNUMERIC_NONNULL(TYPE, IMPALA_TYPE, OP, v1, v2);\
  }

#define BINARY_PREDICATE_CHAR(NAME, OP) \
  BooleanVal Operators::NAME##_Char_Char(\
      FunctionContext* c, const StringVal& v1, const StringVal& v2) {\
    if (v1.is_null || v2.is_null) return BooleanVal::null();\
    BINARY_PREDICATE_CHAR_NONNULL(OP, v1, v2);\
  }

#define NULLSAFE_NUMERIC_DISTINCTION(NAME, TYPE, OP, IS_EQUAL) \
  BooleanVal Operators::NAME##_##TYPE##_##TYPE(\
      FunctionContext* c, const TYPE& v1, const TYPE& v2) {\
    if (v1.is_null) return BooleanVal(IS_EQUAL ? v2.is_null : !v2.is_null); \
    if (v2.is_null) return BooleanVal(!IS_EQUAL);\
    BINARY_PREDICATE_NUMERIC_NONNULL(OP, v1, v2);\
  }

#define NULLSAFE_NONNUMERIC_DISTINCTION(NAME, TYPE, IMPALA_TYPE, OP, IS_EQUAL) \
  BooleanVal Operators::NAME##_##TYPE##_##TYPE(\
      FunctionContext* c, const TYPE& v1, const TYPE& v2) {\
    if (v1.is_null) return BooleanVal(IS_EQUAL ? v2.is_null : !v2.is_null); \
    if (v2.is_null) return BooleanVal(!IS_EQUAL);\
    BINARY_PREDICATE_NONNUMERIC_NONNULL(TYPE, IMPALA_TYPE, OP, v1, v2);\
  }

#define NULLSAFE_CHAR_DISTINCTION(NAME, OP, IS_EQUAL) \
  BooleanVal Operators::NAME##_Char_Char(\
      FunctionContext* c, const StringVal& v1, const StringVal& v2) {\
    if (v1.is_null) return BooleanVal(IS_EQUAL ? v2.is_null : !v2.is_null); \
    if (v2.is_null) return BooleanVal(!IS_EQUAL);\
    BINARY_PREDICATE_CHAR_NONNULL(OP, v1, v2);\
  }

#define BINARY_OP_NUMERIC_TYPES(NAME, OP, UNSIGNED_OP)      \
  BINARY_OP_FN_AS_UNSIGNED(NAME, TinyIntVal, UNSIGNED_OP);  \
  BINARY_OP_FN_AS_UNSIGNED(NAME, SmallIntVal, UNSIGNED_OP); \
  BINARY_OP_FN_AS_UNSIGNED(NAME, IntVal, UNSIGNED_OP);      \
  BINARY_OP_FN_AS_UNSIGNED(NAME, BigIntVal, UNSIGNED_OP);   \
  BINARY_OP_FN(NAME, FloatVal, OP);                         \
  BINARY_OP_FN(NAME, DoubleVal, OP);

#define BINARY_OP_INT_TYPES(NAME, OP) \
  BINARY_OP_FN(NAME, TinyIntVal, OP); \
  BINARY_OP_FN(NAME, SmallIntVal, OP);\
  BINARY_OP_FN(NAME, IntVal, OP);\
  BINARY_OP_FN(NAME, BigIntVal, OP);\

#define BINARY_OP_CHECK_ZERO_INT_TYPES(NAME, OP) \
  BINARY_OP_CHECK_ZERO_FN(NAME, TinyIntVal, OP); \
  BINARY_OP_CHECK_ZERO_FN(NAME, SmallIntVal, OP);\
  BINARY_OP_CHECK_ZERO_FN(NAME, IntVal, OP);\
  BINARY_OP_CHECK_ZERO_FN(NAME, BigIntVal, OP);\

#define BINARY_PREDICATE_ALL_TYPES(NAME, OP) \
  BINARY_PREDICATE_NUMERIC_FN(NAME, BooleanVal, OP); \
  BINARY_PREDICATE_NUMERIC_FN(NAME, TinyIntVal, OP); \
  BINARY_PREDICATE_NUMERIC_FN(NAME, SmallIntVal, OP);\
  BINARY_PREDICATE_NUMERIC_FN(NAME, IntVal, OP);\
  BINARY_PREDICATE_NUMERIC_FN(NAME, BigIntVal, OP);\
  BINARY_PREDICATE_NUMERIC_FN(NAME, FloatVal, OP);\
  BINARY_PREDICATE_NUMERIC_FN(NAME, DoubleVal, OP);\
  BINARY_PREDICATE_NONNUMERIC_FN(NAME, StringVal, StringValue, OP);\
  BINARY_PREDICATE_NONNUMERIC_FN(NAME, TimestampVal, TimestampValue, OP);\
  BINARY_PREDICATE_CHAR(NAME, OP);

#define NULLSAFE_DISTINCTION(NAME, OP, IS_EQUAL) \
  NULLSAFE_NUMERIC_DISTINCTION(NAME, BooleanVal, OP, IS_EQUAL); \
  NULLSAFE_NUMERIC_DISTINCTION(NAME, TinyIntVal, OP, IS_EQUAL); \
  NULLSAFE_NUMERIC_DISTINCTION(NAME, SmallIntVal, OP, IS_EQUAL); \
  NULLSAFE_NUMERIC_DISTINCTION(NAME, IntVal, OP, IS_EQUAL); \
  NULLSAFE_NUMERIC_DISTINCTION(NAME, BigIntVal, OP, IS_EQUAL); \
  NULLSAFE_NUMERIC_DISTINCTION(NAME, FloatVal, OP, IS_EQUAL); \
  NULLSAFE_NUMERIC_DISTINCTION(NAME, DoubleVal, OP, IS_EQUAL); \
  NULLSAFE_NONNUMERIC_DISTINCTION(NAME, StringVal, StringValue, OP, IS_EQUAL);\
  NULLSAFE_NONNUMERIC_DISTINCTION(NAME, TimestampVal, TimestampValue, OP, IS_EQUAL);\
  NULLSAFE_CHAR_DISTINCTION(NAME, OP, IS_EQUAL);

namespace impala {

BINARY_OP_NUMERIC_TYPES(Add, +, Overflow::UnsignedSum);
BINARY_OP_NUMERIC_TYPES(Subtract, -, Overflow::UnsignedDifference);
BINARY_OP_NUMERIC_TYPES(Multiply, *, Overflow::UnsignedProduct);

BINARY_OP_FN(Divide, DoubleVal, /);

BINARY_OP_CHECK_ZERO_INT_TYPES(Int_divide, /);
BINARY_OP_CHECK_ZERO_INT_TYPES(Mod, %);
BINARY_OP_INT_TYPES(Bitand, &);
BINARY_OP_INT_TYPES(Bitxor, ^);
BINARY_OP_INT_TYPES(Bitor, |);

BITNOT_FN(TinyIntVal);
BITNOT_FN(SmallIntVal);
BITNOT_FN(IntVal);
BITNOT_FN(BigIntVal);

static const int64_t FACTORIAL_MAX = 20;
static const int64_t FACTORIAL_LOOKUP[] = {
    1LL, // 0!
    1LL, // 1!
    2LL, // 2!
    6LL, // 3!
    24LL, // 4!
    120LL, // 5!
    720LL, // 6!
    5040LL, // 7!
    40320LL, // 8!
    362880LL, // 9!
    3628800LL, // 10!
    39916800LL, // 11!
    479001600LL, // 12!
    6227020800LL, // 13!
    87178291200LL, // 14!
    1307674368000LL, // 15!
    20922789888000LL, // 16!
    355687428096000LL, // 17!
    6402373705728000LL, // 18!
    121645100408832000LL, // 19!
    2432902008176640000LL, // 20!
};

// Compute factorial - return -1 if out of range
// Factorial of any number <= 1 returns 1
static int64_t ComputeFactorial(int64_t n) {
  // Check range based on arg: 20! < 2^63 -1 < 21!
  if (n > FACTORIAL_MAX) {
    return -1;
  } else if (n < 0) {
    return 1;
  }

  return FACTORIAL_LOOKUP[n];
}

FACTORIAL_FN(TinyIntVal);
FACTORIAL_FN(SmallIntVal);
FACTORIAL_FN(IntVal);
FACTORIAL_FN(BigIntVal);

BINARY_PREDICATE_ALL_TYPES(Eq, ==);
BINARY_PREDICATE_ALL_TYPES(Ne, !=);
BINARY_PREDICATE_ALL_TYPES(Gt, >);
BINARY_PREDICATE_ALL_TYPES(Lt, <);
BINARY_PREDICATE_ALL_TYPES(Ge, >=);
BINARY_PREDICATE_ALL_TYPES(Le, <=);

NULLSAFE_DISTINCTION(DistinctFrom, !=, false);
NULLSAFE_DISTINCTION(NotDistinct, ==, true);

} // namespace impala
