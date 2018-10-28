#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Do some error checking and generate junit symptoms after running a build.

set -euo pipefail
trap 'echo Error in $0 at line $LINENO: $(cd "'$PWD'" && awk "NR == $LINENO" $0)' ERR

if test -v CMAKE_BUILD_TYPE && [[ "${CMAKE_BUILD_TYPE}" =~ 'UBSAN' ]] \
    && [ "${UBSAN_FAIL}" = "error" ] \
    && { grep -rI ": runtime error: " "${IMPALA_HOME}/logs" 2>&1 | sort | uniq \
     | tee logs/ubsan.txt ; }
then
  "${IMPALA_HOME}"/bin/generate_junitxml.py --step UBSAN \
      --stderr "${IMPALA_HOME}"/logs/ubsan.txt --error "Undefined C++ behavior"
fi

rm -rf "${IMPALA_HOME}"/logs_system
mkdir -p "${IMPALA_HOME}"/logs_system
dmesg > "${IMPALA_HOME}"/logs_system/dmesg

# Check dmesg for OOMs and generate a symptom if present.
if [[ $(grep "Out of memory" "${IMPALA_HOME}"/logs_system/dmesg) ]]; then
  "${IMPALA_HOME}"/bin/generate_junitxml.py --phase finalize --step dmesg \
      --stdout "${IMPALA_HOME}"/logs_system/dmesg --error "Process was OOM killed."
fi

# Check for any minidumps and symbolize and dump them.
LOGS_DIR="${IMPALA_HOME}"/logs
if [[ $(find $LOGS_DIR -path "*minidumps*" -name "*dmp") ]]; then
  SYM_DIR=$(mktemp -d)
  dump_breakpad_symbols.py -b $IMPALA_HOME/be/build/latest -d $SYM_DIR
  for minidump in $(find $LOGS_DIR -path "*minidumps*" -name "*dmp"); do
    $IMPALA_TOOLCHAIN/breakpad-$IMPALA_BREAKPAD_VERSION/bin/minidump_stackwalk \
        ${minidump} $SYM_DIR > ${minidump}_dumped 2> ${minidump}_dumped.log
    "${IMPALA_HOME}"/bin/generate_junitxml.py --phase finalize --step minidumps \
        --error "Minidump generated: $minidump" \
        --stderr "$(head -n 100 ${minidump}_dumped)"
  done
  rm -rf $SYM_DIR
fi
