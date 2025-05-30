#!/bin/bash
##===----------------------------------------------------------------------===##
##
## This source file is part of the Hummingbird server framework project
##
## Copyright (c) 2021-2025 the Hummingbird authors
## Licensed under Apache License v2.0
##
## See LICENSE.txt for license information
## See hummingbird/CONTRIBUTORS.txt for the list of Hummingbird authors
##
## SPDX-License-Identifier: Apache-2.0
##
##===----------------------------------------------------------------------===##
##===----------------------------------------------------------------------===##
##
## This source file is part of the SwiftNIO open source project
##
## Copyright (c) 2017-2019 Apple Inc. and the SwiftNIO project authors
## Licensed under Apache License v2.0
##
## See LICENSE.txt for license information
## See CONTRIBUTORS.txt for the list of SwiftNIO project authors
##
## SPDX-License-Identifier: Apache-2.0
##
##===----------------------------------------------------------------------===##

set -eu
here="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

printf "=> Checking format... "
FIRST_OUT="$(git status --porcelain)"
git ls-files -z '*.swift' | xargs -0 swift format format --parallel --in-place
git diff --exit-code '*.swift'

SECOND_OUT="$(git status --porcelain)"
if [[ "$FIRST_OUT" != "$SECOND_OUT" ]]; then
  printf "\033[0;31mformatting issues!\033[0m\n"
  git --no-pager diff
  exit 1
else
  printf "\033[0;32mokay.\033[0m\n"
fi
function replace_acceptable_years() {
    # this needs to replace all acceptable forms with 'YEARS'
    sed -e 's/20[12][0-9]-20[12][0-9]/YEARS/' -e 's/20[12][0-9]/YEARS/' -e '/^#!/ d'
}

printf "=> Checking license headers... "
tmp=$(mktemp /tmp/.hb_validate_XXXXXX)

for language in swift-or-c; do
  declare -a matching_files
  declare -a exceptions
  expections=( )
  matching_files=( -name '*' )
  case "$language" in
      swift-or-c)
        exceptions=( -path '*/Benchmarks/.build/*' -o -name Package.swift)
        matching_files=( -name '*.swift' -o -name '*.c' -o -name '*.h' )
        cat > "$tmp" <<"EOF"
//===----------------------------------------------------------------------===//
//
// This source file is part of the swift-jobs-sqs project
//
// Copyright (c) YEARS the swift-jobs-sqs authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//
EOF
        ;;
      *)
        echo >&2 "ERROR: unknown language '$language'"
        ;;
  esac

  lines_to_compare=$(cat "$tmp" | wc -l | tr -d " ")
  # need to read one more line as we remove the '#!' line
  lines_to_read=$(expr "$lines_to_compare" + 1)
  expected_sha=$(cat "$tmp" | shasum)

  (
    cd "$here/.."
    find . \
      \( \! -path './.build/*' -a \
      \( "${matching_files[@]}" \) -a \
      \( \! \( "${exceptions[@]}" \) \) \) | while read line; do
      if [[ "$(cat "$line" | head -n $lines_to_read | replace_acceptable_years | head -n $lines_to_compare | shasum)" != "$expected_sha" ]]; then
        printf "\033[0;31mmissing headers in file '$line'!\033[0m\n"
        diff -u <(cat "$line" | head -n $lines_to_read | replace_acceptable_years | head -n $lines_to_compare) "$tmp"
        exit 1
      fi
    done
    printf "\033[0;32mokay.\033[0m\n"
  )
done

rm "$tmp"
