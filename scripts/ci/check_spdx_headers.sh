#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 Kevin Monaghan
# SPDX-License-Identifier: MIT-0

set -euo pipefail

status=0
while IFS= read -r -d '' file; do
  if [[ "${file}" == "Cargo.lock" ]]; then
    continue
  fi

  if ! head -n 8 -- "${file}" | grep --fixed-strings --quiet \
    "SPDX-FileCopyrightText: 2026 Kevin Monaghan"; then
    printf 'missing SPDX copyright header: %s\n' "${file}" >&2
    status=1
  fi
  if ! head -n 8 -- "${file}" | grep --fixed-strings --quiet \
    "SPDX-License-Identifier: MIT-0"; then
    printf 'missing MIT-0 SPDX license header: %s\n' "${file}" >&2
    status=1
  fi
done < <(git ls-files --cached --others --exclude-standard -z -- \
  '*.rs' '*.sh' '*.toml' '*.md' '*.yml' '*.yaml')

exit "${status}"
