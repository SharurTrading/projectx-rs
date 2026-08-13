#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 Kevin Monaghan
# SPDX-License-Identifier: MIT-0

set -euo pipefail

# RUSTSEC-2026-0235 is ignored in .cargo/audit.toml because the vulnerable
# rkyv 0.7 series is reachable only through rust_decimal's optional `rkyv`
# feature, which projectx-client never enables. The ignore is sound only
# while that feature path stays inactive: downstream feature unification
# could activate rust_decimal/rkyv, in which case the ignore must be removed
# and the path upgraded instead. This guard fails the build when rkyv 0.7.x
# appears in the resolved feature graph across all targets.

if cargo tree --locked --all-targets -e features | grep -Eq 'rkyv v0\.7\.'; then
  echo "::error::rkyv 0.7.x is active in the feature graph; remove the RUSTSEC-2026-0235 audit ignore and upgrade the path instead" >&2
  exit 1
fi
