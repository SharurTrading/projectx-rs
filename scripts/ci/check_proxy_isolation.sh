#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 Kevin Monaghan
# SPDX-License-Identifier: MIT

set -euo pipefail

# Build before poisoning proxy discovery so this check never depends on
# registry access. TCP port zero cannot host a listening proxy; a client that
# inherits these variables will fail before reaching the loopback fixtures.
cargo test --locked --test realtime_secret_logging --no-run

env -u NO_PROXY -u no_proxy \
  HTTP_PROXY=http://127.0.0.1:0 \
  http_proxy=http://127.0.0.1:0 \
  HTTPS_PROXY=http://127.0.0.1:0 \
  https_proxy=http://127.0.0.1:0 \
  ALL_PROXY=http://127.0.0.1:0 \
  all_proxy=http://127.0.0.1:0 \
  CARGO_NET_OFFLINE=true \
  cargo test --offline --locked --test realtime_secret_logging
