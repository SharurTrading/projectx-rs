#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 Kevin Monaghan
# SPDX-License-Identifier: MIT

set -euo pipefail

if cargo tree --locked --edges normal,features --invert tokio-tungstenite \
  | grep --extended-regexp 'tokio-tungstenite feature "(connect|handshake|native-tls|__rustls-tls|rustls-tls)'; then
  printf '%s\n' \
    'secret-unsafe tokio-tungstenite client-handshake features are enabled in the normal graph' >&2
  exit 1
fi

if cargo tree --locked --edges normal,features --invert tungstenite \
  | grep --extended-regexp 'tungstenite feature "(handshake|native-tls|__rustls-tls|rustls-tls)'; then
  printf '%s\n' \
    'secret-unsafe tungstenite handshake features are enabled in the normal graph' >&2
  exit 1
fi
