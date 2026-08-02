<!--
SPDX-FileCopyrightText: 2026 Kevin Monaghan
SPDX-License-Identifier: MIT
-->

# Security

Please use [GitHub private vulnerability reporting](https://github.com/SharurTrading/projectx-rs/security/advisories/new)
for suspected vulnerabilities. Do not open a public issue containing credentials, bearer tokens,
account identifiers, order identifiers, or captured provider payloads.

The library does not read `.env` files, persist secrets, or expose bearer tokens. Callers own secret
acquisition and storage. Logs and bug reports must use synthetic or redacted data.

The HTTP clients ignore ambient process proxy variables. Configure a proxy only through
`ClientBuilder::proxy`; plaintext loopback fixture endpoints cannot be combined with one.

The provider's SignalR handshake places the bearer token in the WebSocket URL as an
`access_token` query parameter. This crate performs that HTTP/1 upgrade through a dedicated client,
validates the RFC 6455 response, and gives tungstenite only the already-upgraded stream; the
token-bearing request therefore never reaches tungstenite's request logger. Transport errors remain
opaque. Applications must still protect proxy/server access logs and packet captures that can
observe the documented query parameter.

The ignored live probe reads credentials only from the process environment and is read-only. Run it
deliberately; never commit its environment, terminal transcript, or captured provider responses.
