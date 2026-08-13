<!--
SPDX-FileCopyrightText: 2026 Kevin Monaghan
SPDX-License-Identifier: MIT-0
-->

# Changelog

All notable changes will be documented here. This project follows Semantic Versioning after its
first public release.

## [Unreleased]

### Changed

- Relicense the project under the MIT No Attribution License (MIT-0): the attribution requirement
  is dropped while the existing permissive terms otherwise remain unchanged.
- Update the `data-encoding` dependency from 2.11.0 to 2.11.1.
- Refresh CI action pins (`dtolnay/rust-toolchain`, `Swatinem/rust-cache`, `taiki-e/install-action`)
  to their current patch releases.

### Security

- Document RustSec advisory RUSTSEC-2026-0235 (rkyv 0.7.x) as not affecting this crate: the
  vulnerable dependency is only an optional, unactivated feature of `rust_decimal` and is never
  compiled. CI records the justified `cargo audit` ignore and is revisited when `rust_decimal`
  moves that feature to rkyv 0.8.17 or later.

## [2.0.0] - 2026-08-03

### Added

- Cover every operation in the current ProjectX Gateway Swagger document, including authorized-app
  login, logout, status ping, general account search, order lookup by ID, and trade search with
  independently optional time bounds.
- Add a validated, fully redacted `ApplicationCredentials` builder and a validated `TradeQuery`
  builder for the new authorized-app and optional-range request contracts.
- Add a deterministic REST-operation manifest and an ignored public Swagger drift check.
- Add ignored, credentialed live probes that explicitly select a ProjectX data catalog, dynamically
  discover its active MNQ expiry, download recent history, and validate decoded quote and trade/tick
  streams.
- Add deterministic boundary tests proving the history and general limiters admit all 50 and 200
  allowed requests before throttling, including across cloned clients.

### Changed

- Represent `MarketQuote` prices, change values, session statistics, volume, and its separate event
  timestamp as optional because the live hub emits sparse quote updates that omit unchanged values;
  decoding accepts both missing and null optional fields. This is a source-breaking correction that
  avoids fabricating zero values.
- Fence logout against token revisions so cancelled or ambiguous attempts invalidate only the
  submitted session, while definitive pre-send failures and provider throttling retain it.

## [1.0.1] - 2026-08-02

### Fixed

- Preserve exact REST and SignalR decimal tokens without enabling global `rust_decimal` or
  `serde_json` arbitrary-precision features that can alter unrelated downstream serialization
  through Cargo feature unification.
- Emit type-1 real-time frames as exact `RealtimeEvent::Invocation` values; typed invocation
  decoding now reads the retained raw entity token.

## [1.0.0] - 2026-08-02

- License the project under the MIT License and add SPDX file headers.
- Establish the standalone ProjectX REST client boundary.
- Add exact decimal models, typed identifiers, redacted credentials, and deterministic fixtures.
- Cover the client's API-key authentication, discovery, history, order, position, and trade REST
  workflows, including contract-by-ID and partial position close.
- Add bounded SignalR market/user hubs with validated handshakes, invocation completion,
  reconnect notification, rotating-token snapshots, and explicit gap recovery.
- Preserve unknown provider enum codes and add an ignored read-only live probe.
- Enforce ProjectX rolling-window REST limits across cloned clients, honor `Retry-After`, and reject
  locally throttled mutations before network submission.
- Fence authentication and real-time connection generations so stale concurrent work cannot
  overwrite a newer token or session, and make background-task shutdown failures observable.
- Fail closed on cancelled, malformed, or otherwise ambiguous session validation while preserving
  newer concurrent authentication, and require consistent provider success/error-code pairs.
- Add validated builders for order mutations and typed provider enums for order types, order status,
  trade direction, and tick bars; preserve stop-limit response codes while rejecting the
  undocumented type from placement and bracket requests.
- Add the paginated `/api/Order/v2/query` surface with validated filters and typed sorting so
  reconciliation can include every non-terminal order state, including suspended bracket children.
- Harden endpoint validation, request cancellation cleanup, and real-time reader/writer teardown.
- Require HTTPS for remote custom endpoints, classify endpoint-specific pending/unknown mutation
  codes as ambiguous, and fail closed on unknown session-validation outcomes.
- Keep bearer-bearing WebSocket upgrades out of dependency request logs with a dedicated,
  RFC 6455-validated HTTP/1 upgrade path before tungstenite framing begins.
- Ignore inherited process proxy variables so plaintext loopback fixtures and token-bearing
  upgrades can use only an explicitly configured client proxy.
- Disable reqwest's protocol-level retry layer so mutation attempts remain single-shot and query
  retries cannot bypass the shared rate limiter.
- Raise the minimum supported Rust version to 1.95.0 and update the HTTP and WebSocket stacks to
  `reqwest` 0.13 and `tokio-tungstenite` 0.30.
- Enforce strict Rust CI gates for formatting, strict pedantic Clippy, rustdoc, nextest,
  doctests, package verification, locked dependency policy, and full-history secret scanning; pin
  every third-party GitHub Action to an immutable commit.

[Unreleased]: https://github.com/SharurTrading/projectx-rs/compare/v2.0.0...HEAD
[2.0.0]: https://github.com/SharurTrading/projectx-rs/compare/v1.0.1...v2.0.0
[1.0.1]: https://github.com/SharurTrading/projectx-rs/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/SharurTrading/projectx-rs/releases/tag/v1.0.0
