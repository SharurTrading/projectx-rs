<!--
SPDX-FileCopyrightText: 2026 Kevin Monaghan
SPDX-License-Identifier: MIT-0
-->

# Changelog

All notable changes will be documented here. This project follows Semantic Versioning after its
first public release.

## [Unreleased]

### Breaking changes for 3.0.0

- Make event, writer and pending-invocation capacities and invocation deadlines configurable on
  `ClientBuilder`. Remove the estimated event-memory budget and unsupported WebSocket frame,
  message and outbound invocation ceilings (`OutboundMessageTooLarge` is removed). Raise the
  default burst buffers to 65,536 events and 4,096 outstanding control messages/invocations;
  completed subscriptions have no count limit. Yield while decoding coalesced records so a
  ready consumer can drain a batch larger than its queue on a current-thread runtime.

- Preserve healthy real-time sockets across event overflow, malformed SignalR records, invocation
  cancellation, completion timeout and ordinary inactivity. Gaps are nonterminal and consumers
  acknowledge them while connected instead of waiting for a disconnect/reconnect cycle.
- Add `RealtimeGeneration`, `RealtimeMessage`, `recv_message()` and generation-scoped
  `RealtimeSession` admission. A stale session refuses before enqueueing; unknown calls are never
  automatically resent.
- Join both socket producers before publishing their disconnected boundary. Retain ordered
  lifecycle notifications when the data queue is full and keep completions/keepalives flowing
  while data admission awaits gap acknowledgement.
- Preserve the caller’s Connect instruction after remote close. Active WebSocket probes detect
  a failed ping/pong exchange; ordinary market-data silence never expires a connection.

## [2.1.0] - 2026-09-06

### Added

- Add an `Endpoints::thefuturesdesk()` preset selecting the hosted TheFuturesDesk Gateway deployment
  (`https://api.thefuturesdesk.projectx.com` and its `rtc.thefuturesdesk.projectx.com` hubs) to
  complement the TopstepX default. Both API-key and authorized-application credentials work against
  either hosted preset.

### Changed

- Relicense the project under the MIT No Attribution License (MIT-0): the attribution requirement
  is dropped while the existing permissive terms otherwise remain unchanged.
- Update the `data-encoding` dependency from 2.11.0 to 2.11.1, `thiserror` from 2.0.19 to 2.0.20,
  `futures-util` from 0.3.33 to 0.3.34, and `log` from 0.4.33 to 0.4.34.
- Refresh CI action pins (`dtolnay/rust-toolchain`, `Swatinem/rust-cache`, `taiki-e/install-action`)
  to their current patch releases; `taiki-e/install-action` is now pinned to v2.87.0.
- Update the transitive `h2` lockfile entry from 0.4.15 to 0.4.19 and `chacha20` from 0.10.1 to
  0.10.2, clearing a `cargo deny` yanked-crate error on the withdrawn `chacha20` 0.10.1 release
  reached through `rand` 0.10.2.

### Security

- Resolve RustSec advisory RUSTSEC-2026-0258 (`h2` unbounded empty DATA frames) by moving the
  transitive `h2` dependency to 0.4.19, which is past the patched 0.4.16 release. The crate reaches
  `h2` through `reqwest`/`hyper`, so no source change is required.
- Document RustSec advisory RUSTSEC-2026-0235 (rkyv 0.7.x) as not affecting this crate: the
  vulnerable dependency is only an optional, unactivated feature of `rust_decimal` and is never
  compiled. CI records the justified `cargo audit` ignore and enforces a guard that fails the
  build if the rkyv 0.7 feature path ever becomes active; the ignore is removed once
  `rust_decimal` moves that feature to rkyv 0.8.17 or later.

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

[Unreleased]: https://github.com/SharurTrading/projectx-rs/compare/v2.1.0...HEAD
[2.1.0]: https://github.com/SharurTrading/projectx-rs/compare/v2.0.0...v2.1.0
[2.0.0]: https://github.com/SharurTrading/projectx-rs/compare/v1.0.1...v2.0.0
[1.0.1]: https://github.com/SharurTrading/projectx-rs/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/SharurTrading/projectx-rs/releases/tag/v1.0.0
