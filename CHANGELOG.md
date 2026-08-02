<!--
SPDX-FileCopyrightText: 2026 Kevin Monaghan
SPDX-License-Identifier: MIT
-->

# Changelog

All notable changes will be documented here. This project follows Semantic Versioning after its
first public release.

## [Unreleased]

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

[Unreleased]: https://github.com/SharurTrading/projectx-rs/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/SharurTrading/projectx-rs/releases/tag/v1.0.0
