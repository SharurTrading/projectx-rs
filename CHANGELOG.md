<!--
SPDX-FileCopyrightText: 2026 Kevin Monaghan
SPDX-License-Identifier: MIT-0
-->

# Changelog

All notable changes will be documented here. This project follows Semantic Versioning after its
first public release.

## [Unreleased]

### Breaking changes

- Resolve the Rust idiom audit in [#104](https://github.com/SharurTrading/projectx-rs/issues/104).
  `SignalRInvocation::payload()` now returns exact `serde_json::value::RawValue`;
  `entity()` returns `Result<&RawValue, RealtimeError>` so malformed data envelopes remain
  explicit failures. Use `.get()` for JSON text or `decode` / `decode_batch` for exact provider
  DTOs. The invocation retains one raw payload and no floating-point JSON tree.
- `RealtimeClient::invoke` and `RealtimeSession::invoke` borrow `&[serde_json::Value]`.
  Pass `&[]`, `&[value]`, or an existing vector's slice; pre-send refusal leaves arguments
  available to the caller.
- `Error::Configuration` carries `ConfigurationError` instead of a string. Configuration
  causes and credential field identities are machine-readable and contain no supplied values.
  `Error::ValidationAdmission` distinguishes closed or busy validator admission from a lost
  authenticated session. These API changes require a major release.


### Fixed

- Make receiver closure win over a pending transport gap, prevent relatching after receiver
  loss, and refuse reconnect when the single receiver is gone. Spawn socket tasks only after
  installation is admitted; preserve socket join failures in disconnect results. Remove unused
  liveness writes, unreachable start-event retention, and estimated event-weight accounting.
- Keep validator task ownership through shutdown cancellation; retain bearer tokens in
  zeroizing secret storage through decoding, snapshots, stale authentication, and rotation.
- Generate provider code conversions from one mapping per enum and serialize response
  envelope maps through Serde while preserving raw decimal tokens. Link README examples to
  compiled sources and correct example and fixture error handling. All audit resolutions are
  tracked by [#104](https://github.com/SharurTrading/projectx-rs/issues/104).

- Name the two exit reads where the crate's documentation still stated the general rule: the
  crate docs, the `Client` docs, and `ClientBuilder::max_retries` now say
  `search_open_positions_for_exit` and `query_orders_for_exit` refuse locally without sending
  and are never retried, matching the README and the behaviour guide, whose reserve-sizing tip
  now also counts the exit reads. Recovered unmerged follow-up to
  [#95](https://github.com/SharurTrading/projectx-rs/pull/95). Documentation only.

## [6.2.0] - 2026-10-16

### Added

- Reserve shared request capacity for exits, with runtime control, through
  [#84](https://github.com/SharurTrading/projectx-rs/pull/84), reported by the consuming platform
  in [SharurMinus #378](https://github.com/SharurTrading/SharurMinus/issues/378).
  `RateLimitConfig` gains an exit reserve (`with_exit_reserve` / `exit_reserve`), and
  `Client::set_exit_reserve` replaces it at runtime through the state every clone shares,
  including releasing it with zero, which remains the default. Cancellations, full closes and
  partial closes may draw on the whole general budget including the reserve, while ordinary
  requests are admitted only below it, so an exit keeps admission under full ordinary read
  pressure. A replacement preserves consumed quota, admitted attempts and provider cooldowns,
  waiting reads recheck policy changes without losing wakeups, and an invalid replacement is
  `Error::Configuration` and changes nothing; mutations still refuse immediately without sending
  and are never automatically retried. Adds a credential-free `exit_reserve` example, and
  advances the locked `quinn-proto` to 0.11.18 to satisfy the advisory gate.
- `Client::search_open_positions_for_exit` and `Client::query_orders_for_exit`: the open-position
  search and the order query admitted from the exit reserve, as cancels and closes are, for an
  exit that must read what to cancel and close. Each is sent at most once and never waits: an
  exhausted budget or a provider cooldown is `Error::LocallyRateLimited` without sending, and an
  HTTP 429 is `Error::ProviderRateLimited`, since a read is never ambiguous
  ([#93](https://github.com/SharurTrading/projectx-rs/pull/93)). Follow-up
  [#95](https://github.com/SharurTrading/projectx-rs/pull/95) corrects their documentation —
  each order-query page is its own admitted exit request, the reserve needs one request for the
  position search plus one per order page for each account an exit must read, and the general
  statements that reads wait, queue or retry exclude the two exit reads — and pins the cooldown
  and 429 refusals in tests.

### Changed

- Add the repository's step-by-step releasing guide (`RELEASING.md`) and reference it from the
  release gate in the repository guide through
  [#86](https://github.com/SharurTrading/projectx-rs/pull/86) and
  [#87](https://github.com/SharurTrading/projectx-rs/pull/87).
- Add `docs/behaviour-guide.md`, a venue behaviour guide for wrapping the client — connection,
  data and execution behaviour with every fact labelled by its source — linked at the top of the
  README through [#90](https://github.com/SharurTrading/projectx-rs/pull/90) and
  [#91](https://github.com/SharurTrading/projectx-rs/pull/91); the exit-read admission rules
  extend it through [#93](https://github.com/SharurTrading/projectx-rs/pull/93).
- Summarize every crate-root re-export in `src/lib.rs` and pin the public API's type shapes in a
  dedicated test, changing no signature, under
  [#88](https://github.com/SharurTrading/projectx-rs/issues/88)
  ([#89](https://github.com/SharurTrading/projectx-rs/pull/89)).
- Move the kache local-build-cache instructions out of the README and into the contributing guide
  through [#92](https://github.com/SharurTrading/projectx-rs/pull/92).

### Security

- Update the locked `rustls` resolution from 0.23.45 to 0.23.46 for RUSTSEC-2026-0346,
  RUSTSEC-2026-0347, RUSTSEC-2026-0348 and RUSTSEC-2026-0349 (published 2026-10-14): the locked
  0.23.45 is affected and fails the repository's dependency and advisory gates. Lockfile only;
  no source change.

## [6.1.1] - 2026-10-12

### Fixed

- Accept signed bracket tick offsets under
  [#81](https://github.com/SharurTrading/projectx-rs/issues/81), reported by the consuming
  platform in [SharurMinus #337](https://github.com/SharurTrading/SharurMinus/issues/337).
  `Bracket::new` rejected every non-positive value with
  `RequestValidationError::NonPositiveBracketTicks`, which made a native long stop-loss bracket
  unconstructable: the provider requires stop-loss ticks below zero for a long entry and refuses
  the positive form with code `2`, "Ticks should be less than zero when longing." The
  constructor now accepts any nonzero signed `i32` offset and preserves the exact value through
  accessors and serialization, including integer boundaries; only zero offsets and bracket order
  types undocumented for requests remain rejected. The sign is the caller's choice for the entry
  side and the protective leg — this crate neither infers nor rewrites it, and placement
  ambiguity classification and the prohibition on automatic mutation retries are unchanged.

### Changed

- Dependency and CI updates: `jiff` 0.2.38 and `tokio` 1.53.2
  ([#80](https://github.com/SharurTrading/projectx-rs/pull/80)), plus pinned CI action bumps for
  `dtolnay/rust-toolchain` ([#79](https://github.com/SharurTrading/projectx-rs/pull/79)) and
  `taiki-e/install-action` ([#78](https://github.com/SharurTrading/projectx-rs/pull/78)).
  Lockfile and workflow pins only; no source change.

## [6.1.0] - 2026-10-10

### Added

- Carry the order ID and the provider's message on rejections under
  [#74](https://github.com/SharurTrading/projectx-rs/issues/74), reported by the consuming
  platform in [SharurMinus #65](https://github.com/SharurTrading/SharurMinus/issues/65).
  `ProviderError` gains the public field `order_id: Option<OrderId>`, which
  `Client::place_order` fills with the `orderId` the provider returns beside a definitive
  rejection, and the accessor `ProviderError::untrusted_message() -> Option<&str>`, which
  returns the provider's free-form `errorMessage` for every `Error::Provider` rejection, or
  `None` when the provider sent `null` or omitted it. Both were previously discarded during
  decoding, so a code `2` placement rejection could not say why it was refused or which
  rejected record the provider created.

### Changed

- Reverse the policy that the provider's `errorMessage` is never exposed. The text remains
  untrusted remote input, passed through unaltered and bounded only by the configured
  response-size limit; callers clean it before displaying or logging it. It never appears in
  an error's `Display` output, `ProviderError`'s `Debug` output shows only
  `Some("[REDACTED]")` in its place, and the client never logs it. Ambiguous outcomes,
  including placement codes `6` and `7`, are classified exactly as before and carry no
  message. A rejection whose `errorMessage` is not text, or a placement rejection whose
  `orderId` is not a valid order ID, is now a decode failure, which a mutation reports as
  `Error::AmbiguousMutation`.

## [6.0.0] - 2026-09-30

### Breaking changes

- Preserve list-field presence in successful list reads under
  [#61](https://github.com/SharurTrading/projectx-rs/issues/61). Every list
  read — `search_active_accounts`, `search_accounts`, `available_contracts`,
  `search_contracts`, `retrieve_bars`, `search_orders`, `search_open_orders`,
  `search_trades`, `query_trades`, and `search_open_positions` — now returns
  `ProviderList`, whose `Listed` variant carries an explicit provider array,
  including an authoritative empty one, while `Absent` reports a success
  envelope that omitted the field or serialized it as JSON `null`.
  `query_orders` keeps returning `OrderPage`, whose `orders` field is now a
  `ProviderList<Order>`. The previous decoding
  collapsed omitted, `null`, and `[]` into the same empty vector, losing the
  distinction a consumer needs before flattening from a read
  ([SharurPlatform #964](https://github.com/SharurTrading/SharurPlatform/issues/964)).
  Rejection, inconsistent-status, and malformed-payload handling are unchanged.
- Report a missing or malformed `type` discriminator from
  `SignalRInvocation::from_value`
  under [#65](https://github.com/SharurTrading/projectx-rs/issues/65). The
  recognizer now borrows the value (`&Value`) instead of consuming it, returns
  `Ok(None)` for a frame whose `type` is any other unsigned integer — including
  message types this crate does not know — and returns
  `RealtimeError::Protocol` when `type` is missing or not an unsigned integer,
  the same strictness the transport's record recognizer applies, where both
  shapes mark a transport gap.
- Carry the failure origin on `Error::AmbiguousMutation` under
  [#66](https://github.com/SharurTrading/projectx-rs/issues/66). When no
  provider rejection was decoded, the new `origin` field reports the
  transport-level evidence through the new `AmbiguityOrigin` enum — transport
  failure with timeout and connection failures distinguished, an unclassified
  HTTP status, a decode failure, a success body missing a required result
  field, the response-size limit, inconsistent status fields, a 429 refusal,
  or an explicit unclassified fallback — and the error's display text renders
  it, so an operator can tell what kind of ambiguity occurred. Exactly one of
  `code` and `origin` is set in every error this crate constructs: a decoded
  pending or unknown rejection remains the evidence itself. Constructing
  `Error::AmbiguousMutation` now requires the additional field. A URL
  construction failure before any network request now passes through
  unmodified instead of being reported as an ambiguous outcome.

### Fixed

- Clamp provider cooldowns inside the rate limiter under
  [#65](https://github.com/SharurTrading/projectx-rs/issues/65). A
  `Retry-After`-derived duration beyond the limiter's 24-hour ceiling is
  clamped to it, and a duration the monotonic clock cannot represent still
  blocks admission until the latest representable instant, so no
  caller-supplied duration can silently skip a cooldown.

### Changed

- Add the `LAW-INVARIANT` review law to the repository guide through
  [#60](https://github.com/SharurTrading/projectx-rs/pull/60), prohibiting fabricated fallback
  values and silently swallowed invariant failures. The production audit under
  [#59](https://github.com/SharurTrading/projectx-rs/issues/59) recorded every candidate's
  disposition and confirmed no violations, so no library code changed.

## [5.0.1] - 2026-09-28

### Fixed

- Preserve exactly representable financial JSON values and reject precision or range loss under
  [#52](https://github.com/SharurTrading/projectx-rs/issues/52). Raw and quoted decimals, including
  scientific notation, no longer silently round. Reducible trailing zeros are handled before
  representability checks, so exact values such as `100e-30` remain usable. Optional null and
  omitted fields keep their existing behavior; no currency rounding policy is introduced.
- Make the paused-time ping/pong real-time fixture deterministic under the parallel test suite
  through [#53](https://github.com/SharurTrading/projectx-rs/issues/53) and
  [#50](https://github.com/SharurTrading/projectx-rs/issues/50). The fixture now answers the
  client's socket probe explicitly, closes each mock-time window with an application-level witness
  that proves the probe was acknowledged before the next advance, and keeps the live probe
  deadline out of tokio's paused-clock auto-advance window; a new test pins that an unanswered
  socket probe ends the generation.

### Changed

- Update the `rand` lockfile resolution from 0.10.2 to 0.10.3 and the `thiserror` lockfile
  resolution from 2.0.20 to 2.0.21 through
  [#56](https://github.com/SharurTrading/projectx-rs/pull/56).
- Refresh all `taiki-e/install-action` CI pins from v2.87.14 to v2.87.20 through
  [#55](https://github.com/SharurTrading/projectx-rs/pull/55).
- Remove the unresolved `PROC-DECIDE` reference from the repository guide's difficulty ladder
  through [#44](https://github.com/SharurTrading/projectx-rs/issues/44). The operator directed
  removing the reference rather than defining a rule for it.

## [5.0.0] - 2026-09-25

### Breaking changes

- Remove `ClientBuilder::realtime_event_capacity` and `RealtimeError::EventQueueFull` under
  [#45](https://github.com/SharurTrading/projectx-rs/issues/45). Accepted real-time events now
  remain in source order through local consumer backlog without a capacity-created `TransportGap`.
  `RealtimeEventReceiver` exposes queued-event count and oldest-event age. Genuine malformed-data
  gaps retain their recovery fence; outbound control admission remains bounded.

### Changed

- Update the `jiff` lockfile resolution from 0.2.35 to 0.2.37 through
  [#41](https://github.com/SharurTrading/projectx-rs/pull/41).
- Refresh all `taiki-e/install-action` CI pins from v2.87.10 to v2.87.14 through
  [#39](https://github.com/SharurTrading/projectx-rs/pull/39).
- Refresh all `dtolnay/rust-toolchain` CI pins to the current `master` revision through
  [#40](https://github.com/SharurTrading/projectx-rs/pull/40).

## [4.0.0] - 2026-09-17

### Breaking changes

- Carry the provider's published code text on every error that reports a provider code:
  `Error::AmbiguousMutation` gains `code` and `name`, and `Error::CredentialsRejected` and
  `Error::SessionValidationRejected` gain `name`. An ambiguous money-moving outcome therefore keeps
  the provider's rejection code instead of only the operation name.

### Added

- Report the provider's published error-code text on REST rejections. `ProviderError` gains `name`,
  `Error::AmbiguousMutation` gains `code` and `name`, and the credential and session-validation
  rejection variants carry it as well. Code text is per endpoint: code `2` is `OrderRejected` from
  `/api/Order/place` and `OrderNotFound` from `/api/Order/cancel`. Undocumented and future codes
  stay `None`, and the provider's free-form `errorMessage` is still discarded as untrusted remote
  text.
- Document the position-close rejection semantics the provider publishes. `Client::close_contract`
  and `Client::partial_close_contract` report `OrderRejected`, `InvalidCloseSize`, and
  `AccountRejected` for their own endpoints, and a partial close rejected with code `6` records a
  rejected closing order without saying whether the symbol was untradable or the contract had no
  price.

### Security

- Update the locked `rustls` resolution from 0.23.43 to 0.23.45 for
  [RUSTSEC-2026-0285](https://rustsec.org/advisories/RUSTSEC-2026-0285), together with the
  `aws-lc-rs`, `aws-lc-sys`, and `rustls-webpki` resolutions that release requires.

## [3.0.1] - 2026-09-14

### Fixed

- Reject trailing-stop placements that omit `trailPrice` with
  `RequestValidationError::MissingTrailPrice` before submission.
- Correct trailing-stop documentation: placement and modification accept absolute price levels,
  while order searches return a trail distance in price units. Document the provider's placement
  distance limit, modification behavior, bracket-mode requirement, and cancellation eligibility.

### Changed

- Update the `reqwest` lockfile resolution from 0.13.4 to 0.13.5, including its new `base64` 0.23.1
  dependency, through [#33](https://github.com/SharurTrading/projectx-rs/pull/33).
- Refresh all `taiki-e/install-action` CI pins from v2.87.4 to v2.87.10 through
  [#32](https://github.com/SharurTrading/projectx-rs/pull/32).

## [3.0.0] - 2026-09-09

### Breaking changes

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

### Changed

- Update the `rust_decimal` lockfile resolution from 1.42.1 to 1.43.0, picking up upstream fixes
  and performance improvements, and refresh the `taiki-e/install-action` CI pin from v2.87.0 to
  v2.87.4.

### Security

- Remove the RUSTSEC-2026-0235 audit exception and its inactive-feature CI guard after
  `rust_decimal` 1.43.0 removes the vulnerable rkyv 0.7 dependency path from the lockfile;
  `cargo audit --deny warnings` now runs without advisory exceptions.

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

[6.2.0]: https://github.com/SharurTrading/projectx-rs/compare/v6.1.1...v6.2.0
[6.1.1]: https://github.com/SharurTrading/projectx-rs/compare/v6.1.0...v6.1.1
[6.1.0]: https://github.com/SharurTrading/projectx-rs/compare/v6.0.0...v6.1.0
[6.0.0]: https://github.com/SharurTrading/projectx-rs/compare/v5.0.1...v6.0.0
[5.0.1]: https://github.com/SharurTrading/projectx-rs/compare/v5.0.0...v5.0.1
[5.0.0]: https://github.com/SharurTrading/projectx-rs/compare/v4.0.0...v5.0.0
[4.0.0]: https://github.com/SharurTrading/projectx-rs/compare/v3.0.1...v4.0.0
[3.0.1]: https://github.com/SharurTrading/projectx-rs/compare/v3.0.0...v3.0.1
[3.0.0]: https://github.com/SharurTrading/projectx-rs/compare/v2.1.0...v3.0.0
[2.1.0]: https://github.com/SharurTrading/projectx-rs/compare/v2.0.0...v2.1.0
[2.0.0]: https://github.com/SharurTrading/projectx-rs/compare/v1.0.1...v2.0.0
[1.0.1]: https://github.com/SharurTrading/projectx-rs/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/SharurTrading/projectx-rs/releases/tag/v1.0.0
