<!--
SPDX-FileCopyrightText: 2026 Kevin Monaghan
SPDX-License-Identifier: MIT
-->

# ProjectX Rust Client Guide

This guide is the repository contract for writing and reviewing `projectx-client`.

## Mission

Provide a small, provider-native Rust client for the ProjectX Gateway API. The crate must remain
independent of every consuming application. It owns transport, authentication, provider DTOs, and
real-time protocol handling; consumers own routing, portfolios, risk, GUI, storage, and canonical
domain translation.

## Non-negotiables

- **PX-BOUNDARY-01:** No dependency on a consuming trading platform, its crates, types, rules, or
  runtime. Generic Rust ecosystem dependencies are allowed.
- **PX-DECIMAL-01:** Prices, money, balances, fees, and P&L use `rust_decimal::Decimal` in the public
  API. Never expose floating-point values for financial fields.
- **PX-SECRET-01:** Credentials and bearer tokens are never logged, included in errors, persisted,
  or exposed through public token accessors. Debug output is redacted. Remote endpoints require
  authenticated encryption; plain HTTP/WebSocket transport is permitted only for exact loopback
  hosts used by deterministic fixtures. The token-bearing WebSocket HTTP upgrade must not pass
  through a dependency path that logs the request URI or raw headers. Ambient process proxy
  variables are ignored; proxying is an explicit client-builder decision.
- **PX-AUTH-01:** Login sends only `userName` and `apiKey` to `/api/Auth/loginKey`. The HTTP session
  is the sole token writer. Token reads take owned snapshots and never hold a lock across network I/O.
  Token snapshots carry a revision: reauthentication invalidates outstanding validation responses,
  and a rotated token is committed only when the revision it validated is still current. Credential
  replacement is fail-closed: readers cannot use the superseded session while an update is pending,
  successful reauthentication wins the race, and a failed or cancelled authentication restores only
  the first accepted deferred validation result. A validation attempt is guarded from immediately
  before network submission through response decoding: cancellation or any ambiguous post-admission
  outcome invalidates only its exact revision, and only a definitive pre-send failure or HTTP 429 may
  retain that basis. Dropping or shutting down the periodic validator synchronously closes request
  admission and invalidates any already-admitted revision before task cancellation returns control.
- **PX-RESPONSE-01:** Every structured REST response contract requires both `success` and
  `errorCode`, and acceptance requires exactly `success == true` and `errorCode == 0`. Missing or
  contradictory status fields are semantic failures; for mutations and session validation they are
  ambiguous fail-closed outcomes. The sole raw-body exception is the provider-documented,
  unauthenticated `/api/Status/ping`, whose size-bounded body must equal exactly `pong`.
- **PX-ACCOUNT-01:** Active-account discovery sends exactly `onlyActiveAccounts: true` to
  `/api/Account/search`.
- **PX-RUNTIME-01:** The caller owns the async runtime. The library must not create a hidden Tokio
  runtime or block an async executor.
- **PX-TRANSPORT-01:** Responses are size-bounded. Real-time queues are bounded with explicit
  overflow behavior. A websocket is not ready until the SignalR handshake is validated. Real-time
  lifecycle transitions are generation-fenced and single-writer; cancelling an invocation reclaims
  its pending slot, either socket half failing tears down the whole session, and dropping the last
  caller-owned handle cancels every library-owned task.
- **PX-ORDER-01:** Order placement is not automatically retried because a timeout after submission
  is an ambiguous money-moving outcome. Retry policies must distinguish safe queries from mutations.
  Documented pending/unknown outcomes and unrecognized future mutation codes are ambiguous; only an
  endpoint-specific whitelist of documented definitive rejections may become `ProviderError`.
  Dependency-level HTTP retries stay disabled so the client-owned query loop is the only retry
  authority and every outbound attempt receives rate-limit admission.
- **PX-RATE-01:** Authenticated REST attempts share strict rolling-window budgets across client
  clones: history is 50 requests per 30 seconds and all other endpoints are 200 per 60 seconds.
  Queries may wait asynchronously; mutations fail locally before sending when capacity is exhausted.
  Provider 429 cooldowns are shared and mutations are never retried automatically.
- **PX-VALIDATE-01:** Normal CI is deterministic and credential-free. Live tests are ignored,
  read-only, and deliberately invoked.

## Rust API standards

- Public APIs are documented and use typed `thiserror` errors.
- Provider IDs are validated newtypes with private fields and conversions.
- Public enums that may grow are `#[non_exhaustive]`.
- Response-only public structs are `#[non_exhaustive]`; configurable request structs use validated
  constructors or builders rather than permitting invalid intermediate states.
- Configuration uses a builder when optional settings exceed two fields.
- Functions borrow inputs unless they must retain or transfer ownership.
- Never use `unwrap`, `expect`, `panic!`, or `unsafe` in production paths.
- Do not hold a synchronization guard across `.await`.
- Every spawned task has explicit cancellation and tracked teardown; dropping a `JoinHandle` is not
  treated as cancellation.
- Organize source by capability; keep `lib.rs` to documentation and selective re-exports.
- Tests in `tests/` exercise the public API. Unit tests cover private parsing and invariants.

## Review format

Report correctness and design findings as `[BLOCKER]`, `[MAJOR]`, or `[MINOR]`, cite the rule ID,
file, line, risk, and concrete fix. A blocker or major finding requires changes before merge.

## Release gate

Every crates.io or GitHub release must originate from a reviewed pull request and the exact merged
commit on `main`. Direct pushes to protected branches, force-pushes, publishing from a dirty or
unmerged worktree, and moving or replacing a published version tag are prohibited.

Before merging a release pull request:

1. Confirm provider terms permit the intended source distribution and branding.
2. Complete a secret and proprietary-content scan, including Git history.
3. Confirm all fixtures are synthetic and all documentation is public-safe.
4. Review the public API against the compatibility policy and classify the Semantic Versioning
   impact.
5. Update the crate version, lockfile, changelog, and release-facing documentation in the same PR.
6. Run formatting, strict Clippy, tests, rustdoc, package verification, dependency, license, and
   secret checks; require the release PR and post-merge `main` CI runs to pass.

After the release PR merges and `main` CI passes, publish that exact commit to crates.io, create an
annotated `v<version>` tag pointing to it, push the tag without force, and create the matching GitHub
release. Verify the crate can be resolved and compiled from crates.io before considering the release
complete.
