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
  or exposed through public token accessors. Debug output is redacted.
- **PX-AUTH-01:** Login sends only `userName` and `apiKey` to `/api/Auth/loginKey`. The HTTP session
  is the sole token writer. Token reads take owned snapshots and never hold a lock across network I/O.
- **PX-ACCOUNT-01:** Active-account discovery sends exactly `onlyActiveAccounts: true` to
  `/api/Account/search`.
- **PX-RUNTIME-01:** The caller owns the async runtime. The library must not create a hidden Tokio
  runtime or block an async executor.
- **PX-TRANSPORT-01:** Responses are size-bounded. Real-time queues are bounded with explicit
  overflow behavior. A websocket is not ready until the SignalR handshake is validated.
- **PX-ORDER-01:** Order placement is not automatically retried because a timeout after submission
  is an ambiguous money-moving outcome. Retry policies must distinguish safe queries from mutations.
- **PX-VALIDATE-01:** Normal CI is deterministic and credential-free. Live tests are ignored,
  read-only, and deliberately invoked.

## Rust API standards

- Public APIs are documented and use typed `thiserror` errors.
- Provider IDs are validated newtypes with private fields and conversions.
- Public enums that may grow are `#[non_exhaustive]`.
- Configuration uses a builder when optional settings exceed two fields.
- Functions borrow inputs unless they must retain or transfer ownership.
- Never use `unwrap`, `expect`, `panic!`, or `unsafe` in production paths.
- Do not hold a synchronization guard across `.await`.
- Organize source by capability; keep `lib.rs` to documentation and selective re-exports.
- Tests in `tests/` exercise the public API. Unit tests cover private parsing and invariants.

## Review format

Report correctness and design findings as `[BLOCKER]`, `[MAJOR]`, or `[MINOR]`, cite the rule ID,
file, line, risk, and concrete fix. A blocker or major finding requires changes before merge.

## Public-release gate

Before changing `publish = false`, making the repository public, or changing the MIT license:

1. Confirm provider terms permit the intended source distribution and branding.
2. Complete a secret and proprietary-content scan, including Git history.
3. Confirm all fixtures are synthetic and all documentation is public-safe.
4. Run formatting, Clippy, tests, rustdoc, dependency, and license checks.
5. Review the public API and commit to a compatibility policy.
