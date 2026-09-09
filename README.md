<!--
SPDX-FileCopyrightText: 2026 Kevin Monaghan
SPDX-License-Identifier: MIT-0
-->

# projectx-rs

[![CI](https://github.com/SharurTrading/projectx-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/SharurTrading/projectx-rs/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/projectx-client.svg?v=2.1.0)](https://crates.io/crates/projectx-client/2.1.0)
[![docs.rs](https://img.shields.io/docsrs/projectx-client/2.1.0?v=2.1.0)](https://docs.rs/projectx-client/2.1.0/projectx_client/)
[![license: MIT-0](https://img.shields.io/badge/license-MIT--0-blue.svg)](LICENSE)

An async, provider-native Rust client for the ProjectX Gateway API.

This project is available under the [MIT No Attribution License (MIT-0)](LICENSE). It is an
independent, unofficial client and is not affiliated with, endorsed by, or sponsored by ProjectX
Trading LLC. Users are responsible for complying with the provider's terms and maintaining an active
API subscription where required.

Use the official [ProjectX Gateway API documentation](https://gateway.docs.projectx.com/) as the
reference for provider endpoints, request fields, response payloads, and subscription requirements.
This README documents the additional safety and lifecycle behavior supplied by this client.

The minimum supported Rust version is 1.95.0.

Version 2 follows Semantic Versioning. Public API changes that require downstream source changes
will be released under a new major version; additive APIs and fixes use minor and patch releases.
Provider contract changes can still require callers to update operational behavior, so review the
changelog before upgrading and keep recovery around ambiguous money-moving outcomes.

## Installation

```sh
cargo add projectx-client@2
```

Or add the current major release directly:

```toml
[dependencies]
projectx-client = "3"
```

The complete public API is available on [docs.rs](https://docs.rs/projectx-client).

## Design boundaries

- No trading-platform or application dependencies.
- No environment-file loading or credential persistence.
- Exact `Decimal` values for price, money, and P&L, parsed from the provider's
  JSON digits without an `f64` round-trip.
- Typed provider identifiers rather than interchangeable strings and integers.
- A caller-owned Tokio runtime; the library never creates a hidden runtime.
- Typed errors, redacted credentials, bounded HTTP/WebSocket queues, and no
  automatic retry for money-moving mutations.
- Shared rolling-window REST limits with explicit local and provider-throttle errors.
- SignalR market and user hubs with handshake-gated readiness, invocation
  completions, token-aware reconnects, and explicit transport-gap recovery.
- Deterministic tests use synthetic local fixtures. Live tests are opt-in and read-only.

## Quick start

```rust
use projectx_client::{Client, Credentials};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), projectx_client::Error> {
    let credentials = Credentials::new("your-user-name", "your-api-key")?;
    let client = Client::builder(credentials).build()?;

    client.authenticate().await?;
    let accounts = client.search_active_accounts().await?;
    for account in accounts {
        println!("{}", account.name);
    }

    Ok(())
}
```

Applications should source secrets outside this library and must not log credentials or bearer
tokens. See [SECURITY.md](SECURITY.md).

By default a client targets the hosted TopstepX endpoints (`https://api.topstepx.com` with the
`rtc.topstepx.com` hubs). Select the hosted TheFuturesDesk deployment with
`ClientBuilder::endpoints(Endpoints::thefuturesdesk())`, or `Endpoints::custom` for any other
ProjectX Gateway deployment. Both credential types work against either hosted deployment.

## Feature coverage

The current client surface covers every REST operation in the provider's published Gateway
Swagger document:

- API-key and authorized-application login, logout, status ping, and rotating-token validation
- active-only and general account discovery
- contract availability, text search, and lookup by ID
- historical bars
- historical, open, by-ID, and paginated v2 order search, placement, cancellation, and modification
- open positions, full close, and partial close
- bounded and unbounded execution/trade search

It also implements both documented SignalR hubs, market/user subscription helpers,
bounded event delivery, reconnect notification, and exact provider payload models.

Requests with provider invariants are constructed through validated APIs. In particular,
`HistoryRequest::builder`, `OrderQuery::builder`, `TradeQuery::builder`, `PlaceOrder::builder`,
`ModifyOrder::builder`, `Bracket::new`, and `PartialCloseContract::new` reject invalid ranges,
counts, status codes, or empty mutations before any network request can be admitted.

`OrderType::StopLimit` is retained when decoding provider responses, but the current ProjectX order
placement and bracket references do not document type code `3` as a supported request value.
`PlaceOrder::builder` and `Bracket::new` therefore reject it instead of sending an undocumented
money-moving request.

Money-moving methods never retry automatically. Provider codes documented as pending or unknown,
as well as future codes this crate does not recognize, return `Error::AmbiguousMutation`; reconcile
provider state before retrying. Only endpoint-specific codes documented as definitive rejections
return `Error::Provider`.

### Complete working-order reconciliation

The provider's legacy `search_open_orders` endpoint excludes `Suspended` orders. That omission is
material for bracket orders because inactive stop-loss and take-profit children are suspended until
their parent activates them. Do not use `search_open_orders` alone to construct a complete
working-order mirror.

Use `query_orders` with an `OrderQuery` status filter containing every non-terminal lifecycle state:
`OrderStatus::Open`, `OrderStatus::Pending`, `OrderStatus::PendingCancellation`, and
`OrderStatus::Suspended`. Continue through every requested page before declaring reconciliation
complete; `include_total_count(true)` can request an explicit matching count. Treat the result as a
provider snapshot to translate at the consuming application's boundary.

### Session validation and token rotation

`authenticate()` uses the credential type supplied to the client and stores the bearer token
privately. `Client::builder(Credentials)` selects API-key login. Authorized applications can use
`Client::application_builder(ApplicationCredentials)` to send the provider's five-field
application-login contract. Applications that run for more than a short request cycle can instead
use `authenticate_with_validation(period)`:

```rust
use std::time::Duration;

use projectx_client::{Client, Credentials};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), projectx_client::Error> {
    let client = Client::builder(Credentials::new("user", "api-key")?).build()?;
    let validator = client
        .authenticate_with_validation(Duration::from_mins(15))
        .await?;

    // REST requests and real-time hubs created from `client` share the rotating token.

    validator.shutdown().await?;
    Ok(())
}
```

The validator periodically calls the provider's validation endpoint and atomically replaces the
stored token when the provider returns a new one. Token updates are revision-fenced: an older login
or validation request cannot overwrite credentials established by a newer authentication cycle. A
real-time connection snapshots the latest token immediately before every initial connection and
reconnect, so a reconnect does not reuse the token from the original WebSocket session. A token
that receives HTTP 401, a terminal validation code, or an unusable replacement token is immediately
unavailable to new REST and real-time work. If authentication is concurrently replacing that
session, reads fail closed until the race resolves, and the new authentication takes precedence.

The periodic validator stops when the current session is definitively lost and no replacement
authentication is in flight. `SessionValidator::is_finished()` exposes that state; call
`shutdown().await?` to wait for termination and receive the terminal validation error. If shutdown
cancels a request that may already have reached the provider, it returns
`Error::AmbiguousSessionValidation` and invalidates only that request's token revision; authenticate
again before further work. Dropping the guard cancels without waiting. If a validation request is
already admitted, drop synchronously makes that exact token revision unavailable before aborting
the task. Applications that need the terminal result should prefer `shutdown()`.

Direct `validate_session()` calls follow the same rule. Cancellation, a timeout after submission,
an untrustworthy success/error-code pair, an oversized or malformed response, or an unusable rotated
token all fail closed because the provider may already have replaced the old bearer. Definitive
pre-send connection failures and HTTP 429 retain the current revision and remain safely retryable.
`logout()` similarly invalidates only the exact session revision submitted to the provider; an
ambiguous logout cannot erase a newer concurrent authentication, while a definitive pre-send
failure or HTTP 429 retains the existing session.

### Automatic reconnect and subscription replay

A completed SignalR handshake establishes a socket generation. Actual reader/writer failure or
remote closure may reconnect while Connect remains requested; ordinary silence never closes a
healthy socket. Reconnect attempts are serialized and snapshot the current authentication token.
Explicit `disconnect()` disables reconnect, closes and joins socket tasks; cancelling the caller's
close future does not cancel their tracked teardown. Dropping the last `RealtimeClient` owner
cancels its background work.

The client owns no subscription truth. After `RealtimeEvent::Reconnected`, replay the application's
current subscriptions. Use `RealtimeClient::session()` to capture a `RealtimeSession` before spawning
subscription work. Its typed helpers admit only to that exact socket and return `StaleGeneration`
before enqueueing if it has been replaced. A session handle has no disconnect authority and does not
keep the client owner alive. Use `RealtimeEventReceiver::recv_message()` to receive each event with
its `RealtimeGeneration`; `Disconnected` proves both of that generation's socket tasks have stopped.
Late work cannot publish to, settle requests on, or close a replacement generation.

Invocation timeout or cancellation after queue admission remains ambiguous: the provider may have
applied the operation. Only that invocation's pending slot is reclaimed; the socket remains open.
Do not blindly retry an uncertain mutation or infer that an uncertain subscription is absent.

SignalR control traffic remains internal. The client sends a type-6 keepalive after 15 seconds
without an outbound frame and continues processing provider keepalives and invocation completions
while application event delivery is fenced. A valid provider close ends the active generation;
the caller’s Connect instruction remains active across remote closure. Only explicit Disconnect
or dropping the owner cancels recovery. WebSocket probes run every 15 seconds; an unanswered probe
for 15 seconds ends that failed socket. Ordinary application-data silence has no deadline.

### Transport gaps and `acknowledge_transport_gap`

Real-time data delivery is bounded. Queue saturation and malformed SignalR records
latch one nonterminal `TransportGap`. The accepted prefix is delivered first; the gap then reaches
the consumer without waiting for disconnection. Later application data is discarded with that
explicit signal until the consumer installs its recovery boundary and calls
`acknowledge_transport_gap()`. Valid invocation completions and keepalives continue throughout.
Malformed records do not hide later valid control records in the same batch.

Gap acknowledgement resumes data admission on the same socket. It never requests a reconnect.
If the socket actually ends during the gap, its lifecycle boundary is retained separately from the
data queue and attributed to the old generation; a replacement cannot overtake that retained tail.
An affected application projection must reconcile or obtain a fresh snapshot before claiming
continuity. A continuity gap alone does not prove physical subscriptions ended.

### Migrating from 2.x

Version 3 removes `RealtimeError::OutboundMessageTooLarge` and the corresponding arbitrary
invocation-size ceiling. Configure queue capacities on `ClientBuilder` for your workload.

Version 3 removes the previous implicit disconnect after queue overflow, invocation timeout,
invocation cancellation and inactivity. Consumers must acknowledge nonterminal gaps while their
socket is still connected, retain uncertain subscription outcomes, and distinguish these gaps
from generation-ended evidence. Prefer `recv_message()` and generation-scoped `session()` helpers
when requests overlap reconnect. The provider-native REST and exact-decimal DTO surfaces are
unchanged. No failed invocation is automatically resent.

## Real-time example

```rust
use projectx_client::{Client, ContractId, Credentials, Hub, RealtimeEvent};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::builder(Credentials::new("user", "api-key")?).build()?;
    client.authenticate().await?;

    let realtime = client.realtime(Hub::Market);
    let mut events = realtime
        .take_event_receiver()
        .ok_or_else(|| std::io::Error::other("event receiver was already claimed"))?;
    realtime.connect().await?;

    let contract = ContractId::new("CON.F.US.MNQ.M26")?;
    realtime.subscribe_contract_trades(&contract).await?;

    while let Some(event) = events.recv().await {
        match event {
            RealtimeEvent::Reconnected => {
                // Replay the application's canonical subscription set.
                realtime.subscribe_contract_trades(&contract).await?;
            }
            RealtimeEvent::TransportGap => {
                // Mark downstream state stale and start snapshot/reconciliation.
                // Only after that recovery fence is installed may reconnect resume.
                events.acknowledge_transport_gap();
            }
            _ => {}
        }
    }

    realtime.disconnect().await?;
    Ok(())
}
```

`take_event_receiver()` can be claimed once because events have a single ordered consumer. The
receiver should be drained continuously; do not perform slow reconciliation inline in the receive
loop.

## Real-time capabilities

Typed helpers reject use with the wrong hub before sending anything:

| Hub | Subscription helpers |
| --- | --- |
| `Hub::Market` | contract trades, quotes, and market depth |
| `Hub::User` | accounts plus per-account orders, positions, and trades |

Every helper has a matching unsubscribe operation. `invoke()` remains available for a provider
target that does not yet have a typed helper. Invocations are correlated with SignalR completion
frames and return only after provider acceptance, provider rejection, timeout, or session failure.
Inbound invocation messages expose their target and payload and can decode the provider entity into
a caller-selected Serde type.

## Bounds, retries, and failure behavior

The client makes overload and ambiguous execution visible instead of hiding it:

| Boundary | Behavior |
| --- | --- |
| HTTP response body | Streamed under a configurable byte limit; oversized bodies are rejected. |
| HTTP redirects | Not followed automatically, keeping one admitted attempt equal to one outbound request. |
| Dependency HTTP retries | Disabled; only the client-owned query loop retries, with fresh rate-limit admission per attempt. |
| Safe REST queries | Wait for local rate-limit capacity; transient failures use configurable bounded exponential backoff. |
| Session validation | Revision-fenced and cancellation-safe; ambiguous post-admission outcomes invalidate that exact session and require authentication. |
| Order and position mutations | Never retried automatically because a timeout can have an ambiguous outcome. |
| SignalR outbound queue | Bounded; a full queue returns `SendQueueFull` rather than growing without limit. |
| Pending SignalR invocations | Bounded and completion-correlated; timeout or disconnect fails the caller. |
| SignalR event queue | Bounded; overflow produces the fenced `TransportGap` lifecycle described above. |
| Handshake and shutdown | Readiness requires a valid SignalR handshake; close and completion waits are bounded. |

Cancellation cannot make a submitted mutation safe to repeat. If an application drops a mutation
future after polling has begun, it must treat the outcome as potentially ambiguous and reconcile
provider state before retrying, just as it would after `Error::AmbiguousMutation`.

Real-time queue sizes are configurable per hub through `ClientBuilder`:

| Setting | Default | Meaning at saturation |
| --- | --- | --- |
| `realtime_event_capacity` | 65,536 events | Retain a nonterminal gap after the accepted prefix; keep the socket running. |
| `realtime_writer_capacity` | 4,096 messages | Refuse the new invocation with `SendQueueFull` before sending it. |
| `realtime_pending_invocation_capacity` | 4,096 invocations | Refuse new admission with `PendingInvocationCapacity` until a pending call settles. |
| `realtime_invocation_timeout` | 15 seconds | End only the caller's wait, retaining an unknown outcome after admission. |

These are burst buffers and bounds on outstanding control work, not limits on active subscriptions.
Completed subscriptions occupy no pending slots. Choose capacities for the expected burst size and
consumer delay; the SDK imposes no maximum below Tokio's representable permit range. The defaults
pass synthetic fixtures with 1,024 concurrent subscriptions and a 20,000-event, multi-megabyte burst
while the consumer is paused. Coalesced records yield during decoding so a running consumer can
also drain a batch larger than its queue, including on a current-thread runtime.

There is no estimated decoded-memory budget and no arbitrary WebSocket frame, message or outbound
invocation byte ceiling. Payloads consume their actual memory; queue saturation still reports a
continuity gap. Read/write buffers use 64 KiB chunks, and the single writer flushes each dequeued
message. Retained gap and lifecycle notifications remain independent of the data queue.

Connection, handshake and close waits are 10, 10 and 5 seconds. Invocation waits include writer-queue
time and can be increased for gateways or batches requiring more than the default 15 seconds.
SignalR keepalives and active WebSocket ping/pong probes continue as described above; ordinary
silence never triggers a disconnect. REST rate budgets remain separately provider-defined.

`ClientBuilder` also supports custom provider endpoints, HTTP timeouts, response-size limits,
explicit proxy configuration, retry counts, and retry delays. The client deliberately ignores
ambient `HTTP_PROXY`, `HTTPS_PROXY`, and `ALL_PROXY` variables; use `ClientBuilder::proxy` when a
proxy is intended. Unknown provider response-enum codes remain
observable through `Unknown(code)` variants rather than being silently discarded. Prices, balances,
fees, and P&L use `rust_decimal::Decimal` throughout provider decoding; provider-native contract
counts and volumes remain integral. Exact REST and SignalR decimal tokens cross a raw JSON-number
boundary without enabling dependency-wide arbitrary-precision Serde behavior; type-1 frames are
emitted as `RealtimeEvent::Invocation` for exact typed decoding. The SignalR codec handles
record-separator framing, messages coalesced with the handshake response, and provider ping/pong
traffic.

`GatewayQuote` messages are sparse updates rather than guaranteed full snapshots. Accordingly,
`MarketQuote` keeps the symbol and provider `last_updated` timestamp required while representing
prices, change, session statistics, volume, and the separate event timestamp as `Option` values.
Consumers that need a consolidated quote must merge updates by symbol; `None` means unavailable or
not supplied and must not be replaced with a zero price or volume.

Custom remote endpoints must use HTTPS (and therefore WSS for real-time hubs) so API keys and bearer
tokens are never sent in plaintext. Plain HTTP/WS is accepted only for exact loopback hosts used by
local deterministic fixtures, and the builder rejects combining those plaintext fixture endpoints
with an explicit proxy. Ignoring ambient proxy variables also prevents a host environment from
silently redirecting those loopback credentials away from the local machine.

The bearer-bearing WebSocket upgrade runs through a dedicated HTTP/1 client and is fully validated
before the socket enters tungstenite's framing codec. This keeps the token-bearing URI out of
tungstenite's dependency logs; real-time transport errors are intentionally opaque for the same
reason.

## REST rate limits

Local rate limiting is enabled by default and follows the documented
[ProjectX Gateway API](https://gateway.docs.projectx.com/) budgets:

| Endpoint family | Rolling-window budget |
| --- | --- |
| `POST /api/History/retrieveBars` | 50 requests per 30 seconds |
| Every other authenticated REST endpoint | 200 requests per 60 seconds |

The history and general budgets are independent and shared by a `Client` and all of its clones.
Every actual request attempt consumes capacity, including a retry. Login and status ping are not
counted because they are not authenticated requests.

Safe query methods wait asynchronously for capacity without blocking a runtime thread. Rate-limit
waiting happens before the configured HTTP request timeout starts; wrap the complete method future
in `tokio::time::timeout` when an application needs an end-to-end deadline.

Money-moving methods never wait in a local throttle queue. If capacity is unavailable, they return
`Error::LocallyRateLimited`, which guarantees that no request was sent and includes the budget and
minimum retry delay. If the provider returns HTTP 429 after a mutation was sent, the client does not
retry and returns `Error::AmbiguousMutation`; reconcile provider state before deciding what to do.

For query responses, HTTP 429 becomes `Error::ProviderRateLimited`. The client accepts both
delta-seconds and HTTP-date forms of `Retry-After`, applies the longer of that delay and exponential
backoff, and publishes the cooldown to every clone. Missing or malformed `Retry-After` falls back to
the configured rolling-window duration; hostile delays are capped at 24 hours.

Custom gateways can replace the defaults with `ClientBuilder::rate_limits`. Call
`disable_rate_limits()` only when an external coordinator enforces the limits. The built-in state
cannot coordinate independently constructed clients or separate processes using the same
credentials; those deployments still require a shared external limiter.

## Deliberate live validation

Normal tests are credential-free. One ignored live probe compares the checked-in operation manifest
with the provider's public Swagger document. Separate credentialed probes dynamically select the
active MNQ expiry from the available-contract catalog, download recent hourly history, validate the
market SignalR handshake, and require both a decoded MNQ quote and trade/tick event. They never open
the user hub or invoke an order endpoint.

To check only the public REST operation set, run:

```text
cargo test --features live-tests --test gateway_surface -- --ignored
```

The credentialed probes read `PROJECTX_USERNAME` and `PROJECTX_API_KEY` from their process
environment. Inject both values only for the test process through a password manager, CI secret
store, or equivalent ephemeral secret launcher. Do not place them in an `.env` file, shell startup
file, command-line argument, or shell history, and unset any manually exported values immediately
after the probe.

Set `PROJECTX_LIVE_DATA` to exactly `false` for the simulated/evaluation data subscription or `true`
for the live data subscription. The selector is required so the probes cannot silently validate a
different catalog than intended; both values still run against the real ProjectX network. The
probes select the active MNQ contract dynamically so expiry rollover does not stale the test. Run
the streaming probe while MNQ is actively trading: no fresh trade event exists during weekends,
exchange maintenance, holidays, or halts.

When the credentials have been injected deliberately, run:

```text
cargo test --features live-tests --test live_read_only -- --ignored --test-threads=1 --nocapture
```

This deliberately runs three serialized, read-only probes: authentication/contract discovery and
market-hub handshake, recent MNQ history retrieval, and fresh MNQ quote plus trade/tick streaming.
All three must pass for the live validation gate.

## License

Licensed under the [MIT No Attribution License (MIT-0)](LICENSE).
