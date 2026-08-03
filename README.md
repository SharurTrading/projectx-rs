<!--
SPDX-FileCopyrightText: 2026 Kevin Monaghan
SPDX-License-Identifier: MIT
-->

# projectx-rs

[![CI](https://github.com/SharurTrading/projectx-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/SharurTrading/projectx-rs/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/projectx-client.svg?release=1.0.1)](https://crates.io/crates/projectx-client/1.0.1)
[![docs.rs](https://img.shields.io/docsrs/projectx-client/1.0.1?release=1.0.1)](https://docs.rs/projectx-client/1.0.1/projectx_client/)
[![license: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

An async, provider-native Rust client for the ProjectX Gateway API.

This project is available under the [MIT License](LICENSE). It is an independent, unofficial client
and is not affiliated with, endorsed by, or sponsored by ProjectX Trading LLC. Users are responsible
for complying with the provider's terms and maintaining an active API subscription where required.

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
projectx-client = "2"
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

After `connect()` completes the SignalR handshake, a watchdog monitors connection and activity
state. A dropped or stale connection is replaced automatically with a freshly authenticated
handshake. Reconnect attempts are serialized, so concurrent failure signals cannot create competing
sessions.

The transport deliberately does not own subscription truth. When a replacement handshake succeeds,
it emits `RealtimeEvent::Reconnected`; the application must then replay its canonical subscription
set. Keep that set outside `RealtimeClient` and make replay idempotent. Calling `disconnect()` is an
explicit shutdown: it stops the watchdog, performs a bounded close handshake, and does not trigger
automatic reconnect.

Connection generations are fenced as well. A late task or reconnect attempt from a superseded
session cannot publish readiness, complete an invocation, or tear down the replacement session. A
reader or writer failure closes the whole generation, and dropping the final real-time client handle
cancels its background work.

An invocation whose completion times out is ambiguous: the provider may have applied a subscription
change before its acknowledgement was lost. Cancellation after the invocation enters the writer
queue has the same ambiguity. In either case the client ends that connection generation; after
`Reconnected`, replay the canonical subscription set instead of guessing which operation applied.

SignalR control traffic remains internal: the client sends a type-6 SignalR ping after 15 seconds
without an outbound frame, while provider keepalive messages refresh inbound liveness without
entering the application event queue. A provider close message ends the active generation, and
automatic reconnect continues only when that message explicitly sets `allowReconnect` to `true`;
terminal provider closes therefore cannot create a reconnect loop.

### Transport gaps and `acknowledge_transport_gap`

Real-time delivery is bounded. If the consumer falls behind far enough that a provider frame cannot
enter the event queue, continuing with a partial stream would make an order book, position mirror,
or other projection silently incorrect. The client therefore disconnects, pauses automatic
reconnect, ends the overflowed connection generation, drains every event that was already accepted,
delivers that generation's final `Disconnected` event, and only then emits one ordered
`RealtimeEvent::TransportGap` marker. A generation fence prevents late producer work from entering
the queue while this ordered tail is delivered.

`RealtimeEventReceiver::acknowledge_transport_gap()` is a recovery gate, not a data repair method.
Use this sequence:

1. Receive `TransportGap` and mark every affected downstream projection stale or unavailable.
2. Install the application's recovery fence and arrange a fresh snapshot or reconciliation.
3. Call `acknowledge_transport_gap()` only after that fence is in place. Calling it before the gap
   marker has been delivered has no effect.
4. The watchdog may now reconnect. On `Reconnected`, replay the canonical subscriptions.
5. Apply snapshot-before-delta recovery and clear the stale state only when reconciliation is
   complete.

This explicit acknowledgement prevents an overflow/reconnect loop from presenting a new live stream
as though no data were lost.

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

The current built-in real-time limits are 256 outbound messages, 256 pending invocations, and 512
received events. The event queue also has a 32 MiB aggregate decoded-memory charge: each JSON event
costs 256 bytes plus 16 times its encoded frame length, so the count limit cannot multiply the
maximum frame size into an unsafe allocation. One fixed-size terminal lifecycle event is reserved
outside that data budget so a gap always ends with `Disconnected`. An outbound invocation may encode
to at most 64 KiB. WebSocket messages and individual frames are capped at 1 MiB and 256 KiB, with
64 KiB read/write buffers and a 256 KiB maximum write buffer. Connection, handshake,
invocation-completion, and close waits are bounded at 10, 10, 15, and 5 seconds respectively. The
client sends a SignalR ping after 15 seconds without an outbound frame; the watchdog checks every 5
seconds and treats 30 seconds without inbound transport activity as stale. These are client
implementation limits, not provider guarantees.

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

Licensed under the [MIT License](LICENSE).
