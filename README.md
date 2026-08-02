<!--
SPDX-FileCopyrightText: 2026 Kevin Monaghan
SPDX-License-Identifier: MIT
-->

# projectx-rs

An async, provider-native Rust client for the ProjectX Gateway API.

This project is available under the [MIT License](LICENSE). It is an independent, unofficial client
and is not affiliated with, endorsed by, or sponsored by ProjectX Trading LLC. Users are responsible
for complying with the provider's terms and maintaining an active API subscription where required.

Use the official [ProjectX Gateway API documentation](https://gateway.docs.projectx.com/) as the
reference for provider endpoints, request fields, response payloads, and subscription requirements.
This README documents the additional safety and lifecycle behavior supplied by this client.

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

```rust,no_run
use projectx_client::{Client, Credentials};

# async fn run() -> Result<(), projectx_client::Error> {
let credentials = Credentials::new("your-user-name", "your-api-key")?;
let client = Client::builder(credentials).build()?;

client.authenticate().await?;
let accounts = client.search_active_accounts().await?;
for account in accounts {
    println!("{}", account.name);
}
# Ok(())
# }
```

Applications should source secrets outside this library and must not log credentials or bearer
tokens. See [SECURITY.md](SECURITY.md).

## Feature coverage

The client covers the complete documented Gateway REST surface:

- API-key login and rotating-token validation
- active-account discovery
- contract availability, text search, and lookup by ID
- historical bars
- order search, placement, cancellation, and modification
- open positions, full close, and partial close
- execution/trade search

It also implements both documented SignalR hubs, market/user subscription helpers,
bounded event delivery, reconnect notification, and exact provider payload models.

### Session validation and token rotation

`authenticate()` performs API-key login and stores the bearer token privately. Applications that
run for more than a short request cycle can instead use `authenticate_with_validation(period)`:

```rust,no_run
use std::time::Duration;

use projectx_client::{Client, Credentials};

# async fn run() -> Result<(), projectx_client::Error> {
let client = Client::builder(Credentials::new("user", "api-key")?).build()?;
let validator = client
    .authenticate_with_validation(Duration::from_secs(15 * 60))
    .await?;

// REST requests and real-time hubs created from `client` share the rotating token.

validator.shutdown().await;
# Ok(())
# }
```

The validator periodically calls the provider's validation endpoint and atomically replaces the
stored token when the provider returns a new one. A real-time connection snapshots the latest token
immediately before every initial connection and reconnect, so a reconnect does not reuse the token
from the original WebSocket session. Dropping `SessionValidator` cancels validation; call
`shutdown().await` when waiting for the background task to finish matters.

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

### Transport gaps and `acknowledge_transport_gap`

Real-time delivery is bounded. If the consumer falls behind far enough that a provider frame cannot
enter the event queue, continuing with a partial stream would make an order book, position mirror,
or other projection silently incorrect. The client therefore disconnects, pauses automatic
reconnect, drains events that were already accepted, and then emits one ordered
`RealtimeEvent::TransportGap` marker.

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

```rust,no_run
use projectx_client::{Client, ContractId, Credentials, Hub, RealtimeEvent};

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let client = Client::builder(Credentials::new("user", "api-key")?).build()?;
client.authenticate().await?;

let realtime = client.realtime(Hub::Market);
let mut events = realtime
    .take_event_receiver()
    .await
    .ok_or("event receiver was already claimed")?;
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
# Ok(())
# }
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
| Safe REST queries | Wait for local rate-limit capacity; transient failures use configurable bounded exponential backoff. |
| Order and position mutations | Never retried automatically because a timeout can have an ambiguous outcome. |
| SignalR outbound queue | Bounded; a full queue returns `SendQueueFull` rather than growing without limit. |
| Pending SignalR invocations | Bounded and completion-correlated; timeout or disconnect fails the caller. |
| SignalR event queue | Bounded; overflow produces the fenced `TransportGap` lifecycle described above. |
| Handshake and shutdown | Readiness requires a valid SignalR handshake; close and completion waits are bounded. |

The current built-in real-time limits are 1,024 outbound messages, 1,024 pending invocations, and
10,000 received events. Handshake, invocation-completion, and close waits are bounded at 10, 15, and
5 seconds respectively. The watchdog checks every 5 seconds and treats 30 seconds without transport
activity as stale. These are client implementation limits, not provider guarantees.

`ClientBuilder` also supports custom provider endpoints, HTTP timeouts, response-size limits, proxy
configuration, retry counts, and retry delays. Unknown provider enum codes remain observable through
`Unknown(code)` variants rather than being silently discarded. Prices, quantities, balances, and
P&L remain `rust_decimal::Decimal` throughout provider decoding. The SignalR codec handles record
separator framing, messages coalesced with the handshake response, and provider ping/pong traffic.

## REST rate limits

Local rate limiting is enabled by default and follows the documented
[ProjectX Gateway API](https://gateway.docs.projectx.com/) budgets:

| Endpoint family | Rolling-window budget |
| --- | --- |
| `POST /api/History/retrieveBars` | 50 requests per 30 seconds |
| Every other authenticated REST endpoint | 200 requests per 60 seconds |

The history and general budgets are independent and shared by a `Client` and all of its clones.
Every actual request attempt consumes capacity, including a retry. API-key login is not counted
because it is not an authenticated request.

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
the configured rolling-window duration.

Custom gateways can replace the defaults with `ClientBuilder::rate_limits`. Call
`disable_rate_limits()` only when an external coordinator enforces the limits. The built-in state
cannot coordinate independently constructed clients or separate processes using the same
credentials; those deployments still require a shared external limiter.

## Deliberate live validation

Normal tests are credential-free. The ignored live probe authenticates, lists
active accounts and contracts, validates the market SignalR handshake, and
disconnects without opening the user hub or invoking an order endpoint:

```text
cargo test --features live-tests --test live_read_only -- --ignored
```

## License

Licensed under the [MIT License](LICENSE).
