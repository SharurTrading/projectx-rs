<!--
SPDX-FileCopyrightText: 2026 Kevin Monaghan
SPDX-License-Identifier: MIT
-->

# projectx-rs

An async, provider-native Rust client for the ProjectX Gateway API.

This project is available under the [MIT License](LICENSE). It is an independent, unofficial client
and is not affiliated with, endorsed by, or sponsored by ProjectX Trading LLC. Users are responsible
for complying with the provider's terms and maintaining an active API subscription where required.

## Design boundaries

- No trading-platform or application dependencies.
- No environment-file loading or credential persistence.
- Exact `Decimal` values for price, money, and P&L, parsed from the provider's
  JSON digits without an `f64` round-trip.
- Typed provider identifiers rather than interchangeable strings and integers.
- A caller-owned Tokio runtime; the library never creates a hidden runtime.
- Typed errors, redacted credentials, bounded HTTP/WebSocket queues, and no
  automatic retry for money-moving mutations.
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

## Status

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
            // Fence and recover downstream state before acknowledging.
            events.acknowledge_transport_gap();
        }
        _ => {}
    }
}
# Ok(())
# }
```

## Deliberate live validation

Normal tests are credential-free. The ignored live probe authenticates, lists
active accounts and contracts, validates the market SignalR handshake, and
disconnects without opening the user hub or invoking an order endpoint:

```text
cargo test --features live-tests --test live_read_only -- --ignored
```

## License

Licensed under the [MIT License](LICENSE).
