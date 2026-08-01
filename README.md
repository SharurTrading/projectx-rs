# projectx-rs

An async, provider-native Rust client for the ProjectX Gateway API.

This repository is currently private and under active extraction. The crate is intentionally not
publishable until its public-release checklist, provider terms review, and API compatibility review
are complete.

## Design boundaries

- No trading-platform or application dependencies.
- No environment-file loading or credential persistence.
- Exact `Decimal` values for price, money, and P&L.
- Typed provider identifiers rather than interchangeable strings and integers.
- A caller-owned Tokio runtime; the library never creates a hidden runtime.
- Typed errors, redacted credentials, bounded HTTP responses, and no automatic retry for order placement.
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

The initial private milestone provides the REST session and core account, contract, history, order,
position, and trade endpoints. SignalR market and user streams will land before public release.

