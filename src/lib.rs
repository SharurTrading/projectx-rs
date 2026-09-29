// SPDX-FileCopyrightText: 2026 Kevin Monaghan
// SPDX-License-Identifier: MIT-0

//! Async Rust client for the `ProjectX` Gateway API.
//!
//! This crate models `ProjectX`'s provider-native HTTP contract. It deliberately
//! owns no trading-engine concepts, runtime, environment loading, or application
//! state. Applications authenticate explicitly and translate these types at
//! their own boundary.
//!
//! Authenticated REST requests use shared rolling-window admission control.
//! Safe queries wait asynchronously for capacity, while mutations return
//! [`Error::LocallyRateLimited`] before sending when the local budget is full.
//!
//! Successful list reads return [`ProviderList`] so an explicit provider
//! array, including an authoritative empty one, stays distinguishable from a
//! success envelope that omits the field or serializes it as JSON `null`.
//!
//! # Example
//!
//! ```no_run
//! use projectx_client::{Client, Credentials, ProviderList};
//!
//! # async fn example() -> Result<(), projectx_client::Error> {
//! let credentials = Credentials::new("user", "api-key")?;
//! let client = Client::builder(credentials).build()?;
//! client.authenticate().await?;
//! match client.search_active_accounts().await? {
//!     ProviderList::Listed(accounts) => {
//!         for account in accounts {
//!             println!("{}", account.name);
//!         }
//!     }
//!     ProviderList::Absent => eprintln!("the provider omitted the account list"),
//! }
//! # Ok(())
//! # }
//! ```

mod client;
mod config;
mod credentials;
mod decimal_serde;
mod error;
mod error_codes;
mod ids;
mod models;
mod rate_limit;
mod realtime;
mod timestamp;
mod token;

pub use client::{Client, ClientBuilder, SessionValidator};
pub use config::Endpoints;
pub use credentials::{ApplicationCredentials, ApplicationCredentialsBuilder, Credentials};
pub use error::{Error, ProviderError};
pub use ids::{AccountId, ContractId, OrderId, PositionId, SymbolId, TradeId};
pub use models::{
    Account, Bar, BarUnit, Bracket, CancelOrder, CloseContract, Contract, DepthType,
    HistoryRequest, HistoryRequestBuilder, MarketDepth, MarketQuote, MarketTrade, ModifyOrder,
    ModifyOrderBuilder, OperationResponse, Order, OrderPage, OrderQuery, OrderQueryBuilder,
    OrderResponse, OrderSearch, OrderSortBy, OrderSortDirection, OrderStatus, OrderType,
    PartialCloseContract, PlaceOrder, PlaceOrderBuilder, Position, PositionType, ProviderList,
    RequestValidationError, SearchContracts, Side, Trade, TradeLogType, TradeQuery,
    TradeQueryBuilder, TradeSearch,
};
pub use rate_limit::{RateLimit, RateLimitConfig, RateLimitKind};
pub use realtime::{
    Hub, RealtimeClient, RealtimeError, RealtimeEvent, RealtimeEventReceiver, RealtimeGeneration,
    RealtimeMessage, RealtimeSession, SignalRInvocation,
};
pub use rust_decimal::Decimal;
pub use timestamp::{ProviderDate, ProviderDateError, Timestamp, TimestampError};
