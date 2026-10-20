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
//! Safe queries wait asynchronously for capacity, while mutations and the two
//! exit reads return [`Error::LocallyRateLimited`] before sending when the
//! local budget is full.
//!
//! Successful list reads return [`ProviderList`] so an explicit provider
//! array, including an authoritative empty one, stays distinguishable from a
//! success envelope that omits the field or serializes it as JSON `null`.
//!
//! # Example
//!
//! This network example is checked for compilation by rustdoc, but it is not
//! run: authentication requires real credentials and contacting the provider.
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

/// Authenticated REST client, its builder, and its session-validation guard.
pub use client::{Client, ClientBuilder, SessionValidator};
/// REST and real-time endpoints for one `ProjectX` deployment.
pub use config::Endpoints;
/// API-key and authorized-application credentials and their builder.
pub use credentials::{ApplicationCredentials, ApplicationCredentialsBuilder, Credentials};
/// Client errors, provider rejections, and ambiguous-mutation origins.
pub use error::{
    AmbiguityOrigin, ConfigurationError, CredentialField, Error, ProviderError,
    ValidationAdmissionError,
};
/// Validated provider account, contract, order, position, symbol, and trade identifiers.
pub use ids::{AccountId, ContractId, OrderId, PositionId, SymbolId, TradeId};
/// Provider-native request, response, order, position, trade, and market-data models.
pub use models::{
    Account, Bar, BarUnit, Bracket, CancelOrder, CloseContract, Contract, DepthType,
    HistoryRequest, HistoryRequestBuilder, MarketDepth, MarketQuote, MarketTrade, ModifyOrder,
    ModifyOrderBuilder, OperationResponse, Order, OrderPage, OrderQuery, OrderQueryBuilder,
    OrderResponse, OrderSearch, OrderSortBy, OrderSortDirection, OrderStatus, OrderType,
    PartialCloseContract, PlaceOrder, PlaceOrderBuilder, Position, PositionType, ProviderList,
    RequestValidationError, SearchContracts, Side, Trade, TradeLogType, TradeQuery,
    TradeQueryBuilder, TradeSearch,
};
/// Rolling-window REST rate-limit kinds, limits, and configuration.
pub use rate_limit::{RateLimit, RateLimitConfig, RateLimitKind};
/// Real-time hubs, client, session, events, messages, and invocation decoding.
pub use realtime::{
    Hub, RealtimeClient, RealtimeError, RealtimeEvent, RealtimeEventReceiver, RealtimeGeneration,
    RealtimeMessage, RealtimeSession, SignalRInvocation,
};
/// Decimal type used for provider prices, money, balances, fees, and P&L.
pub use rust_decimal::Decimal;
/// Validated provider timestamps, civil dates, and their parse errors.
pub use timestamp::{ProviderDate, ProviderDateError, Timestamp, TimestampError};
