// SPDX-FileCopyrightText: 2026 Kevin Monaghan
// SPDX-License-Identifier: MIT

//! Async Rust client for the `ProjectX` Gateway API.
//!
//! This crate models `ProjectX`'s provider-native HTTP contract. It deliberately
//! owns no trading-engine concepts, runtime, environment loading, or application
//! state. Applications authenticate explicitly and translate these types at
//! their own boundary.
//!
//! # Example
//!
//! ```no_run
//! use projectx_client::{Client, Credentials};
//!
//! # async fn example() -> Result<(), projectx_client::Error> {
//! let credentials = Credentials::new("user", "api-key")?;
//! let client = Client::builder(credentials).build()?;
//! client.authenticate().await?;
//! let accounts = client.search_active_accounts().await?;
//! # let _ = accounts;
//! # Ok(())
//! # }
//! ```

mod client;
mod config;
mod credentials;
mod error;
mod ids;
mod models;
mod realtime;
mod token;

pub use client::{Client, ClientBuilder, SessionValidator};
pub use config::Endpoints;
pub use credentials::Credentials;
pub use error::{Error, ProviderError};
pub use ids::{AccountId, ContractId, OrderId, PositionId, SymbolId, TradeId};
pub use models::{
    Account, Bar, BarUnit, Bracket, CancelOrder, CloseContract, Contract, DepthType,
    HistoryRequest, MarketDepth, MarketQuote, MarketTrade, ModifyOrder, OperationResponse, Order,
    OrderResponse, OrderSearch, OrderType, PartialCloseContract, PlaceOrder, Position,
    PositionType, SearchContracts, Side, Trade, TradeSearch,
};
pub use realtime::{
    Hub, RealtimeClient, RealtimeError, RealtimeEvent, RealtimeEventReceiver, SignalRInvocation,
};
pub use rust_decimal::Decimal;
