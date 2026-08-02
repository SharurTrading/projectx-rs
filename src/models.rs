// SPDX-FileCopyrightText: 2026 Kevin Monaghan
// SPDX-License-Identifier: MIT

//! Provider-native request and response models.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};

use crate::{AccountId, ContractId, OrderId, PositionId, SymbolId, TradeId};

/// A `ProjectX` order side.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum Side {
    /// Bid (buy).
    Bid,
    /// Ask (sell).
    Ask,
    /// Provider code not known to this crate version.
    Unknown(i32),
}

impl Side {
    /// Returns the provider's numeric wire code.
    #[must_use]
    pub const fn code(self) -> i32 {
        match self {
            Self::Bid => 0,
            Self::Ask => 1,
            Self::Unknown(code) => code,
        }
    }
}

impl Serialize for Side {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_i32(self.code())
    }
}

impl<'de> Deserialize<'de> for Side {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Ok(match i32::deserialize(deserializer)? {
            0 => Self::Bid,
            1 => Self::Ask,
            code => Self::Unknown(code),
        })
    }
}

/// A `ProjectX` order type.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum OrderType {
    /// Limit order.
    Limit,
    /// Market order.
    Market,
    /// Stop order.
    Stop,
    /// Trailing-stop order.
    TrailingStop,
    /// Join the best bid.
    JoinBid,
    /// Join the best ask.
    JoinAsk,
    /// Provider code not known to this crate version.
    Unknown(i32),
}

impl OrderType {
    /// Returns the provider's numeric wire code.
    #[must_use]
    pub const fn code(self) -> i32 {
        match self {
            Self::Limit => 1,
            Self::Market => 2,
            Self::Stop => 4,
            Self::TrailingStop => 5,
            Self::JoinBid => 6,
            Self::JoinAsk => 7,
            Self::Unknown(code) => code,
        }
    }
}

impl Serialize for OrderType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_i32(self.code())
    }
}

impl<'de> Deserialize<'de> for OrderType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Ok(match i32::deserialize(deserializer)? {
            1 => Self::Limit,
            2 => Self::Market,
            4 => Self::Stop,
            5 => Self::TrailingStop,
            6 => Self::JoinBid,
            7 => Self::JoinAsk,
            code => Self::Unknown(code),
        })
    }
}

/// A `ProjectX` position direction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum PositionType {
    /// No directional position.
    Undefined,
    /// Net long position.
    Long,
    /// Net short position.
    Short,
    /// Provider code not known to this crate version.
    Unknown(i32),
}

impl PositionType {
    /// Returns the provider's numeric wire code.
    #[must_use]
    pub const fn code(self) -> i32 {
        match self {
            Self::Undefined => 0,
            Self::Long => 1,
            Self::Short => 2,
            Self::Unknown(code) => code,
        }
    }
}

impl Serialize for PositionType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_i32(self.code())
    }
}

impl<'de> Deserialize<'de> for PositionType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Ok(match i32::deserialize(deserializer)? {
            0 => Self::Undefined,
            1 => Self::Long,
            2 => Self::Short,
            code => Self::Unknown(code),
        })
    }
}

/// A `ProjectX` depth-of-market update kind.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum DepthType {
    /// Provider sentinel with no book mutation.
    Unknown,
    /// Resting ask level.
    Ask,
    /// Resting bid level.
    Bid,
    /// Best ask update.
    BestAsk,
    /// Best bid update.
    BestBid,
    /// Trade notification carried on the depth stream.
    Trade,
    /// Full book reset.
    Reset,
    /// Session-low notification.
    Low,
    /// Session-high notification.
    High,
    /// New best bid.
    NewBestBid,
    /// New best ask.
    NewBestAsk,
    /// Fill notification carried on the depth stream.
    Fill,
    /// Provider code not known to this crate version.
    UnknownCode(i32),
}

impl DepthType {
    /// Returns the provider's numeric wire code.
    #[must_use]
    pub const fn code(self) -> i32 {
        match self {
            Self::Unknown => 0,
            Self::Ask => 1,
            Self::Bid => 2,
            Self::BestAsk => 3,
            Self::BestBid => 4,
            Self::Trade => 5,
            Self::Reset => 6,
            Self::Low => 7,
            Self::High => 8,
            Self::NewBestBid => 9,
            Self::NewBestAsk => 10,
            Self::Fill => 11,
            Self::UnknownCode(code) => code,
        }
    }
}

impl Serialize for DepthType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_i32(self.code())
    }
}

impl<'de> Deserialize<'de> for DepthType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Ok(match i32::deserialize(deserializer)? {
            0 => Self::Unknown,
            1 => Self::Ask,
            2 => Self::Bid,
            3 => Self::BestAsk,
            4 => Self::BestBid,
            5 => Self::Trade,
            6 => Self::Reset,
            7 => Self::Low,
            8 => Self::High,
            9 => Self::NewBestBid,
            10 => Self::NewBestAsk,
            11 => Self::Fill,
            code => Self::UnknownCode(code),
        })
    }
}

/// Historical-bar aggregation unit.
#[derive(Clone, Copy, Debug, Deserialize_repr, Eq, PartialEq, Serialize_repr)]
#[non_exhaustive]
#[repr(i32)]
pub enum BarUnit {
    /// Seconds.
    Second = 1,
    /// Minutes.
    Minute = 2,
    /// Hours.
    Hour = 3,
    /// Days.
    Day = 4,
    /// Weeks.
    Week = 5,
    /// Months.
    Month = 6,
}

/// A `ProjectX` account.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Account {
    /// Provider account identifier.
    pub id: AccountId,
    /// Provider display name.
    pub name: String,
    /// Current account balance, when included by the endpoint.
    #[serde(default, with = "rust_decimal::serde::arbitrary_precision_option")]
    pub balance: Option<Decimal>,
    /// Whether the provider permits trading.
    pub can_trade: bool,
    /// Whether the provider marks the account visible.
    pub is_visible: bool,
    /// Whether this is a simulated account, when included by the endpoint.
    #[serde(default)]
    pub simulated: Option<bool>,
}

/// A `ProjectX` futures contract.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Contract {
    /// Provider contract identifier.
    pub id: ContractId,
    /// Provider short name.
    pub name: String,
    /// Human-readable description.
    pub description: String,
    /// Minimum price increment.
    #[serde(with = "rust_decimal::serde::arbitrary_precision")]
    pub tick_size: Decimal,
    /// Monetary value of one tick.
    #[serde(with = "rust_decimal::serde::arbitrary_precision")]
    pub tick_value: Decimal,
    /// Whether this is the provider's active contract.
    pub active_contract: bool,
    /// Provider root symbol identifier.
    pub symbol_id: SymbolId,
}

/// Contract search parameters.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SearchContracts {
    /// Whether to search the live-data catalog.
    pub live: bool,
    /// Provider search text.
    pub search_text: String,
}

/// Historical-bar request parameters.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HistoryRequest {
    /// Explicit provider contract.
    pub contract_id: ContractId,
    /// Whether to use the live-data subscription.
    pub live: bool,
    /// ISO-8601 range start.
    pub start_time: String,
    /// ISO-8601 range end.
    pub end_time: String,
    /// Aggregation unit.
    pub unit: BarUnit,
    /// Number of units per bar.
    pub unit_number: i32,
    /// Maximum number of bars, up to the provider limit of 20,000.
    pub limit: i32,
    /// Whether to include the current partial bar.
    pub include_partial_bar: bool,
}

/// A historical OHLCV bar.
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct Bar {
    /// Provider timestamp.
    pub t: String,
    /// Open price.
    #[serde(with = "rust_decimal::serde::arbitrary_precision")]
    pub o: Decimal,
    /// High price.
    #[serde(with = "rust_decimal::serde::arbitrary_precision")]
    pub h: Decimal,
    /// Low price.
    #[serde(with = "rust_decimal::serde::arbitrary_precision")]
    pub l: Decimal,
    /// Close price.
    #[serde(with = "rust_decimal::serde::arbitrary_precision")]
    pub c: Decimal,
    /// Provider volume units.
    pub v: i64,
}

/// Historical order search parameters.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderSearch {
    /// Provider account.
    pub account_id: AccountId,
    /// ISO-8601 range start.
    pub start_timestamp: String,
    /// Optional ISO-8601 range end.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_timestamp: Option<String>,
}

/// A `ProjectX` order.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Order {
    /// Provider order identifier.
    pub id: OrderId,
    /// Provider account.
    pub account_id: AccountId,
    /// Provider contract.
    pub contract_id: ContractId,
    /// Provider symbol, when included by the endpoint.
    #[serde(default)]
    pub symbol_id: Option<SymbolId>,
    /// Provider creation timestamp.
    pub creation_timestamp: String,
    /// Provider update timestamp.
    pub update_timestamp: String,
    /// Provider order-status code.
    pub status: i32,
    /// Provider order type.
    #[serde(rename = "type")]
    pub order_type: OrderType,
    /// Order side.
    pub side: Side,
    /// Ordered quantity.
    pub size: i64,
    /// Optional limit price.
    #[serde(default, with = "rust_decimal::serde::arbitrary_precision_option")]
    pub limit_price: Option<Decimal>,
    /// Optional stop price.
    #[serde(default, with = "rust_decimal::serde::arbitrary_precision_option")]
    pub stop_price: Option<Decimal>,
    /// Optional cumulative filled quantity.
    #[serde(default)]
    pub fill_volume: Option<i64>,
    /// Optional average fill price.
    #[serde(default, with = "rust_decimal::serde::arbitrary_precision_option")]
    pub filled_price: Option<Decimal>,
    /// Optional caller tag.
    #[serde(default)]
    pub custom_tag: Option<String>,
}

/// `ProjectX` bracket-leg configuration.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Bracket {
    /// Distance in provider ticks.
    pub ticks: i32,
    /// Bracket order type.
    #[serde(rename = "type")]
    pub order_type: OrderType,
}

/// Order placement parameters.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PlaceOrder {
    /// Provider account.
    pub account_id: AccountId,
    /// Provider contract.
    pub contract_id: ContractId,
    /// Order type.
    #[serde(rename = "type")]
    pub order_type: OrderType,
    /// Order side.
    pub side: Side,
    /// Order quantity.
    pub size: i64,
    /// Optional limit price.
    #[serde(
        skip_serializing_if = "Option::is_none",
        with = "rust_decimal::serde::arbitrary_precision_option"
    )]
    pub limit_price: Option<Decimal>,
    /// Optional stop price.
    #[serde(
        skip_serializing_if = "Option::is_none",
        with = "rust_decimal::serde::arbitrary_precision_option"
    )]
    pub stop_price: Option<Decimal>,
    /// Optional trailing price.
    #[serde(
        skip_serializing_if = "Option::is_none",
        with = "rust_decimal::serde::arbitrary_precision_option"
    )]
    pub trail_price: Option<Decimal>,
    /// Optional caller tag. It must be unique within the account.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub custom_tag: Option<String>,
    /// Optional stop-loss bracket.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop_loss_bracket: Option<Bracket>,
    /// Optional take-profit bracket.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub take_profit_bracket: Option<Bracket>,
}

/// Successful order-placement result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderResponse {
    /// Provider order identifier.
    pub order_id: OrderId,
}

/// Order cancellation parameters.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CancelOrder {
    /// Provider account.
    pub account_id: AccountId,
    /// Provider order.
    pub order_id: OrderId,
}

/// Order modification parameters.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ModifyOrder {
    /// Provider account.
    pub account_id: AccountId,
    /// Provider order.
    pub order_id: OrderId,
    /// Optional replacement quantity.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<i64>,
    /// Optional replacement limit price.
    #[serde(
        skip_serializing_if = "Option::is_none",
        with = "rust_decimal::serde::arbitrary_precision_option"
    )]
    pub limit_price: Option<Decimal>,
    /// Optional replacement stop price.
    #[serde(
        skip_serializing_if = "Option::is_none",
        with = "rust_decimal::serde::arbitrary_precision_option"
    )]
    pub stop_price: Option<Decimal>,
    /// Optional replacement trailing price.
    #[serde(
        skip_serializing_if = "Option::is_none",
        with = "rust_decimal::serde::arbitrary_precision_option"
    )]
    pub trail_price: Option<Decimal>,
}

/// Position close parameters.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CloseContract {
    /// Provider account.
    pub account_id: AccountId,
    /// Provider contract.
    pub contract_id: ContractId,
}

/// Partial-position close parameters.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PartialCloseContract {
    /// Provider account.
    pub account_id: AccountId,
    /// Provider contract.
    pub contract_id: ContractId,
    /// Positive quantity to close.
    pub size: i64,
}

/// A `ProjectX` open position.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Position {
    /// Provider position identifier.
    pub id: PositionId,
    /// Provider account.
    pub account_id: AccountId,
    /// Provider contract.
    pub contract_id: ContractId,
    /// Provider creation timestamp.
    pub creation_timestamp: String,
    /// Provider position-type code.
    #[serde(rename = "type")]
    pub position_type: PositionType,
    /// Signed or directional provider quantity.
    pub size: i64,
    /// Average entry price.
    #[serde(with = "rust_decimal::serde::arbitrary_precision")]
    pub average_price: Decimal,
}

/// Trade search parameters.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TradeSearch {
    /// Provider account.
    pub account_id: AccountId,
    /// ISO-8601 range start.
    pub start_timestamp: String,
    /// Optional ISO-8601 range end.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_timestamp: Option<String>,
}

/// A `ProjectX` execution trade.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Trade {
    /// Provider trade identifier.
    pub id: TradeId,
    /// Provider account.
    pub account_id: AccountId,
    /// Provider contract.
    pub contract_id: ContractId,
    /// Provider creation timestamp.
    pub creation_timestamp: String,
    /// Execution price.
    #[serde(with = "rust_decimal::serde::arbitrary_precision")]
    pub price: Decimal,
    /// Optional realized P&L.
    #[serde(default, with = "rust_decimal::serde::arbitrary_precision_option")]
    pub profit_and_loss: Option<Decimal>,
    /// Provider fees.
    #[serde(with = "rust_decimal::serde::arbitrary_precision")]
    pub fees: Decimal,
    /// Execution side.
    pub side: Side,
    /// Execution quantity.
    pub size: i64,
    /// Whether the provider voided this trade.
    pub voided: bool,
    /// Originating order.
    pub order_id: OrderId,
}

/// Consolidated quote from the market hub.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct MarketQuote {
    /// Provider symbol identifier.
    #[serde(alias = "symbol")]
    pub raw_symbol: SymbolId,
    /// Human-readable symbol name, when supplied.
    #[serde(default)]
    pub symbol_name: Option<String>,
    /// Last trade price.
    #[serde(with = "rust_decimal::serde::arbitrary_precision")]
    pub last_price: Decimal,
    /// Best bid price.
    #[serde(with = "rust_decimal::serde::arbitrary_precision")]
    pub best_bid: Decimal,
    /// Best ask price.
    #[serde(with = "rust_decimal::serde::arbitrary_precision")]
    pub best_ask: Decimal,
    /// Session price change.
    #[serde(with = "rust_decimal::serde::arbitrary_precision")]
    pub change: Decimal,
    /// Session percent change.
    #[serde(with = "rust_decimal::serde::arbitrary_precision")]
    pub change_percent: Decimal,
    /// Session open.
    #[serde(default, with = "rust_decimal::serde::arbitrary_precision_option")]
    pub open: Option<Decimal>,
    /// Session high.
    #[serde(default, with = "rust_decimal::serde::arbitrary_precision_option")]
    pub high: Option<Decimal>,
    /// Session low.
    #[serde(default, with = "rust_decimal::serde::arbitrary_precision_option")]
    pub low: Option<Decimal>,
    /// Session cumulative volume.
    pub volume: i64,
    /// Provider last-updated timestamp.
    pub last_updated: String,
    /// Event timestamp.
    pub timestamp: String,
}

/// Depth-of-market update from the market hub.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct MarketDepth {
    /// Provider symbol identifier, when supplied.
    #[serde(default, alias = "symbolId")]
    pub symbol_id: Option<SymbolId>,
    /// Event timestamp.
    pub timestamp: String,
    /// Provider depth event code.
    #[serde(rename = "type")]
    pub depth_type: DepthType,
    /// Price level.
    #[serde(with = "rust_decimal::serde::arbitrary_precision")]
    pub price: Decimal,
    /// Incremental volume for the update.
    pub volume: i64,
    /// Resting volume after the update.
    pub current_volume: i64,
    /// Zero-based level index, when supplied.
    #[serde(default)]
    pub index: Option<i32>,
}

/// Trade print from the market hub.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct MarketTrade {
    /// Provider symbol identifier.
    pub symbol_id: SymbolId,
    /// Trade price.
    #[serde(with = "rust_decimal::serde::arbitrary_precision")]
    pub price: Decimal,
    /// Event timestamp.
    pub timestamp: String,
    /// Provider trade-log code.
    #[serde(rename = "type")]
    pub trade_type: i32,
    /// Trade quantity.
    pub volume: i64,
}

/// Successful response for an operation without a result body.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OperationResponse;

#[derive(Debug)]
pub(crate) struct Envelope<T> {
    pub(crate) body: T,
    pub(crate) success: bool,
    pub(crate) error_code: Option<i32>,
}

impl<'de, T> Deserialize<'de> for Envelope<T>
where
    T: serde::de::DeserializeOwned,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error as _;

        let value = serde_json::Value::deserialize(deserializer)?;
        let mut object = value
            .as_object()
            .cloned()
            .ok_or_else(|| D::Error::custom("provider response must be an object"))?;
        let success = object
            .remove("success")
            .and_then(|value| value.as_bool())
            .ok_or_else(|| D::Error::custom("provider response success flag is missing"))?;
        let error_code = object
            .remove("errorCode")
            .map(serde_json::from_value)
            .transpose()
            .map_err(D::Error::custom)?
            .flatten();
        object.remove("errorMessage");
        let body =
            serde_json::from_value(serde_json::Value::Object(object)).map_err(D::Error::custom)?;
        Ok(Self {
            body,
            success,
            error_code,
        })
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct AccountsBody {
    pub(crate) accounts: Vec<Account>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ContractsBody {
    pub(crate) contracts: Vec<Contract>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ContractBody {
    pub(crate) contract: Contract,
}

#[derive(Debug, Deserialize)]
pub(crate) struct BarsBody {
    #[serde(default, deserialize_with = "null_to_empty")]
    pub(crate) bars: Vec<Bar>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct OrdersBody {
    pub(crate) orders: Vec<Order>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PlaceOrderBody {
    pub(crate) order_id: Option<OrderId>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct PositionsBody {
    pub(crate) positions: Vec<Position>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct TradesBody {
    pub(crate) trades: Vec<Trade>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct EmptyBody {}

pub(crate) fn null_to_empty<'de, D, T>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Ok(Option::<Vec<T>>::deserialize(deserializer)?.unwrap_or_default())
}
