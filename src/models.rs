//! Provider-native request and response models.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};

use crate::{AccountId, ContractId, OrderId, PositionId, SymbolId, TradeId};

/// A `ProjectX` order side.
#[derive(Clone, Copy, Debug, Deserialize_repr, Eq, PartialEq, Serialize_repr)]
#[non_exhaustive]
#[repr(i32)]
pub enum Side {
    /// Bid (buy).
    Bid = 0,
    /// Ask (sell).
    Ask = 1,
}

/// A `ProjectX` order type.
#[derive(Clone, Copy, Debug, Deserialize_repr, Eq, PartialEq, Serialize_repr)]
#[non_exhaustive]
#[repr(i32)]
pub enum OrderType {
    /// Limit order.
    Limit = 1,
    /// Market order.
    Market = 2,
    /// Stop order.
    Stop = 4,
    /// Trailing-stop order.
    TrailingStop = 5,
    /// Join the best bid.
    JoinBid = 6,
    /// Join the best ask.
    JoinAsk = 7,
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
    #[serde(default, with = "rust_decimal::serde::float_option")]
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
    #[serde(with = "rust_decimal::serde::float")]
    pub tick_size: Decimal,
    /// Monetary value of one tick.
    #[serde(with = "rust_decimal::serde::float")]
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
    #[serde(with = "rust_decimal::serde::float")]
    pub o: Decimal,
    /// High price.
    #[serde(with = "rust_decimal::serde::float")]
    pub h: Decimal,
    /// Low price.
    #[serde(with = "rust_decimal::serde::float")]
    pub l: Decimal,
    /// Close price.
    #[serde(with = "rust_decimal::serde::float")]
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
    #[serde(default, with = "rust_decimal::serde::float_option")]
    pub limit_price: Option<Decimal>,
    /// Optional stop price.
    #[serde(default, with = "rust_decimal::serde::float_option")]
    pub stop_price: Option<Decimal>,
    /// Optional cumulative filled quantity.
    #[serde(default)]
    pub fill_volume: Option<i64>,
    /// Optional average fill price.
    #[serde(default, with = "rust_decimal::serde::float_option")]
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
        with = "rust_decimal::serde::float_option"
    )]
    pub limit_price: Option<Decimal>,
    /// Optional stop price.
    #[serde(
        skip_serializing_if = "Option::is_none",
        with = "rust_decimal::serde::float_option"
    )]
    pub stop_price: Option<Decimal>,
    /// Optional trailing price.
    #[serde(
        skip_serializing_if = "Option::is_none",
        with = "rust_decimal::serde::float_option"
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
        with = "rust_decimal::serde::float_option"
    )]
    pub limit_price: Option<Decimal>,
    /// Optional replacement stop price.
    #[serde(
        skip_serializing_if = "Option::is_none",
        with = "rust_decimal::serde::float_option"
    )]
    pub stop_price: Option<Decimal>,
    /// Optional replacement trailing price.
    #[serde(
        skip_serializing_if = "Option::is_none",
        with = "rust_decimal::serde::float_option"
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
    pub position_type: i32,
    /// Signed or directional provider quantity.
    pub size: i64,
    /// Average entry price.
    #[serde(with = "rust_decimal::serde::float")]
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
    #[serde(with = "rust_decimal::serde::float")]
    pub price: Decimal,
    /// Optional realized P&L.
    #[serde(default, with = "rust_decimal::serde::float_option")]
    pub profit_and_loss: Option<Decimal>,
    /// Provider fees.
    #[serde(with = "rust_decimal::serde::float")]
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

/// Successful response for an operation without a result body.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OperationResponse;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Envelope<T> {
    #[serde(flatten)]
    pub(crate) body: T,
    pub(crate) success: bool,
    #[serde(default)]
    pub(crate) error_code: Option<i32>,
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
