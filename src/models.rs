// SPDX-FileCopyrightText: 2026 Kevin Monaghan
// SPDX-License-Identifier: MIT

//! Provider-native request and response models.

use std::collections::BTreeMap;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use serde_repr::{Deserialize_repr, Serialize_repr};
use thiserror::Error;

use crate::{
    AccountId, ContractId, OrderId, PositionId, ProviderDate, SymbolId, Timestamp, TradeId,
};

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
    /// Stop-limit response code.
    ///
    /// The current provider request reference does not document this type for
    /// order placement or bracket creation, so validated request builders
    /// reject it while response decoding preserves the wire value.
    StopLimit,
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
            Self::StopLimit => 3,
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
            3 => Self::StopLimit,
            4 => Self::Stop,
            5 => Self::TrailingStop,
            6 => Self::JoinBid,
            7 => Self::JoinAsk,
            code => Self::Unknown(code),
        })
    }
}

/// A `ProjectX` order lifecycle status.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum OrderStatus {
    /// Provider sentinel indicating no lifecycle status.
    None,
    /// Working order.
    Open,
    /// Completely filled order.
    Filled,
    /// Cancelled order.
    Cancelled,
    /// Expired order.
    Expired,
    /// Provider-rejected order.
    Rejected,
    /// Order awaiting activation or acknowledgement.
    Pending,
    /// Order awaiting cancellation.
    PendingCancellation,
    /// Suspended order, including inactive bracket children.
    Suspended,
    /// Provider code not known to this crate version.
    Unknown(i32),
}

/// Field used to sort an [`OrderQuery`] result page.
///
/// This enum is request-only: response order is represented by the returned
/// [`OrderPage::orders`] sequence.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize_repr)]
#[non_exhaustive]
#[repr(i32)]
pub enum OrderSortBy {
    /// Sort by order creation time.
    CreatedAt = 0,
    /// Sort by provider order identifier.
    Id = 1,
}

/// Direction used to sort an [`OrderQuery`] result page.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize_repr)]
#[non_exhaustive]
#[repr(i32)]
pub enum OrderSortDirection {
    /// Ascending order.
    Ascending = 0,
    /// Descending order.
    Descending = 1,
}

impl OrderStatus {
    /// Returns the provider's numeric wire code.
    #[must_use]
    pub const fn code(self) -> i32 {
        match self {
            Self::None => 0,
            Self::Open => 1,
            Self::Filled => 2,
            Self::Cancelled => 3,
            Self::Expired => 4,
            Self::Rejected => 5,
            Self::Pending => 6,
            Self::PendingCancellation => 7,
            Self::Suspended => 8,
            Self::Unknown(code) => code,
        }
    }
}

impl Serialize for OrderStatus {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_i32(self.code())
    }
}

impl<'de> Deserialize<'de> for OrderStatus {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Ok(match i32::deserialize(deserializer)? {
            0 => Self::None,
            1 => Self::Open,
            2 => Self::Filled,
            3 => Self::Cancelled,
            4 => Self::Expired,
            5 => Self::Rejected,
            6 => Self::Pending,
            7 => Self::PendingCancellation,
            8 => Self::Suspended,
            code => Self::Unknown(code),
        })
    }
}

/// A `ProjectX` market-trade aggressor classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum TradeLogType {
    /// Buyer-initiated trade.
    Buy,
    /// Seller-initiated trade.
    Sell,
    /// Provider code not known to this crate version.
    Unknown(i32),
}

impl TradeLogType {
    /// Returns the provider's numeric wire code.
    #[must_use]
    pub const fn code(self) -> i32 {
        match self {
            Self::Buy => 0,
            Self::Sell => 1,
            Self::Unknown(code) => code,
        }
    }
}

impl Serialize for TradeLogType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_i32(self.code())
    }
}

impl<'de> Deserialize<'de> for TradeLogType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Ok(match i32::deserialize(deserializer)? {
            0 => Self::Buy,
            1 => Self::Sell,
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
///
/// The provider's `Unspecified = 0` sentinel is intentionally omitted so a
/// request must select a concrete aggregation.
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
    /// Individual trades (ticks).
    Tick = 7,
}

/// A `ProjectX` account.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[non_exhaustive]
#[serde(rename_all = "camelCase")]
pub struct Account {
    /// Provider account identifier.
    pub id: AccountId,
    /// Provider display name.
    pub name: String,
    /// Current account balance, when included by the endpoint.
    #[serde(default, with = "crate::decimal_serde::option")]
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
#[non_exhaustive]
#[serde(rename_all = "camelCase")]
pub struct Contract {
    /// Provider contract identifier.
    pub id: ContractId,
    /// Provider short name.
    pub name: String,
    /// Human-readable description.
    pub description: String,
    /// Minimum price increment.
    #[serde(with = "crate::decimal_serde")]
    pub tick_size: Decimal,
    /// Monetary value of one tick.
    #[serde(with = "crate::decimal_serde")]
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
///
/// Construct this request with [`HistoryRequest::builder`]. The builder starts
/// with one unit per bar, the provider maximum of 20,000 bars, and partial bars
/// excluded; each default can be overridden explicitly.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HistoryRequest {
    /// Explicit provider contract.
    contract_id: ContractId,
    /// Whether to use the live-data subscription.
    live: bool,
    /// Absolute range start.
    start_time: Timestamp,
    /// Absolute range end.
    end_time: Timestamp,
    /// Aggregation unit.
    unit: BarUnit,
    /// Number of units per bar.
    unit_number: i32,
    /// Maximum number of bars, up to the provider limit of 20,000.
    limit: i32,
    /// Whether to include the current partial bar.
    include_partial_bar: bool,
}

impl HistoryRequest {
    /// Starts a validated historical-bar request.
    pub fn builder(
        contract_id: ContractId,
        live: bool,
        start_time: Timestamp,
        end_time: Timestamp,
        unit: BarUnit,
    ) -> HistoryRequestBuilder {
        HistoryRequestBuilder {
            contract_id,
            live,
            start_time,
            end_time,
            unit,
            unit_number: 1,
            limit: 20_000,
            include_partial_bar: false,
        }
    }

    /// Borrows the provider contract.
    #[must_use]
    pub const fn contract_id(&self) -> &ContractId {
        &self.contract_id
    }

    /// Returns whether the live-data subscription is selected.
    #[must_use]
    pub const fn is_live(&self) -> bool {
        self.live
    }

    /// Returns the absolute range start.
    #[must_use]
    pub const fn start_time(&self) -> Timestamp {
        self.start_time
    }

    /// Returns the absolute range end.
    #[must_use]
    pub const fn end_time(&self) -> Timestamp {
        self.end_time
    }

    /// Returns the aggregation unit.
    #[must_use]
    pub const fn unit(&self) -> BarUnit {
        self.unit
    }

    /// Returns the positive number of units per bar.
    #[must_use]
    pub const fn unit_number(&self) -> i32 {
        self.unit_number
    }

    /// Returns the requested bar limit in `1..=20_000`.
    #[must_use]
    pub const fn limit(&self) -> i32 {
        self.limit
    }

    /// Returns whether the current partial bar is requested.
    #[must_use]
    pub const fn includes_partial_bar(&self) -> bool {
        self.include_partial_bar
    }
}

/// Builder for a validated [`HistoryRequest`].
#[derive(Clone, Debug)]
#[must_use = "a HistoryRequestBuilder does nothing until build is called"]
pub struct HistoryRequestBuilder {
    contract_id: ContractId,
    live: bool,
    start_time: Timestamp,
    end_time: Timestamp,
    unit: BarUnit,
    unit_number: i32,
    limit: i32,
    include_partial_bar: bool,
}

impl HistoryRequestBuilder {
    /// Sets the positive number of units per bar.
    pub const fn unit_number(mut self, unit_number: i32) -> Self {
        self.unit_number = unit_number;
        self
    }

    /// Sets the maximum number of bars in `1..=20_000`.
    pub const fn limit(mut self, limit: i32) -> Self {
        self.limit = limit;
        self
    }

    /// Selects whether to include the current partial bar.
    pub const fn include_partial_bar(mut self, include: bool) -> Self {
        self.include_partial_bar = include;
        self
    }

    /// Validates and builds the historical-bar request.
    ///
    /// # Errors
    ///
    /// Returns an error when the range does not increase, the unit number is
    /// non-positive, or the limit falls outside `1..=20_000`.
    pub fn build(self) -> Result<HistoryRequest, RequestValidationError> {
        if self.start_time >= self.end_time {
            return Err(RequestValidationError::HistoryRangeNotIncreasing);
        }
        if self.unit_number <= 0 {
            return Err(RequestValidationError::NonPositiveHistoryUnitNumber);
        }
        if !(1..=20_000).contains(&self.limit) {
            return Err(RequestValidationError::HistoryLimitOutOfRange);
        }
        Ok(HistoryRequest {
            contract_id: self.contract_id,
            live: self.live,
            start_time: self.start_time,
            end_time: self.end_time,
            unit: self.unit,
            unit_number: self.unit_number,
            limit: self.limit,
            include_partial_bar: self.include_partial_bar,
        })
    }
}

/// A historical OHLCV bar.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[non_exhaustive]
pub struct Bar {
    /// Provider timestamp.
    pub t: Timestamp,
    /// Open price.
    #[serde(with = "crate::decimal_serde")]
    pub o: Decimal,
    /// High price.
    #[serde(with = "crate::decimal_serde")]
    pub h: Decimal,
    /// Low price.
    #[serde(with = "crate::decimal_serde")]
    pub l: Decimal,
    /// Close price.
    #[serde(with = "crate::decimal_serde")]
    pub c: Decimal,
    /// Provider volume units.
    pub v: i64,
    /// Optional provider business date.
    #[serde(default)]
    pub d: Option<ProviderDate>,
    /// Optional provider aggregate key.
    #[serde(default)]
    pub k: Option<i64>,
}

/// Historical order search parameters.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderSearch {
    /// Provider account.
    account_id: AccountId,
    /// Absolute range start.
    start_timestamp: Timestamp,
    /// Optional absolute range end.
    #[serde(skip_serializing_if = "Option::is_none")]
    end_timestamp: Option<Timestamp>,
}

impl OrderSearch {
    /// Creates a validated historical order search.
    ///
    /// # Errors
    ///
    /// Returns [`RequestValidationError::SearchRangeNotIncreasing`] when an
    /// end timestamp is not later than the start timestamp.
    pub fn new(
        account_id: AccountId,
        start_timestamp: Timestamp,
        end_timestamp: Option<Timestamp>,
    ) -> Result<Self, RequestValidationError> {
        validate_search_range(start_timestamp, end_timestamp)?;
        Ok(Self {
            account_id,
            start_timestamp,
            end_timestamp,
        })
    }

    /// Returns the provider account.
    #[must_use]
    pub const fn account_id(&self) -> AccountId {
        self.account_id
    }

    /// Returns the range start.
    #[must_use]
    pub const fn start_timestamp(&self) -> Timestamp {
        self.start_timestamp
    }

    /// Returns the optional range end.
    #[must_use]
    pub const fn end_timestamp(&self) -> Option<Timestamp> {
        self.end_timestamp
    }
}

/// Filtered, paginated order-query parameters.
///
/// Construct this request with [`OrderQuery::builder`]. Unlike
/// [`Client::search_open_orders`](crate::Client::search_open_orders), the v2
/// query can explicitly include [`OrderStatus::Suspended`] bracket children.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderQuery {
    filter: OrderFilter,
    #[serde(skip_serializing_if = "Option::is_none")]
    page_size: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    page_offset: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sort_by: Option<OrderSortBy>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sort_direction: Option<OrderSortDirection>,
    #[serde(skip_serializing_if = "Option::is_none")]
    include_total_count: Option<bool>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct OrderFilter {
    account_id: AccountId,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    statuses: Vec<OrderStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    contract_id: Option<ContractId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    created_after: Option<Timestamp>,
    #[serde(skip_serializing_if = "Option::is_none")]
    created_before: Option<Timestamp>,
}

impl OrderQuery {
    /// Starts a validated v2 order query for an account.
    pub fn builder(account_id: AccountId) -> OrderQueryBuilder {
        OrderQueryBuilder {
            account_id,
            statuses: Vec::new(),
            contract_id: None,
            created_after: None,
            created_before: None,
            page_size: None,
            page_offset: None,
            sort_by: None,
            sort_direction: None,
            include_total_count: None,
        }
    }

    /// Returns the provider account being queried.
    #[must_use]
    pub const fn account_id(&self) -> AccountId {
        self.filter.account_id
    }

    /// Borrows the requested lifecycle statuses.
    #[must_use]
    pub fn statuses(&self) -> &[OrderStatus] {
        &self.filter.statuses
    }

    /// Borrows the optional provider contract filter.
    #[must_use]
    pub const fn contract_id(&self) -> Option<&ContractId> {
        self.filter.contract_id.as_ref()
    }

    /// Returns the optional lower creation-time bound.
    #[must_use]
    pub const fn created_after(&self) -> Option<Timestamp> {
        self.filter.created_after
    }

    /// Returns the optional upper creation-time bound.
    #[must_use]
    pub const fn created_before(&self) -> Option<Timestamp> {
        self.filter.created_before
    }

    /// Returns the optional positive page size.
    #[must_use]
    pub const fn page_size(&self) -> Option<i32> {
        self.page_size
    }

    /// Returns the optional non-negative page offset.
    #[must_use]
    pub const fn page_offset(&self) -> Option<i32> {
        self.page_offset
    }

    /// Returns the optional sort field.
    #[must_use]
    pub const fn sort_by(&self) -> Option<OrderSortBy> {
        self.sort_by
    }

    /// Returns the optional sort direction.
    #[must_use]
    pub const fn sort_direction(&self) -> Option<OrderSortDirection> {
        self.sort_direction
    }

    /// Returns the optional total-count request flag.
    #[must_use]
    pub const fn include_total_count(&self) -> Option<bool> {
        self.include_total_count
    }
}

/// Builder for a validated [`OrderQuery`].
#[derive(Clone, Debug)]
#[must_use = "an OrderQueryBuilder does nothing until build is called"]
pub struct OrderQueryBuilder {
    account_id: AccountId,
    statuses: Vec<OrderStatus>,
    contract_id: Option<ContractId>,
    created_after: Option<Timestamp>,
    created_before: Option<Timestamp>,
    page_size: Option<i32>,
    page_offset: Option<i32>,
    sort_by: Option<OrderSortBy>,
    sort_direction: Option<OrderSortDirection>,
    include_total_count: Option<bool>,
}

impl OrderQueryBuilder {
    /// Replaces the lifecycle-status filter.
    pub fn statuses(mut self, statuses: impl IntoIterator<Item = OrderStatus>) -> Self {
        self.statuses = statuses.into_iter().collect();
        self
    }

    /// Restricts results to one provider contract.
    pub fn contract_id(mut self, contract_id: ContractId) -> Self {
        self.contract_id = Some(contract_id);
        self
    }

    /// Sets the lower creation-time bound.
    pub const fn created_after(mut self, created_after: Timestamp) -> Self {
        self.created_after = Some(created_after);
        self
    }

    /// Sets the upper creation-time bound.
    pub const fn created_before(mut self, created_before: Timestamp) -> Self {
        self.created_before = Some(created_before);
        self
    }

    /// Sets the positive number of orders requested per page.
    pub const fn page_size(mut self, page_size: i32) -> Self {
        self.page_size = Some(page_size);
        self
    }

    /// Sets the non-negative result offset.
    pub const fn page_offset(mut self, page_offset: i32) -> Self {
        self.page_offset = Some(page_offset);
        self
    }

    /// Selects the result sort field.
    pub const fn sort_by(mut self, sort_by: OrderSortBy) -> Self {
        self.sort_by = Some(sort_by);
        self
    }

    /// Selects the result sort direction.
    pub const fn sort_direction(mut self, sort_direction: OrderSortDirection) -> Self {
        self.sort_direction = Some(sort_direction);
        self
    }

    /// Selects whether the response should include a total matching count.
    pub const fn include_total_count(mut self, include: bool) -> Self {
        self.include_total_count = Some(include);
        self
    }

    /// Validates and builds the v2 order query.
    ///
    /// # Errors
    ///
    /// Returns an error for an unknown request-status code, a creation range
    /// that does not increase, a non-positive page size, or a negative page
    /// offset.
    pub fn build(self) -> Result<OrderQuery, RequestValidationError> {
        if let Some(code) = self.statuses.iter().find_map(|status| match status {
            OrderStatus::Unknown(code) => Some(*code),
            _ => None,
        }) {
            return Err(RequestValidationError::UnsupportedOrderStatus { code });
        }
        if self
            .created_after
            .zip(self.created_before)
            .is_some_and(|(after, before)| after >= before)
        {
            return Err(RequestValidationError::SearchRangeNotIncreasing);
        }
        if self.page_size.is_some_and(|size| size <= 0) {
            return Err(RequestValidationError::NonPositiveOrderPageSize);
        }
        if self.page_offset.is_some_and(|offset| offset < 0) {
            return Err(RequestValidationError::NegativeOrderPageOffset);
        }
        Ok(OrderQuery {
            filter: OrderFilter {
                account_id: self.account_id,
                statuses: self.statuses,
                contract_id: self.contract_id,
                created_after: self.created_after,
                created_before: self.created_before,
            },
            page_size: self.page_size,
            page_offset: self.page_offset,
            sort_by: self.sort_by,
            sort_direction: self.sort_direction,
            include_total_count: self.include_total_count,
        })
    }
}

/// One page returned by [`Client::query_orders`](crate::Client::query_orders).
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[non_exhaustive]
#[serde(rename_all = "camelCase")]
pub struct OrderPage {
    /// Orders in provider-selected page order.
    #[serde(default, deserialize_with = "null_to_empty")]
    pub orders: Vec<Order>,
    /// Total matching order count when requested and supplied by the provider.
    #[serde(default)]
    pub total_count: Option<i32>,
}

/// A `ProjectX` order.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[non_exhaustive]
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
    pub creation_timestamp: Timestamp,
    /// Provider update timestamp.
    pub update_timestamp: Timestamp,
    /// Provider order status.
    pub status: OrderStatus,
    /// Provider order type.
    #[serde(rename = "type")]
    pub order_type: OrderType,
    /// Order side.
    pub side: Side,
    /// Ordered quantity.
    pub size: i32,
    /// Optional limit price.
    #[serde(default, with = "crate::decimal_serde::option")]
    pub limit_price: Option<Decimal>,
    /// Optional stop price.
    #[serde(default, with = "crate::decimal_serde::option")]
    pub stop_price: Option<Decimal>,
    /// Optional cumulative filled quantity.
    #[serde(default)]
    pub fill_volume: Option<i32>,
    /// Optional average fill price.
    #[serde(default, with = "crate::decimal_serde::option")]
    pub filled_price: Option<Decimal>,
    /// Optional caller tag.
    #[serde(default)]
    pub custom_tag: Option<String>,
    /// Optional trailing distance in provider ticks.
    #[serde(default)]
    pub trail_distance: Option<i32>,
    /// Optional current trailing-stop price.
    #[serde(default, with = "crate::decimal_serde::option")]
    pub trail_price: Option<Decimal>,
    /// Parent order for a bracket child, when supplied.
    #[serde(default)]
    pub parent_order_id: Option<OrderId>,
    /// Provider-linked peer order, when supplied.
    #[serde(default)]
    pub linked_order_id: Option<OrderId>,
}

/// Validation failures while constructing a provider request.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum RequestValidationError {
    /// An order placement quantity was zero or negative.
    #[error("order size must be positive")]
    NonPositiveOrderSize,
    /// A replacement quantity was zero or negative.
    #[error("replacement order size must be positive")]
    NonPositiveReplacementSize,
    /// An order modification contained no replacement values.
    #[error("order modification requires at least one replacement value")]
    EmptyModification,
    /// A bracket distance was zero or negative.
    #[error("bracket ticks must be positive")]
    NonPositiveBracketTicks,
    /// An order request used an undocumented or unknown provider type code.
    #[error("unsupported order type code {code}")]
    UnsupportedOrderType {
        /// Unrecognized provider wire code.
        code: i32,
    },
    /// An order request used a provider side code unknown to this crate version.
    #[error("unsupported order side code {code}")]
    UnsupportedOrderSide {
        /// Unrecognized provider wire code.
        code: i32,
    },
    /// An order query used a provider status code unknown to this crate version.
    #[error("unsupported order status code {code}")]
    UnsupportedOrderStatus {
        /// Unrecognized provider wire code.
        code: i32,
    },
    /// A v2 order query requested a zero or negative page size.
    #[error("order-query page size must be positive")]
    NonPositiveOrderPageSize,
    /// A v2 order query requested a negative page offset.
    #[error("order-query page offset must not be negative")]
    NegativeOrderPageOffset,
    /// A historical-bar unit count was zero or negative.
    #[error("historical-bar unit number must be positive")]
    NonPositiveHistoryUnitNumber,
    /// A historical-bar limit exceeded the provider-supported range.
    #[error("historical-bar limit must be between 1 and 20,000")]
    HistoryLimitOutOfRange,
    /// A historical-bar range ended at or before its start.
    #[error("historical-bar end time must be later than its start time")]
    HistoryRangeNotIncreasing,
    /// An order or trade search ended at or before its start.
    #[error("search end time must be later than its start time")]
    SearchRangeNotIncreasing,
    /// A partial-close quantity was zero or negative.
    #[error("partial-close size must be positive")]
    NonPositivePartialCloseSize,
}

/// `ProjectX` bracket-leg configuration.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Bracket {
    /// Distance in provider ticks.
    ticks: i32,
    /// Bracket order type.
    #[serde(rename = "type")]
    order_type: OrderType,
}

impl Bracket {
    /// Creates a bracket leg with a positive distance in ticks.
    ///
    /// # Errors
    ///
    /// Returns an error when `ticks` is zero or negative, or when `order_type`
    /// is not documented by the provider for bracket requests.
    pub fn new(ticks: i32, order_type: OrderType) -> Result<Self, RequestValidationError> {
        if ticks <= 0 {
            return Err(RequestValidationError::NonPositiveBracketTicks);
        }
        validate_request_order_type(order_type)?;
        Ok(Self { ticks, order_type })
    }

    /// Returns the distance in provider ticks.
    #[must_use]
    pub const fn ticks(&self) -> i32 {
        self.ticks
    }

    /// Returns the bracket order type.
    #[must_use]
    pub const fn order_type(&self) -> OrderType {
        self.order_type
    }
}

/// Order placement parameters.
///
/// Construct this request with [`PlaceOrder::builder`], which prevents an
/// invalid non-positive quantity from reaching the transport.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PlaceOrder {
    /// Provider account.
    account_id: AccountId,
    /// Provider contract.
    contract_id: ContractId,
    /// Order type.
    #[serde(rename = "type")]
    order_type: OrderType,
    /// Order side.
    side: Side,
    /// Order quantity.
    size: i32,
    /// Optional limit price.
    #[serde(
        skip_serializing_if = "Option::is_none",
        with = "crate::decimal_serde::option"
    )]
    limit_price: Option<Decimal>,
    /// Optional stop price.
    #[serde(
        skip_serializing_if = "Option::is_none",
        with = "crate::decimal_serde::option"
    )]
    stop_price: Option<Decimal>,
    /// Optional trailing price.
    #[serde(
        skip_serializing_if = "Option::is_none",
        with = "crate::decimal_serde::option"
    )]
    trail_price: Option<Decimal>,
    /// Optional caller tag. It must be unique within the account.
    #[serde(skip_serializing_if = "Option::is_none")]
    custom_tag: Option<String>,
    /// Optional stop-loss bracket.
    #[serde(skip_serializing_if = "Option::is_none")]
    stop_loss_bracket: Option<Bracket>,
    /// Optional take-profit bracket.
    #[serde(skip_serializing_if = "Option::is_none")]
    take_profit_bracket: Option<Bracket>,
}

impl PlaceOrder {
    /// Starts a validated order-placement request.
    pub fn builder(
        account_id: AccountId,
        contract_id: ContractId,
        order_type: OrderType,
        side: Side,
        quantity: i32,
    ) -> PlaceOrderBuilder {
        PlaceOrderBuilder {
            account_id,
            contract_id,
            order_type,
            side,
            size: quantity,
            limit_price: None,
            stop_price: None,
            trail_price: None,
            custom_tag: None,
            stop_loss_bracket: None,
            take_profit_bracket: None,
        }
    }

    /// Returns the provider account.
    #[must_use]
    pub const fn account_id(&self) -> AccountId {
        self.account_id
    }

    /// Borrows the provider contract.
    #[must_use]
    pub const fn contract_id(&self) -> &ContractId {
        &self.contract_id
    }

    /// Returns the order type.
    #[must_use]
    pub const fn order_type(&self) -> OrderType {
        self.order_type
    }

    /// Returns the order side.
    #[must_use]
    pub const fn side(&self) -> Side {
        self.side
    }

    /// Returns the positive order quantity.
    #[must_use]
    pub const fn size(&self) -> i32 {
        self.size
    }

    /// Returns the optional limit price.
    #[must_use]
    pub const fn limit_price(&self) -> Option<Decimal> {
        self.limit_price
    }

    /// Returns the optional stop price.
    #[must_use]
    pub const fn stop_price(&self) -> Option<Decimal> {
        self.stop_price
    }

    /// Returns the optional trailing price.
    #[must_use]
    pub const fn trail_price(&self) -> Option<Decimal> {
        self.trail_price
    }

    /// Borrows the optional caller tag.
    #[must_use]
    pub fn custom_tag(&self) -> Option<&str> {
        self.custom_tag.as_deref()
    }

    /// Borrows the optional stop-loss bracket.
    #[must_use]
    pub const fn stop_loss_bracket(&self) -> Option<&Bracket> {
        self.stop_loss_bracket.as_ref()
    }

    /// Borrows the optional take-profit bracket.
    #[must_use]
    pub const fn take_profit_bracket(&self) -> Option<&Bracket> {
        self.take_profit_bracket.as_ref()
    }
}

/// Builder for a validated [`PlaceOrder`].
#[derive(Clone, Debug)]
#[must_use = "a PlaceOrderBuilder does nothing until build is called"]
pub struct PlaceOrderBuilder {
    account_id: AccountId,
    contract_id: ContractId,
    order_type: OrderType,
    side: Side,
    size: i32,
    limit_price: Option<Decimal>,
    stop_price: Option<Decimal>,
    trail_price: Option<Decimal>,
    custom_tag: Option<String>,
    stop_loss_bracket: Option<Bracket>,
    take_profit_bracket: Option<Bracket>,
}

impl PlaceOrderBuilder {
    /// Sets the optional limit price.
    pub const fn limit_price(mut self, limit_price: Decimal) -> Self {
        self.limit_price = Some(limit_price);
        self
    }

    /// Sets the optional stop price.
    pub const fn stop_price(mut self, stop_price: Decimal) -> Self {
        self.stop_price = Some(stop_price);
        self
    }

    /// Sets the optional trailing price.
    pub const fn trail_price(mut self, trail_price: Decimal) -> Self {
        self.trail_price = Some(trail_price);
        self
    }

    /// Sets the optional caller tag, which must be unique within the account.
    pub fn custom_tag(mut self, custom_tag: impl Into<String>) -> Self {
        self.custom_tag = Some(custom_tag.into());
        self
    }

    /// Sets the optional stop-loss bracket.
    pub fn stop_loss_bracket(mut self, stop_loss_bracket: Bracket) -> Self {
        self.stop_loss_bracket = Some(stop_loss_bracket);
        self
    }

    /// Sets the optional take-profit bracket.
    pub fn take_profit_bracket(mut self, take_profit_bracket: Bracket) -> Self {
        self.take_profit_bracket = Some(take_profit_bracket);
        self
    }

    /// Validates and builds the order-placement request.
    ///
    /// # Errors
    ///
    /// Returns an error when the order quantity is zero or negative, when its
    /// order type is undocumented for placement, or when its side code is
    /// unknown to this crate version.
    pub fn build(self) -> Result<PlaceOrder, RequestValidationError> {
        if self.size <= 0 {
            return Err(RequestValidationError::NonPositiveOrderSize);
        }
        validate_request_order_type(self.order_type)?;
        if let Side::Unknown(code) = self.side {
            return Err(RequestValidationError::UnsupportedOrderSide { code });
        }
        Ok(PlaceOrder {
            account_id: self.account_id,
            contract_id: self.contract_id,
            order_type: self.order_type,
            side: self.side,
            size: self.size,
            limit_price: self.limit_price,
            stop_price: self.stop_price,
            trail_price: self.trail_price,
            custom_tag: self.custom_tag,
            stop_loss_bracket: self.stop_loss_bracket,
            take_profit_bracket: self.take_profit_bracket,
        })
    }
}

fn validate_request_order_type(order_type: OrderType) -> Result<(), RequestValidationError> {
    match order_type {
        OrderType::Limit
        | OrderType::Market
        | OrderType::Stop
        | OrderType::TrailingStop
        | OrderType::JoinBid
        | OrderType::JoinAsk => Ok(()),
        OrderType::StopLimit => Err(RequestValidationError::UnsupportedOrderType { code: 3 }),
        OrderType::Unknown(code) => Err(RequestValidationError::UnsupportedOrderType { code }),
    }
}

/// Successful order-placement result.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
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
///
/// Construct this request with [`ModifyOrder::builder`], which requires at
/// least one replacement value and rejects non-positive replacement sizes.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ModifyOrder {
    /// Provider account.
    account_id: AccountId,
    /// Provider order.
    order_id: OrderId,
    /// Optional replacement quantity.
    #[serde(skip_serializing_if = "Option::is_none")]
    size: Option<i32>,
    /// Optional replacement limit price.
    #[serde(
        skip_serializing_if = "Option::is_none",
        with = "crate::decimal_serde::option"
    )]
    limit_price: Option<Decimal>,
    /// Optional replacement stop price.
    #[serde(
        skip_serializing_if = "Option::is_none",
        with = "crate::decimal_serde::option"
    )]
    stop_price: Option<Decimal>,
    /// Optional replacement trailing price.
    #[serde(
        skip_serializing_if = "Option::is_none",
        with = "crate::decimal_serde::option"
    )]
    trail_price: Option<Decimal>,
}

impl ModifyOrder {
    /// Starts a validated order-modification request.
    pub const fn builder(account_id: AccountId, order_id: OrderId) -> ModifyOrderBuilder {
        ModifyOrderBuilder {
            account_id,
            order_id,
            size: None,
            limit_price: None,
            stop_price: None,
            trail_price: None,
        }
    }

    /// Returns the provider account.
    #[must_use]
    pub const fn account_id(&self) -> AccountId {
        self.account_id
    }

    /// Returns the provider order.
    #[must_use]
    pub const fn order_id(&self) -> OrderId {
        self.order_id
    }

    /// Returns the optional positive replacement quantity.
    #[must_use]
    pub const fn size(&self) -> Option<i32> {
        self.size
    }

    /// Returns the optional replacement limit price.
    #[must_use]
    pub const fn limit_price(&self) -> Option<Decimal> {
        self.limit_price
    }

    /// Returns the optional replacement stop price.
    #[must_use]
    pub const fn stop_price(&self) -> Option<Decimal> {
        self.stop_price
    }

    /// Returns the optional replacement trailing price.
    #[must_use]
    pub const fn trail_price(&self) -> Option<Decimal> {
        self.trail_price
    }
}

/// Builder for a validated [`ModifyOrder`].
#[derive(Clone, Copy, Debug)]
#[must_use = "a ModifyOrderBuilder does nothing until build is called"]
pub struct ModifyOrderBuilder {
    account_id: AccountId,
    order_id: OrderId,
    size: Option<i32>,
    limit_price: Option<Decimal>,
    stop_price: Option<Decimal>,
    trail_price: Option<Decimal>,
}

impl ModifyOrderBuilder {
    /// Sets the replacement quantity.
    pub const fn size(mut self, size: i32) -> Self {
        self.size = Some(size);
        self
    }

    /// Sets the replacement limit price.
    pub const fn limit_price(mut self, limit_price: Decimal) -> Self {
        self.limit_price = Some(limit_price);
        self
    }

    /// Sets the replacement stop price.
    pub const fn stop_price(mut self, stop_price: Decimal) -> Self {
        self.stop_price = Some(stop_price);
        self
    }

    /// Sets the replacement trailing price.
    pub const fn trail_price(mut self, trail_price: Decimal) -> Self {
        self.trail_price = Some(trail_price);
        self
    }

    /// Validates and builds the order-modification request.
    ///
    /// # Errors
    ///
    /// Returns [`RequestValidationError::NonPositiveReplacementSize`] when a
    /// replacement quantity is zero or negative, or
    /// [`RequestValidationError::EmptyModification`] when no replacement value was
    /// supplied.
    pub fn build(self) -> Result<ModifyOrder, RequestValidationError> {
        if self.size.is_some_and(|size| size <= 0) {
            return Err(RequestValidationError::NonPositiveReplacementSize);
        }
        if self.size.is_none()
            && self.limit_price.is_none()
            && self.stop_price.is_none()
            && self.trail_price.is_none()
        {
            return Err(RequestValidationError::EmptyModification);
        }
        Ok(ModifyOrder {
            account_id: self.account_id,
            order_id: self.order_id,
            size: self.size,
            limit_price: self.limit_price,
            stop_price: self.stop_price,
            trail_price: self.trail_price,
        })
    }
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
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PartialCloseContract {
    /// Provider account.
    account_id: AccountId,
    /// Provider contract.
    contract_id: ContractId,
    /// Positive quantity to close.
    size: i32,
}

impl PartialCloseContract {
    /// Creates a partial-position close with a positive quantity.
    ///
    /// # Errors
    ///
    /// Returns [`RequestValidationError::NonPositivePartialCloseSize`] when
    /// `size` is zero or negative.
    pub fn new(
        account_id: AccountId,
        contract_id: ContractId,
        size: i32,
    ) -> Result<Self, RequestValidationError> {
        if size <= 0 {
            return Err(RequestValidationError::NonPositivePartialCloseSize);
        }
        Ok(Self {
            account_id,
            contract_id,
            size,
        })
    }

    /// Returns the provider account.
    #[must_use]
    pub const fn account_id(&self) -> AccountId {
        self.account_id
    }

    /// Borrows the provider contract.
    #[must_use]
    pub const fn contract_id(&self) -> &ContractId {
        &self.contract_id
    }

    /// Returns the positive quantity to close.
    #[must_use]
    pub const fn size(&self) -> i32 {
        self.size
    }
}

/// A `ProjectX` open position.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[non_exhaustive]
#[serde(rename_all = "camelCase")]
pub struct Position {
    /// Provider position identifier.
    pub id: PositionId,
    /// Provider account.
    pub account_id: AccountId,
    /// Provider contract.
    pub contract_id: ContractId,
    /// Provider contract display name, when supplied.
    #[serde(default)]
    pub contract_display_name: Option<String>,
    /// Provider creation timestamp.
    pub creation_timestamp: Timestamp,
    /// Provider position-type code.
    #[serde(rename = "type")]
    pub position_type: PositionType,
    /// Signed or directional provider quantity.
    pub size: i32,
    /// Average entry price.
    #[serde(with = "crate::decimal_serde")]
    pub average_price: Decimal,
}

/// Trade search parameters.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TradeSearch {
    /// Provider account.
    account_id: AccountId,
    /// Absolute range start.
    start_timestamp: Timestamp,
    /// Optional absolute range end.
    #[serde(skip_serializing_if = "Option::is_none")]
    end_timestamp: Option<Timestamp>,
}

impl TradeSearch {
    /// Creates a validated execution search.
    ///
    /// # Errors
    ///
    /// Returns [`RequestValidationError::SearchRangeNotIncreasing`] when an
    /// end timestamp is present and is not later than the start.
    pub fn new(
        account_id: AccountId,
        start_timestamp: Timestamp,
        end_timestamp: Option<Timestamp>,
    ) -> Result<Self, RequestValidationError> {
        validate_search_range(start_timestamp, end_timestamp)?;
        Ok(Self {
            account_id,
            start_timestamp,
            end_timestamp,
        })
    }

    /// Returns the provider account.
    #[must_use]
    pub const fn account_id(&self) -> AccountId {
        self.account_id
    }

    /// Returns the range start.
    #[must_use]
    pub const fn start_timestamp(&self) -> Timestamp {
        self.start_timestamp
    }

    /// Returns the optional range end.
    #[must_use]
    pub const fn end_timestamp(&self) -> Option<Timestamp> {
        self.end_timestamp
    }
}

fn validate_search_range(
    start_timestamp: Timestamp,
    end_timestamp: Option<Timestamp>,
) -> Result<(), RequestValidationError> {
    if end_timestamp.is_some_and(|end| end <= start_timestamp) {
        Err(RequestValidationError::SearchRangeNotIncreasing)
    } else {
        Ok(())
    }
}

/// A `ProjectX` execution trade.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[non_exhaustive]
#[serde(rename_all = "camelCase")]
pub struct Trade {
    /// Provider trade identifier.
    pub id: TradeId,
    /// Provider account.
    pub account_id: AccountId,
    /// Provider contract.
    pub contract_id: ContractId,
    /// Provider creation timestamp.
    pub creation_timestamp: Timestamp,
    /// Execution price.
    #[serde(with = "crate::decimal_serde")]
    pub price: Decimal,
    /// Optional realized P&L.
    #[serde(default, with = "crate::decimal_serde::option")]
    pub profit_and_loss: Option<Decimal>,
    /// Provider fees.
    #[serde(with = "crate::decimal_serde")]
    pub fees: Decimal,
    /// Optional provider commissions, separate from fees.
    #[serde(default, with = "crate::decimal_serde::option")]
    pub commissions: Option<Decimal>,
    /// Execution side.
    pub side: Side,
    /// Execution quantity.
    pub size: i32,
    /// Whether the provider voided this trade.
    pub voided: bool,
    /// Originating order.
    pub order_id: OrderId,
}

/// Consolidated quote from the market hub.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[non_exhaustive]
#[serde(rename_all = "camelCase")]
pub struct MarketQuote {
    /// Provider symbol identifier.
    #[serde(alias = "symbol")]
    pub raw_symbol: SymbolId,
    /// Human-readable symbol name, when supplied.
    #[serde(default)]
    pub symbol_name: Option<String>,
    /// Last trade price.
    #[serde(with = "crate::decimal_serde")]
    pub last_price: Decimal,
    /// Best bid price.
    #[serde(with = "crate::decimal_serde")]
    pub best_bid: Decimal,
    /// Best ask price.
    #[serde(with = "crate::decimal_serde")]
    pub best_ask: Decimal,
    /// Session price change.
    #[serde(with = "crate::decimal_serde")]
    pub change: Decimal,
    /// Session percent change.
    #[serde(with = "crate::decimal_serde")]
    pub change_percent: Decimal,
    /// Session open.
    #[serde(default, with = "crate::decimal_serde::option")]
    pub open: Option<Decimal>,
    /// Session high.
    #[serde(default, with = "crate::decimal_serde::option")]
    pub high: Option<Decimal>,
    /// Session low.
    #[serde(default, with = "crate::decimal_serde::option")]
    pub low: Option<Decimal>,
    /// Session cumulative volume.
    pub volume: i64,
    /// Provider last-updated timestamp.
    pub last_updated: Timestamp,
    /// Event timestamp.
    pub timestamp: Timestamp,
}

/// Depth-of-market update from the market hub.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[non_exhaustive]
#[serde(rename_all = "camelCase")]
pub struct MarketDepth {
    /// Provider symbol identifier, when supplied.
    #[serde(default, alias = "symbolId")]
    pub symbol_id: Option<SymbolId>,
    /// Event timestamp.
    pub timestamp: Timestamp,
    /// Provider depth event code.
    #[serde(rename = "type")]
    pub depth_type: DepthType,
    /// Price level.
    #[serde(with = "crate::decimal_serde")]
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
#[non_exhaustive]
#[serde(rename_all = "camelCase")]
pub struct MarketTrade {
    /// Provider symbol identifier.
    pub symbol_id: SymbolId,
    /// Trade price.
    #[serde(with = "crate::decimal_serde")]
    pub price: Decimal,
    /// Event timestamp.
    pub timestamp: Timestamp,
    /// Provider aggressor classification.
    #[serde(rename = "type")]
    pub trade_type: TradeLogType,
    /// Trade quantity.
    pub volume: i64,
}

/// Successful response for an operation without a result body.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct OperationResponse;

#[derive(Debug)]
pub(crate) enum Envelope<T> {
    Accepted(T),
    Rejected { error_code: i32 },
    InconsistentStatus { success: bool, error_code: i32 },
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

        let mut object = BTreeMap::<String, Box<RawValue>>::deserialize(deserializer)?;
        let success = object
            .remove("success")
            .ok_or_else(|| D::Error::custom("provider response success flag is missing"))
            .and_then(|value| serde_json::from_str(value.get()).map_err(D::Error::custom))?;
        let error_code = object
            .remove("errorCode")
            .ok_or_else(|| D::Error::custom("provider response error code is missing"))
            .and_then(|value| serde_json::from_str(value.get()).map_err(D::Error::custom))?;
        object.remove("errorMessage");
        if success != (error_code == 0) {
            return Ok(Self::InconsistentStatus {
                success,
                error_code,
            });
        }
        if !success {
            return Ok(Self::Rejected { error_code });
        }
        let mut body_json = String::from("{");
        for (index, (key, value)) in object.into_iter().enumerate() {
            if index > 0 {
                body_json.push(',');
            }
            body_json.push_str(&serde_json::to_string(&key).map_err(D::Error::custom)?);
            body_json.push(':');
            body_json.push_str(value.get());
        }
        body_json.push('}');
        let body = serde_json::from_str(&body_json).map_err(D::Error::custom)?;
        Ok(Self::Accepted(body))
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct AccountsBody {
    #[serde(default, deserialize_with = "null_to_empty")]
    pub(crate) accounts: Vec<Account>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ContractsBody {
    #[serde(default, deserialize_with = "null_to_empty")]
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
    #[serde(default, deserialize_with = "null_to_empty")]
    pub(crate) orders: Vec<Order>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PlaceOrderBody {
    pub(crate) order_id: Option<OrderId>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct PositionsBody {
    #[serde(default, deserialize_with = "null_to_empty")]
    pub(crate) positions: Vec<Position>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct TradesBody {
    #[serde(default, deserialize_with = "null_to_empty")]
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

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! assert_empty_list {
        ($body:ty, $field:ident, $json:literal) => {{
            let envelope: Envelope<$body> = serde_json::from_str($json)
                .unwrap_or_else(|error| panic!("fixture envelope must decode: {error}"));
            let Envelope::Accepted(body) = envelope else {
                panic!("fixture envelope must be accepted");
            };
            assert!(body.$field.is_empty());
        }};
    }

    #[test]
    fn optional_list_bodies_normalize_missing_and_null_to_empty() {
        assert_empty_list!(
            AccountsBody,
            accounts,
            r#"{"success":true,"errorCode":0,"accounts":null}"#
        );
        assert_empty_list!(
            ContractsBody,
            contracts,
            r#"{"success":true,"errorCode":0}"#
        );
        assert_empty_list!(
            OrdersBody,
            orders,
            r#"{"success":true,"errorCode":0,"orders":null}"#
        );
        assert_empty_list!(
            OrderPage,
            orders,
            r#"{"success":true,"errorCode":0,"orders":null}"#
        );
        assert_empty_list!(
            PositionsBody,
            positions,
            r#"{"success":true,"errorCode":0}"#
        );
        assert_empty_list!(
            TradesBody,
            trades,
            r#"{"success":true,"errorCode":0,"trades":null}"#
        );
    }

    #[test]
    fn rejected_envelope_does_not_require_an_endpoint_body() {
        let envelope: Envelope<AccountsBody> =
            serde_json::from_str(r#"{"success":false,"errorCode":17,"errorMessage":"synthetic"}"#)
                .unwrap_or_else(|error| panic!("rejection envelope must decode: {error}"));

        assert!(matches!(envelope, Envelope::Rejected { error_code: 17 }));
    }

    #[test]
    fn envelope_requires_a_consistent_provider_status() {
        assert!(
            serde_json::from_str::<Envelope<AccountsBody>>(r#"{"success":true,"accounts":[]}"#)
                .is_err()
        );
        for (json, success, error_code) in [
            (r#"{"success":true,"errorCode":17}"#, true, 17),
            (r#"{"success":false,"errorCode":0}"#, false, 0),
        ] {
            let envelope: Envelope<AccountsBody> = serde_json::from_str(json)
                .unwrap_or_else(|error| panic!("inconsistent envelope must decode: {error}"));
            assert!(matches!(
                envelope,
                Envelope::InconsistentStatus {
                    success: actual_success,
                    error_code: actual_error_code,
                } if actual_success == success && actual_error_code == error_code
            ));
        }
    }
}
