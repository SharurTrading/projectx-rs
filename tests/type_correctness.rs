// SPDX-FileCopyrightText: 2026 Kevin Monaghan
// SPDX-License-Identifier: MIT-0

//! Public-type correctness pins from the type audit (issue #88).
//!
//! These tests exercise only the public API. They pin the types that were
//! verified correct against the provider fixtures while the types are working
//! in testing: `Decimal` for every price, money, balance, fee, and P&L field
//! (PX-DECIMAL-01), validated identifier newtypes with provider integer
//! widths, forward-compatible `Unknown` enum codes, and `Option` only where
//! the provider may omit a field. They intentionally do not change any type.

use std::str::FromStr;

use projectx_client::{
    Account, Contract, Decimal, DepthType, MarketDepth, MarketQuote, MarketTrade, OrderStatus,
    OrderType, PositionType, ProviderDate, ProviderList, Side, Timestamp, TradeLogType,
};
use serde_json::json;

fn decimal(value: &str) -> Decimal {
    Decimal::from_str(value).unwrap_or_else(|error| panic!("fixture decimal must parse: {error}"))
}

fn assert_wire_code<T>(value: &T, code: i32)
where
    T: std::fmt::Debug + PartialEq + serde::Serialize + serde::de::DeserializeOwned,
{
    assert_eq!(
        serde_json::to_value(value)
            .unwrap_or_else(|error| panic!("provider code must encode: {error}")),
        json!(code)
    );
    assert_eq!(
        &serde_json::from_value::<T>(json!(code))
            .unwrap_or_else(|error| panic!("provider code must decode: {error}")),
        value
    );
}

#[test]
fn coded_enums_preserve_unknown_provider_codes() {
    // Future provider codes remain evidence across the full signed wire range.
    for code in [i32::MIN, -1, 99, i32::MAX] {
        assert_eq!(Side::Unknown(code).code(), code);
        assert_eq!(OrderType::Unknown(code).code(), code);
        assert_eq!(OrderStatus::Unknown(code).code(), code);
        assert_eq!(TradeLogType::Unknown(code).code(), code);
        assert_eq!(PositionType::Unknown(code).code(), code);
        assert_eq!(DepthType::UnknownCode(code).code(), code);

        assert_wire_code(&Side::Unknown(code), code);
        assert_wire_code(&OrderType::Unknown(code), code);
        assert_wire_code(&OrderStatus::Unknown(code), code);
        assert_wire_code(&TradeLogType::Unknown(code), code);
        assert_wire_code(&PositionType::Unknown(code), code);
        assert_wire_code(&DepthType::UnknownCode(code), code);
    }
}

#[test]
fn coded_enums_keep_documented_provider_wire_values() {
    macro_rules! assert_known_codes {
        ($($variant:path => $code:literal),+ $(,)?) => {
            $(
                assert_eq!($variant.code(), $code);
                assert_wire_code(&$variant, $code);
            )+
        };
    }

    assert_known_codes! {
        Side::Bid => 0,
        Side::Ask => 1,
        OrderType::Limit => 1,
        OrderType::Market => 2,
        OrderType::StopLimit => 3,
        OrderType::Stop => 4,
        OrderType::TrailingStop => 5,
        OrderType::JoinBid => 6,
        OrderType::JoinAsk => 7,
        OrderStatus::None => 0,
        OrderStatus::Open => 1,
        OrderStatus::Filled => 2,
        OrderStatus::Cancelled => 3,
        OrderStatus::Expired => 4,
        OrderStatus::Rejected => 5,
        OrderStatus::Pending => 6,
        OrderStatus::PendingCancellation => 7,
        OrderStatus::Suspended => 8,
        TradeLogType::Buy => 0,
        TradeLogType::Sell => 1,
        PositionType::Undefined => 0,
        PositionType::Long => 1,
        PositionType::Short => 2,
        DepthType::Unknown => 0,
        DepthType::Ask => 1,
        DepthType::Bid => 2,
        DepthType::BestAsk => 3,
        DepthType::BestBid => 4,
        DepthType::Trade => 5,
        DepthType::Reset => 6,
        DepthType::Low => 7,
        DepthType::High => 8,
        DepthType::NewBestBid => 9,
        DepthType::NewBestAsk => 10,
        DepthType::Fill => 11,
    }
}

#[test]
fn account_and_contract_money_fields_are_decimal() {
    let account: Account = serde_json::from_value(json!({
        "id": 42,
        "name": "SYNTHETIC",
        "balance": 1234.50,
        "canTrade": true,
        "isVisible": true
    }))
    .unwrap_or_else(|error| panic!("account must decode: {error}"));
    assert_eq!(account.balance, Some(decimal("1234.5")));
    assert_eq!(account.simulated, None, "omitted simulated stays absent");

    let sparse: Account = serde_json::from_value(json!({
        "id": 42,
        "name": "SYNTHETIC",
        "canTrade": true,
        "isVisible": true
    }))
    .unwrap_or_else(|error| panic!("sparse account must decode: {error}"));
    assert_eq!(
        sparse.balance, None,
        "omitted balance stays absent, not zero"
    );

    let contract: Contract = serde_json::from_value(json!({
        "id": "CON.F.US.MNQ.M26",
        "name": "MNQM26",
        "description": "Micro E-mini Nasdaq-100 Jun 2026",
        "tickSize": 0.25,
        "tickValue": 0.50,
        "activeContract": true,
        "symbolId": "F.US.MNQ"
    }))
    .unwrap_or_else(|error| panic!("contract must decode: {error}"));
    assert_eq!(contract.tick_size, decimal("0.25"));
    assert_eq!(contract.tick_value, decimal("0.5"));
    assert_eq!(contract.symbol_id.as_str(), "F.US.MNQ");
}

#[test]
fn market_data_prices_are_decimal_and_sparse_fields_stay_optional() {
    // Sparse quote: only the supplied fields are present; the rest stay None
    // rather than becoming a fabricated zero price or volume.
    let quote: MarketQuote = serde_json::from_value(json!({
        "symbol": "F.US.MNQ",
        "lastPrice": 20000.125,
        "lastUpdated": "2026-01-01T00:00:00Z"
    }))
    .unwrap_or_else(|error| panic!("sparse quote must decode: {error}"));
    assert_eq!(quote.raw_symbol.as_str(), "F.US.MNQ");
    assert_eq!(quote.last_price, Some(decimal("20000.125")));
    assert_eq!(quote.best_bid, None);
    assert_eq!(quote.best_ask, None);
    assert_eq!(quote.volume, None);

    let depth: MarketDepth = serde_json::from_value(json!({
        "symbolId": "F.US.MNQ",
        "timestamp": "2026-01-01T00:00:00Z",
        "type": 2,
        "price": 19999.75,
        "volume": 3,
        "currentVolume": 10
    }))
    .unwrap_or_else(|error| panic!("depth must decode: {error}"));
    assert_eq!(depth.price, decimal("19999.75"));
    assert_eq!(depth.volume, 3);
    assert_eq!(depth.current_volume, 10);
    assert_eq!(depth.index, None);

    let trade: MarketTrade = serde_json::from_value(json!({
        "symbolId": "F.US.MNQ",
        "price": 20000.25,
        "timestamp": "2026-01-01T00:00:00Z",
        "type": 0,
        "volume": 2
    }))
    .unwrap_or_else(|error| panic!("market trade must decode: {error}"));
    assert_eq!(trade.price, decimal("20000.25"));
    assert_eq!(trade.volume, 2);
    assert_eq!(trade.trade_type, TradeLogType::Buy);
}

#[test]
fn timestamps_and_provider_dates_reject_invalid_values() {
    assert!(Timestamp::new("2026-01-01").is_err());
    assert!(Timestamp::new("2026-01-01T00:00:00").is_err());
    assert_eq!(
        Timestamp::new("2026-01-01T08:00:00+08:00")
            .unwrap_or_else(|error| panic!("absolute timestamp must parse: {error}"))
            .to_string(),
        "2026-01-01T00:00:00Z"
    );
    assert!(ProviderDate::new("2026-02-30").is_err());
    assert_eq!(
        ProviderDate::new("2026-02-28")
            .unwrap_or_else(|error| panic!("civil date must parse: {error}"))
            .to_string(),
        "2026-02-28"
    );
}

#[test]
fn provider_list_keeps_explicit_empty_distinct_from_absent() {
    let listed: ProviderList<u8> = ProviderList::Listed(Vec::new());
    assert!(listed.is_listed());
    assert!(listed.is_explicitly_empty());
    assert_eq!(listed.into_listed(), Some(Vec::new()));

    let absent: ProviderList<u8> = ProviderList::Absent;
    assert!(!absent.is_listed());
    assert!(!absent.is_explicitly_empty());
    assert_eq!(absent.into_listed(), None);
}
