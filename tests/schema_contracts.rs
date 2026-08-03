// SPDX-FileCopyrightText: 2026 Kevin Monaghan
// SPDX-License-Identifier: MIT

//! Public provider-schema and validation contract tests.

use projectx_client::{
    AccountId, Bar, BarUnit, ContractId, Decimal, HistoryRequest, Order, OrderId, OrderQuery,
    OrderSearch, OrderSortBy, OrderSortDirection, OrderStatus, Position, PositionId,
    RequestValidationError, SymbolId, Timestamp, Trade, TradeId, TradeQuery, TradeSearch,
};
use serde_json::json;

fn timestamp(value: &str) -> Timestamp {
    Timestamp::new(value).unwrap_or_else(|error| panic!("fixture timestamp must be valid: {error}"))
}

fn account_id() -> AccountId {
    AccountId::new(42).unwrap_or_else(|error| panic!("fixture account must be valid: {error}"))
}

fn contract_id() -> ContractId {
    ContractId::new("CON.F.US.MNQ.M26")
        .unwrap_or_else(|error| panic!("fixture contract must be valid: {error}"))
}

#[test]
fn provider_timestamps_require_absolute_rfc3339_values() {
    for invalid in [
        "not-a-timestamp",
        "2026-01-01",
        "2026-01-01T00:00:00",
        "2026-13-01T00:00:00Z",
        "2026-01-01T24:00:00Z",
    ] {
        assert!(
            Timestamp::new(invalid).is_err(),
            "{invalid:?} must not be accepted as an absolute provider timestamp"
        );
        assert!(
            serde_json::from_value::<Timestamp>(json!(invalid)).is_err(),
            "{invalid:?} must not deserialize as an absolute provider timestamp"
        );
    }

    let absolute = timestamp("2026-01-01T08:30:15.250+08:00");
    assert_eq!(absolute.to_string(), "2026-01-01T00:30:15.25Z");
    assert_eq!(
        serde_json::to_value(absolute)
            .unwrap_or_else(|error| panic!("timestamp must serialize: {error}")),
        json!("2026-01-01T00:30:15.25Z")
    );
}

#[test]
fn history_and_search_ranges_must_increase() {
    let earlier = timestamp("2026-01-01T00:00:00Z");
    let later = timestamp("2026-01-02T00:00:00Z");

    for (start, end) in [(later, earlier), (earlier, earlier)] {
        assert_eq!(
            HistoryRequest::builder(contract_id(), false, start, end, BarUnit::Minute).build(),
            Err(RequestValidationError::HistoryRangeNotIncreasing)
        );
        assert_eq!(
            OrderSearch::new(account_id(), start, Some(end)),
            Err(RequestValidationError::SearchRangeNotIncreasing)
        );
        assert_eq!(
            TradeSearch::new(account_id(), start, Some(end)),
            Err(RequestValidationError::SearchRangeNotIncreasing)
        );
        assert_eq!(
            TradeQuery::builder(account_id())
                .start_timestamp(start)
                .end_timestamp(end)
                .build(),
            Err(RequestValidationError::SearchRangeNotIncreasing)
        );
    }

    let history = HistoryRequest::builder(
        contract_id(),
        false,
        timestamp("2026-01-01T08:00:00+08:00"),
        later,
        BarUnit::Minute,
    )
    .build()
    .unwrap_or_else(|error| panic!("increasing history range must be valid: {error}"));
    let history_json = serde_json::to_value(history)
        .unwrap_or_else(|error| panic!("history request must serialize: {error}"));
    assert_eq!(history_json["startTime"], json!("2026-01-01T00:00:00Z"));
    assert_eq!(history_json["endTime"], json!("2026-01-02T00:00:00Z"));
}

#[test]
fn trade_query_serializes_independently_optional_bounds() {
    let unbounded = TradeQuery::builder(account_id())
        .build()
        .unwrap_or_else(|error| panic!("unbounded trade query must build: {error}"));
    assert_eq!(
        serde_json::to_value(unbounded)
            .unwrap_or_else(|error| panic!("trade query must serialize: {error}")),
        json!({"accountId": 42})
    );

    let end = timestamp("2026-01-02T00:00:00Z");
    let end_only = TradeQuery::builder(account_id())
        .end_timestamp(end)
        .build()
        .unwrap_or_else(|error| panic!("end-only trade query must build: {error}"));
    assert_eq!(end_only.start_timestamp(), None);
    assert_eq!(end_only.end_timestamp(), Some(end));
    assert_eq!(
        serde_json::to_value(end_only)
            .unwrap_or_else(|error| panic!("trade query must serialize: {error}")),
        json!({
            "accountId": 42,
            "endTimestamp": "2026-01-02T00:00:00Z"
        })
    );
}

#[test]
fn v2_order_query_serializes_the_provider_filter_and_pagination_schema() {
    let query = OrderQuery::builder(account_id())
        .statuses([OrderStatus::Open, OrderStatus::Suspended])
        .contract_id(contract_id())
        .created_after(timestamp("2026-01-01T00:00:00Z"))
        .created_before(timestamp("2026-01-02T00:00:00Z"))
        .page_size(100)
        .page_offset(200)
        .sort_by(OrderSortBy::Id)
        .sort_direction(OrderSortDirection::Ascending)
        .include_total_count(true)
        .build()
        .unwrap_or_else(|error| panic!("valid v2 order query must build: {error}"));

    assert_eq!(query.account_id(), account_id());
    assert_eq!(
        query.statuses(),
        [OrderStatus::Open, OrderStatus::Suspended]
    );
    assert_eq!(query.contract_id(), Some(&contract_id()));
    assert_eq!(query.page_size(), Some(100));
    assert_eq!(query.page_offset(), Some(200));
    assert_eq!(query.sort_by(), Some(OrderSortBy::Id));
    assert_eq!(query.sort_direction(), Some(OrderSortDirection::Ascending));
    assert_eq!(query.include_total_count(), Some(true));
    assert_eq!(
        serde_json::to_value(query)
            .unwrap_or_else(|error| panic!("v2 order query must serialize: {error}")),
        json!({
            "filter": {
                "accountId": 42,
                "statuses": [1, 8],
                "contractId": "CON.F.US.MNQ.M26",
                "createdAfter": "2026-01-01T00:00:00Z",
                "createdBefore": "2026-01-02T00:00:00Z"
            },
            "pageSize": 100,
            "pageOffset": 200,
            "sortBy": 1,
            "sortDirection": 0,
            "includeTotalCount": true
        })
    );

    let minimal = OrderQuery::builder(account_id())
        .build()
        .unwrap_or_else(|error| panic!("minimal v2 order query must build: {error}"));
    assert_eq!(
        serde_json::to_value(minimal)
            .unwrap_or_else(|error| panic!("minimal v2 order query must serialize: {error}")),
        json!({"filter": {"accountId": 42}})
    );
}

#[test]
fn v2_order_query_rejects_invalid_filters_and_pagination() {
    let earlier = timestamp("2026-01-01T00:00:00Z");
    let later = timestamp("2026-01-02T00:00:00Z");

    assert_eq!(
        OrderQuery::builder(account_id())
            .statuses([OrderStatus::Unknown(99)])
            .build(),
        Err(RequestValidationError::UnsupportedOrderStatus { code: 99 })
    );
    for (after, before) in [(later, earlier), (earlier, earlier)] {
        assert_eq!(
            OrderQuery::builder(account_id())
                .created_after(after)
                .created_before(before)
                .build(),
            Err(RequestValidationError::SearchRangeNotIncreasing)
        );
    }
    for size in [0, -1] {
        assert_eq!(
            OrderQuery::builder(account_id()).page_size(size).build(),
            Err(RequestValidationError::NonPositiveOrderPageSize)
        );
    }
    assert_eq!(
        OrderQuery::builder(account_id()).page_offset(-1).build(),
        Err(RequestValidationError::NegativeOrderPageOffset)
    );
}

#[test]
fn provider_string_identifiers_reject_embedded_whitespace_and_controls() {
    for invalid in [
        "CON F.US.MNQ",
        "CON.F.US.\tMNQ",
        "CON.F.US.MNQ\n",
        "CON.F.US.\0MNQ",
        "CON.F.US.\u{2003}MNQ",
    ] {
        assert!(ContractId::new(invalid).is_err());
        assert!(SymbolId::new(invalid).is_err());
        assert!(serde_json::from_value::<ContractId>(json!(invalid)).is_err());
        assert!(serde_json::from_value::<SymbolId>(json!(invalid)).is_err());
    }

    assert_eq!(
        ContractId::new("CON.F.US.MNQ.M26")
            .unwrap_or_else(|error| panic!("provider contract must be valid: {error}"))
            .as_str(),
        "CON.F.US.MNQ.M26"
    );
}

#[test]
fn numeric_identifier_widths_follow_the_provider_schema() {
    let account: AccountId = serde_json::from_value(json!(i32::MAX))
        .unwrap_or_else(|error| panic!("the maximum int32 account ID must decode: {error}"));
    let position: PositionId = serde_json::from_value(json!(i32::MAX))
        .unwrap_or_else(|error| panic!("the maximum int32 position ID must decode: {error}"));
    let order: OrderId = serde_json::from_value(json!(i64::MAX))
        .unwrap_or_else(|error| panic!("the maximum int64 order ID must decode: {error}"));
    let trade: TradeId = serde_json::from_value(json!(i64::MAX))
        .unwrap_or_else(|error| panic!("the maximum int64 trade ID must decode: {error}"));

    assert_eq!(account.get(), i32::MAX);
    assert_eq!(position.get(), i32::MAX);
    assert_eq!(order.get(), i64::MAX);
    assert_eq!(trade.get(), i64::MAX);

    let above_i32 = i64::from(i32::MAX) + 1;
    assert!(serde_json::from_value::<AccountId>(json!(above_i32)).is_err());
    assert!(serde_json::from_value::<PositionId>(json!(above_i32)).is_err());
    assert!(serde_json::from_value::<AccountId>(json!(0)).is_err());
    assert!(serde_json::from_value::<PositionId>(json!(-1)).is_err());
    assert!(serde_json::from_str::<OrderId>("9223372036854775808").is_err());
    assert!(serde_json::from_str::<TradeId>("9223372036854775808").is_err());
}

#[test]
fn current_provider_response_fields_decode_exactly() {
    let bar: Bar = serde_json::from_value(json!({
        "t": "2026-01-01T08:00:00+08:00",
        "o": 100.125,
        "h": 101.25,
        "l": 99.75,
        "c": 100.50,
        "v": 12,
        "d": "2026-01-01",
        "k": 4_294_967_296_i64
    }))
    .unwrap_or_else(|error| panic!("current bar schema must decode: {error}"));
    assert_eq!(bar.t, timestamp("2026-01-01T00:00:00Z"));
    assert_eq!(
        bar.d.map(|date| date.to_string()).as_deref(),
        Some("2026-01-01")
    );
    assert_eq!(bar.k, Some(4_294_967_296));

    let order: Order = serde_json::from_value(json!({
        "id": 8_589_934_592_i64,
        "accountId": 42,
        "contractId": "CON.F.US.MNQ.M26",
        "symbolId": "F.US.MNQ",
        "creationTimestamp": "2026-01-01T00:00:00Z",
        "updateTimestamp": "2026-01-01T00:00:01Z",
        "status": 1,
        "type": 5,
        "side": 0,
        "size": 2,
        "limitPrice": null,
        "stopPrice": 99.75,
        "fillVolume": 1,
        "filledPrice": 100.125,
        "customTag": "schema-contract",
        "trailDistance": 8,
        "trailPrice": 100.25,
        "parentOrderId": 4_294_967_296_i64,
        "linkedOrderId": 4_294_967_297_i64
    }))
    .unwrap_or_else(|error| panic!("current order schema must decode: {error}"));
    assert_eq!(order.trail_distance, Some(8));
    assert_eq!(order.trail_price, Some(Decimal::new(10_025, 2)));
    assert_eq!(order.parent_order_id.map(OrderId::get), Some(4_294_967_296));
    assert_eq!(order.linked_order_id.map(OrderId::get), Some(4_294_967_297));

    let position: Position = serde_json::from_value(json!({
        "id": 21,
        "accountId": 42,
        "contractId": "CON.F.US.MNQ.M26",
        "contractDisplayName": "MNQM26",
        "creationTimestamp": "2026-01-01T00:00:00Z",
        "type": 1,
        "size": -2,
        "averagePrice": 100.125
    }))
    .unwrap_or_else(|error| panic!("current position schema must decode: {error}"));
    assert_eq!(position.contract_display_name.as_deref(), Some("MNQM26"));
    assert_eq!(position.size, -2);

    let trade: Trade = serde_json::from_value(json!({
        "id": 8_589_934_593_i64,
        "accountId": 42,
        "contractId": "CON.F.US.MNQ.M26",
        "creationTimestamp": "2026-01-01T00:00:01Z",
        "price": 100.125,
        "profitAndLoss": 5.25,
        "fees": 1.40,
        "commissions": 0.45,
        "side": 1,
        "size": 2,
        "voided": false,
        "orderId": 8_589_934_592_i64
    }))
    .unwrap_or_else(|error| panic!("current trade schema must decode: {error}"));
    assert_eq!(trade.commissions, Some(Decimal::new(45, 2)));
    assert_eq!(trade.size, 2);
}

#[test]
fn provider_models_reject_out_of_range_int32_values() {
    let above_i32 = i64::from(i32::MAX) + 1;
    let invalid_position = json!({
        "id": above_i32,
        "accountId": 42,
        "contractId": "CON.F.US.MNQ.M26",
        "creationTimestamp": "2026-01-01T00:00:00Z",
        "type": 1,
        "size": 1,
        "averagePrice": 100.00
    });
    assert!(serde_json::from_value::<Position>(invalid_position).is_err());

    let invalid_order = json!({
        "id": 84,
        "accountId": 42,
        "contractId": "CON.F.US.MNQ.M26",
        "creationTimestamp": "2026-01-01T00:00:00Z",
        "updateTimestamp": "2026-01-01T00:00:01Z",
        "status": 1,
        "type": 1,
        "side": 0,
        "size": above_i32
    });
    assert!(serde_json::from_value::<Order>(invalid_order).is_err());

    let invalid_trade = json!({
        "id": 63,
        "accountId": above_i32,
        "contractId": "CON.F.US.MNQ.M26",
        "creationTimestamp": "2026-01-01T00:00:01Z",
        "price": 100.25,
        "fees": 1.40,
        "side": 1,
        "size": 1,
        "voided": false,
        "orderId": 84
    });
    assert!(serde_json::from_value::<Trade>(invalid_trade).is_err());

    let invalid_bar_date = json!({
        "t": "2026-01-01T00:00:00Z",
        "o": 100,
        "h": 101,
        "l": 99,
        "c": 100,
        "v": 1,
        "d": "2026-02-30"
    });
    assert!(serde_json::from_value::<Bar>(invalid_bar_date).is_err());
}
