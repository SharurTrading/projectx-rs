// SPDX-FileCopyrightText: 2026 Kevin Monaghan
// SPDX-License-Identifier: MIT

//! Public REST endpoint contract tests using synthetic local fixtures.

use httpmock::{Mock, prelude::*};
use projectx_client::{
    AccountId, BarUnit, Bracket, CancelOrder, Client, CloseContract, ContractId, Credentials,
    Decimal, Endpoints, HistoryRequest, ModifyOrder, OrderId, OrderQuery, OrderSearch, OrderSortBy,
    OrderSortDirection, OrderStatus, OrderType, PartialCloseContract, PlaceOrder, SearchContracts,
    Side, Timestamp, TradeQuery, TradeSearch,
};
use serde_json::json;

fn fixture_client(server: &MockServer) -> Client {
    let credentials = Credentials::new("synthetic-user", "synthetic-key")
        .unwrap_or_else(|error| panic!("fixture credentials must be valid: {error}"));
    let endpoints = Endpoints::custom(&server.base_url(), &server.base_url())
        .unwrap_or_else(|error| panic!("fixture URL must be valid: {error}"));
    Client::builder(credentials)
        .endpoints(endpoints)
        .max_retries(0)
        .build()
        .unwrap_or_else(|error| panic!("fixture client must build: {error}"))
}

async fn authenticated_client(server: &MockServer) -> Client {
    let _login = server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/api/Auth/loginKey")
                .json_body(json!({
                    "userName": "synthetic-user",
                    "apiKey": "synthetic-key"
                }));
            then.status(200)
                .json_body(json!({"success": true, "errorCode": 0, "token": "synthetic-token"}));
        })
        .await;
    let client = fixture_client(server);
    client
        .authenticate()
        .await
        .unwrap_or_else(|error| panic!("fixture login must succeed: {error}"));
    client
}

fn account_id() -> AccountId {
    AccountId::new(42).unwrap_or_else(|error| panic!("fixture account must be valid: {error}"))
}

fn contract_id() -> ContractId {
    ContractId::new("CON.F.US.MNQ.M26")
        .unwrap_or_else(|error| panic!("fixture contract must be valid: {error}"))
}

fn order_id() -> OrderId {
    OrderId::new(84).unwrap_or_else(|error| panic!("fixture order must be valid: {error}"))
}

#[tokio::test]
async fn contract_and_history_endpoints_match_provider_contracts() {
    let server = MockServer::start_async().await;
    let available = available_contracts_mock(&server).await;
    let search = contract_search_mock(&server).await;
    let by_id = contract_by_id_mock(&server).await;
    let history = history_mock(&server).await;

    let client = authenticated_client(&server).await;
    let contracts = client
        .available_contracts(false)
        .await
        .unwrap_or_else(|error| panic!("available contracts must succeed: {error}"));
    let searched = client
        .search_contracts(&SearchContracts {
            live: false,
            search_text: "MNQ".to_owned(),
        })
        .await
        .unwrap_or_else(|error| panic!("contract search must succeed: {error}"));
    let contract = client
        .contract_by_id(&contract_id())
        .await
        .unwrap_or_else(|error| panic!("contract lookup must succeed: {error}"));
    let history_request = HistoryRequest::builder(
        contract_id(),
        false,
        timestamp("2026-01-01T00:00:00Z"),
        timestamp("2026-01-02T00:00:00Z"),
        BarUnit::Minute,
    )
    .limit(100)
    .build()
    .unwrap_or_else(|error| panic!("fixture history request must be valid: {error}"));
    let bars = client
        .retrieve_bars(&history_request)
        .await
        .unwrap_or_else(|error| panic!("history must succeed: {error}"));

    available.assert_async().await;
    search.assert_async().await;
    by_id.assert_async().await;
    history.assert_async().await;
    assert_eq!(contracts, searched);
    assert_eq!(contract, contracts[0]);
    assert_eq!(bars[0].c, Decimal::new(10_025, 2));
    assert_eq!(
        bars[0].d.map(|date| date.to_string()).as_deref(),
        Some("2026-01-01")
    );
    assert_eq!(bars[0].k, Some(4_294_967_296));
}

#[tokio::test]
async fn order_endpoints_use_typed_exact_requests() {
    let server = MockServer::start_async().await;
    let _: projectx_client::Order = serde_json::from_value(order_json())
        .unwrap_or_else(|error| panic!("fixture order must decode: {error}"));
    let search = order_search_mock(&server).await;
    let by_id = order_by_id_mock(&server).await;
    let open = open_order_mock(&server).await;
    let place = place_order_mock(&server).await;
    let cancel = operation_mock(
        &server,
        "/api/Order/cancel",
        json!({"accountId": 42, "orderId": 84}),
    )
    .await;
    let modify = operation_mock(
        &server,
        "/api/Order/modify",
        json!({
            "accountId": 42,
            "orderId": 84,
            "size": 2,
            "limitPrice": 100.25
        }),
    )
    .await;

    let client = authenticated_client(&server).await;
    let orders = client
        .search_orders(
            &OrderSearch::new(account_id(), timestamp("2026-01-01T00:00:00Z"), None)
                .unwrap_or_else(|error| panic!("fixture order search must be valid: {error}")),
        )
        .await
        .unwrap_or_else(|error| panic!("order search must succeed: {error:?}"));
    let order = client
        .order_by_id(account_id(), order_id())
        .await
        .unwrap_or_else(|error| panic!("order lookup must succeed: {error}"));
    let open_orders = client
        .search_open_orders(account_id())
        .await
        .unwrap_or_else(|error| panic!("open-order search must succeed: {error}"));
    let stop_loss = Bracket::new(4, OrderType::Stop)
        .unwrap_or_else(|error| panic!("fixture bracket must be valid: {error}"));
    let place_request =
        PlaceOrder::builder(account_id(), contract_id(), OrderType::Limit, Side::Bid, 1)
            .limit_price(Decimal::new(10_025, 2))
            .custom_tag("synthetic-order")
            .stop_loss_bracket(stop_loss)
            .build()
            .unwrap_or_else(|error| panic!("fixture order must be valid: {error}"));
    let placed = client
        .place_order(&place_request)
        .await
        .unwrap_or_else(|error| panic!("order placement must succeed: {error}"));
    client
        .cancel_order(&CancelOrder {
            account_id: account_id(),
            order_id: order_id(),
        })
        .await
        .unwrap_or_else(|error| panic!("order cancellation must succeed: {error}"));
    let modify_request = ModifyOrder::builder(account_id(), order_id())
        .size(2)
        .limit_price(Decimal::new(10_025, 2))
        .build()
        .unwrap_or_else(|error| panic!("fixture modification must be valid: {error}"));
    client
        .modify_order(&modify_request)
        .await
        .unwrap_or_else(|error| panic!("order modification must succeed: {error}"));

    search.assert_async().await;
    by_id.assert_async().await;
    open.assert_async().await;
    place.assert_async().await;
    cancel.assert_async().await;
    modify.assert_async().await;
    assert_eq!(orders, open_orders);
    assert_eq!(order, orders[0]);
    assert_eq!(orders[0].status, OrderStatus::Open);
    assert_eq!(orders[0].trail_distance, Some(8));
    assert_eq!(orders[0].trail_price, Some(Decimal::new(10_025, 2)));
    assert_eq!(
        orders[0].parent_order_id.map(OrderId::get),
        Some(4_294_967_296)
    );
    assert_eq!(
        orders[0].linked_order_id.map(OrderId::get),
        Some(4_294_967_297)
    );
    assert_eq!(placed.order_id, order_id());
}

#[tokio::test]
async fn v2_order_query_supports_complete_working_order_reconciliation() {
    let server = MockServer::start_async().await;
    let query_mock = order_query_mock(&server).await;
    let client = authenticated_client(&server).await;
    let query = OrderQuery::builder(account_id())
        .statuses([
            OrderStatus::Open,
            OrderStatus::Pending,
            OrderStatus::PendingCancellation,
            OrderStatus::Suspended,
        ])
        .contract_id(contract_id())
        .created_after(timestamp("2026-01-01T00:00:00Z"))
        .created_before(timestamp("2026-01-02T00:00:00Z"))
        .page_size(50)
        .page_offset(0)
        .sort_by(OrderSortBy::CreatedAt)
        .sort_direction(OrderSortDirection::Descending)
        .include_total_count(true)
        .build()
        .unwrap_or_else(|error| panic!("fixture v2 order query must be valid: {error}"));

    let page = client
        .query_orders(&query)
        .await
        .unwrap_or_else(|error| panic!("v2 order query must succeed: {error}"));

    query_mock.assert_async().await;
    assert_eq!(page.total_count, Some(2));
    assert_eq!(page.orders.len(), 2);
    assert_eq!(page.orders[0].status, OrderStatus::Open);
    assert_eq!(page.orders[1].status, OrderStatus::Suspended);
}

#[tokio::test]
async fn position_and_trade_endpoints_match_provider_contracts() {
    let server = MockServer::start_async().await;
    let positions = position_search_mock(&server).await;
    let close = operation_mock(
        &server,
        "/api/Position/closeContract",
        json!({"accountId": 42, "contractId": "CON.F.US.MNQ.M26"}),
    )
    .await;
    let partial = operation_mock(
        &server,
        "/api/Position/partialCloseContract",
        json!({"accountId": 42, "contractId": "CON.F.US.MNQ.M26", "size": 1}),
    )
    .await;
    let trades = trade_search_mock(&server).await;
    let all_trades = all_trades_query_mock(&server).await;
    let ending_trades = ending_trades_query_mock(&server).await;

    let client = authenticated_client(&server).await;
    let open_positions = client
        .search_open_positions(account_id())
        .await
        .unwrap_or_else(|error| panic!("position search must succeed: {error}"));
    client
        .close_contract(&CloseContract {
            account_id: account_id(),
            contract_id: contract_id(),
        })
        .await
        .unwrap_or_else(|error| panic!("position close must succeed: {error}"));
    let partial_close = PartialCloseContract::new(account_id(), contract_id(), 1)
        .unwrap_or_else(|error| panic!("fixture partial close must be valid: {error}"));
    client
        .partial_close_contract(&partial_close)
        .await
        .unwrap_or_else(|error| panic!("partial close must succeed: {error}"));
    let executions = client
        .search_trades(
            &TradeSearch::new(account_id(), timestamp("2026-01-01T00:00:00Z"), None)
                .unwrap_or_else(|error| panic!("fixture trade search must be valid: {error}")),
        )
        .await
        .unwrap_or_else(|error| panic!("trade search must succeed: {error}"));
    let all_query = TradeQuery::builder(account_id())
        .build()
        .unwrap_or_else(|error| panic!("unbounded trade query must be valid: {error}"));
    let all_executions = client
        .query_trades(&all_query)
        .await
        .unwrap_or_else(|error| panic!("unbounded trade query must succeed: {error}"));
    let ending_query = TradeQuery::builder(account_id())
        .end_timestamp(timestamp("2026-01-02T00:00:00Z"))
        .build()
        .unwrap_or_else(|error| panic!("end-only trade query must be valid: {error}"));
    let ending_executions = client
        .query_trades(&ending_query)
        .await
        .unwrap_or_else(|error| panic!("end-only trade query must succeed: {error}"));

    positions.assert_async().await;
    close.assert_async().await;
    partial.assert_async().await;
    trades.assert_async().await;
    all_trades.assert_async().await;
    ending_trades.assert_async().await;
    assert_eq!(open_positions[0].average_price, Decimal::new(10_000, 2));
    assert_eq!(
        open_positions[0].contract_display_name.as_deref(),
        Some("MNQM26")
    );
    assert_eq!(executions[0].fees, Decimal::new(140, 2));
    assert_eq!(executions[0].commissions, Some(Decimal::new(45, 2)));
    assert_eq!(all_executions, executions);
    assert_eq!(ending_executions, executions);
}

fn contract_json() -> serde_json::Value {
    json!({
        "id": "CON.F.US.MNQ.M26",
        "name": "MNQM26",
        "description": "Synthetic micro contract",
        "tickSize": 0.25,
        "tickValue": 0.50,
        "activeContract": true,
        "symbolId": "F.US.MNQ"
    })
}

fn timestamp(value: &str) -> Timestamp {
    Timestamp::new(value).unwrap_or_else(|error| panic!("fixture timestamp must be valid: {error}"))
}

async fn available_contracts_mock(server: &MockServer) -> Mock<'_> {
    server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/api/Contract/available")
                .json_body(json!({"live": false}));
            then.status(200).json_body(json!({
                "contracts": [contract_json()],
                "success": true,
                "errorCode": 0
            }));
        })
        .await
}

async fn contract_search_mock(server: &MockServer) -> Mock<'_> {
    server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/api/Contract/search")
                .json_body(json!({"live": false, "searchText": "MNQ"}));
            then.status(200).json_body(json!({
                "contracts": [contract_json()],
                "success": true,
                "errorCode": 0
            }));
        })
        .await
}

async fn contract_by_id_mock(server: &MockServer) -> Mock<'_> {
    server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/api/Contract/searchById")
                .json_body(json!({"contractId": "CON.F.US.MNQ.M26"}));
            then.status(200)
                .json_body(json!({"contract": contract_json(), "success": true, "errorCode": 0}));
        })
        .await
}

async fn history_mock(server: &MockServer) -> Mock<'_> {
    server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/api/History/retrieveBars")
                .json_body(json!({
                    "contractId": "CON.F.US.MNQ.M26",
                    "live": false,
                    "startTime": "2026-01-01T00:00:00Z",
                    "endTime": "2026-01-02T00:00:00Z",
                    "unit": 2,
                    "unitNumber": 1,
                    "limit": 100,
                    "includePartialBar": false
                }));
            then.status(200).json_body(json!({
                "bars": [{
                    "t": "2026-01-01T00:00:00Z",
                    "o": 100.10,
                    "h": 101.20,
                    "l": 99.90,
                    "c": 100.25,
                    "v": 12,
                    "d": "2026-01-01",
                    "k": 4_294_967_296_i64
                }],
                "success": true,
                "errorCode": 0
            }));
        })
        .await
}

async fn operation_mock<'a>(
    server: &'a MockServer,
    path: &'a str,
    body: serde_json::Value,
) -> Mock<'a> {
    server
        .mock_async(move |when, then| {
            when.method(POST).path(path).json_body(body);
            then.status(200)
                .json_body(json!({"success": true, "errorCode": 0}));
        })
        .await
}

async fn order_search_mock(server: &MockServer) -> Mock<'_> {
    server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/api/Order/search")
                .json_body(json!({
                    "accountId": 42,
                    "startTimestamp": "2026-01-01T00:00:00Z"
                }));
            then.status(200)
                .json_body(json!({"orders": [order_json()], "success": true, "errorCode": 0}));
        })
        .await
}

async fn open_order_mock(server: &MockServer) -> Mock<'_> {
    server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/api/Order/searchOpen")
                .json_body(json!({"accountId": 42}));
            then.status(200)
                .json_body(json!({"orders": [order_json()], "success": true, "errorCode": 0}));
        })
        .await
}

async fn order_by_id_mock(server: &MockServer) -> Mock<'_> {
    server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/api/Order/searchById")
                .json_body(json!({"accountId": 42, "orderId": 84}));
            then.status(200).json_body(json!({
                "order": order_json(),
                "success": true,
                "errorCode": 0
            }));
        })
        .await
}

async fn order_query_mock(server: &MockServer) -> Mock<'_> {
    server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/api/Order/v2/query")
                .json_body(json!({
                    "filter": {
                        "accountId": 42,
                        "statuses": [1, 6, 7, 8],
                        "contractId": "CON.F.US.MNQ.M26",
                        "createdAfter": "2026-01-01T00:00:00Z",
                        "createdBefore": "2026-01-02T00:00:00Z"
                    },
                    "pageSize": 50,
                    "pageOffset": 0,
                    "sortBy": 0,
                    "sortDirection": 1,
                    "includeTotalCount": true
                }));
            then.status(200).json_body(json!({
                "orders": [order_json(), suspended_order_json()],
                "totalCount": 2,
                "success": true,
                "errorCode": 0
            }));
        })
        .await
}

async fn place_order_mock(server: &MockServer) -> Mock<'_> {
    server
        .mock_async(|when, then| {
            when.method(POST).path("/api/Order/place").json_body(json!({
                "accountId": 42,
                "contractId": "CON.F.US.MNQ.M26",
                "type": 1,
                "side": 0,
                "size": 1,
                "limitPrice": 100.25,
                "customTag": "synthetic-order",
                "stopLossBracket": {"ticks": 4, "type": 4}
            }));
            then.status(200)
                .json_body(json!({"orderId": 84, "success": true, "errorCode": 0}));
        })
        .await
}

fn order_json() -> serde_json::Value {
    json!({
        "id": 84,
        "accountId": 42,
        "contractId": "CON.F.US.MNQ.M26",
        "symbolId": "F.US.MNQ",
        "creationTimestamp": "2026-01-01T00:00:00Z",
        "updateTimestamp": "2026-01-01T00:00:01Z",
        "status": 1,
        "type": 1,
        "side": 0,
        "size": 1,
        "limitPrice": 100.25,
        "stopPrice": null,
        "fillVolume": 0,
        "filledPrice": null,
        "customTag": "synthetic-order",
        "trailDistance": 8,
        "trailPrice": 100.25,
        "parentOrderId": 4_294_967_296_i64,
        "linkedOrderId": 4_294_967_297_i64
    })
}

fn suspended_order_json() -> serde_json::Value {
    let mut order = order_json();
    order["id"] = json!(85);
    order["status"] = json!(8);
    order
}

async fn position_search_mock(server: &MockServer) -> Mock<'_> {
    server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/api/Position/searchOpen")
                .json_body(json!({"accountId": 42}));
            then.status(200).json_body(json!({
                "positions": [{
                    "id": 21,
                    "accountId": 42,
                    "contractId": "CON.F.US.MNQ.M26",
                    "contractDisplayName": "MNQM26",
                    "creationTimestamp": "2026-01-01T00:00:00Z",
                    "type": 1,
                    "size": 2,
                    "averagePrice": 100.00
                }],
                "success": true,
                "errorCode": 0
            }));
        })
        .await
}

async fn trade_search_mock(server: &MockServer) -> Mock<'_> {
    server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/api/Trade/search")
                .json_body(json!({
                    "accountId": 42,
                    "startTimestamp": "2026-01-01T00:00:00Z"
                }));
            then.status(200).json_body(json!({
                "trades": [trade_json()],
                "success": true,
                "errorCode": 0
            }));
        })
        .await
}

async fn all_trades_query_mock(server: &MockServer) -> Mock<'_> {
    server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/api/Trade/search")
                .json_body(json!({"accountId": 42}));
            then.status(200).json_body(json!({
                "trades": [trade_json()],
                "success": true,
                "errorCode": 0
            }));
        })
        .await
}

async fn ending_trades_query_mock(server: &MockServer) -> Mock<'_> {
    server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/api/Trade/search")
                .json_body(json!({
                    "accountId": 42,
                    "endTimestamp": "2026-01-02T00:00:00Z"
                }));
            then.status(200).json_body(json!({
                "trades": [trade_json()],
                "success": true,
                "errorCode": 0
            }));
        })
        .await
}

fn trade_json() -> serde_json::Value {
    json!({
        "id": 63,
        "accountId": 42,
        "contractId": "CON.F.US.MNQ.M26",
        "creationTimestamp": "2026-01-01T00:00:01Z",
        "price": 100.25,
        "profitAndLoss": 5.00,
        "fees": 1.40,
        "commissions": 0.45,
        "side": 1,
        "size": 1,
        "voided": false,
        "orderId": 84
    })
}
