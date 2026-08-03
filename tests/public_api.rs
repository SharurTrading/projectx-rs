// SPDX-FileCopyrightText: 2026 Kevin Monaghan
// SPDX-License-Identifier: MIT

//! Public-API contract tests using synthetic local fixtures.

use std::time::Duration;

use httpmock::prelude::*;
use projectx_client::{
    AccountId, ApplicationCredentials, BarUnit, Bracket, Client, ContractId, Credentials, Decimal,
    Endpoints, Error, HistoryRequest, ModifyOrder, OrderId, OrderStatus, OrderType,
    PartialCloseContract, PlaceOrder, RequestValidationError, Side, Timestamp, TradeLogType,
};
use serde_json::json;

fn fixture_client(server: &MockServer) -> Client {
    let credentials = Credentials::new("synthetic-user", "synthetic-key")
        .unwrap_or_else(|error| panic!("synthetic credentials must be valid: {error}"));
    let endpoints = Endpoints::custom(&server.base_url(), &server.base_url())
        .unwrap_or_else(|error| panic!("fixture URL must be valid: {error}"));
    Client::builder(credentials)
        .endpoints(endpoints)
        .build()
        .unwrap_or_else(|error| panic!("fixture client must build: {error}"))
}

#[tokio::test]
async fn authenticates_then_searches_active_accounts() {
    let server = MockServer::start_async().await;
    let login = server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/api/Auth/loginKey")
                .header("accept", "text/plain")
                .json_body(json!({
                    "userName": "synthetic-user",
                    "apiKey": "synthetic-key"
                }));
            then.status(200).json_body(json!({
                "success": true,
                "errorCode": 0,
                "token": "synthetic-token"
            }));
        })
        .await;
    let accounts = server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/api/Account/search")
                .header("authorization", "Bearer synthetic-token")
                .json_body(json!({"onlyActiveAccounts": true}));
            then.status(200).json_body(json!({
                "accounts": [{
                    "id": 42,
                    "name": "SYNTHETIC",
                    "balance": 1234.50,
                    "canTrade": true,
                    "isVisible": true,
                    "simulated": true
                }],
                "success": true,
                "errorCode": 0,
                "errorMessage": null
            }));
        })
        .await;
    let all_accounts = server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/api/Account/search")
                .header("authorization", "Bearer synthetic-token")
                .json_body(json!({"onlyActiveAccounts": false}));
            then.status(200).json_body(json!({
                "accounts": [],
                "success": true,
                "errorCode": 0
            }));
        })
        .await;

    let client = fixture_client(&server);
    client
        .authenticate()
        .await
        .unwrap_or_else(|error| panic!("fixture login must succeed: {error}"));
    let result = client
        .search_active_accounts()
        .await
        .unwrap_or_else(|error| panic!("fixture account search must succeed: {error}"));
    let all = client
        .search_accounts(false)
        .await
        .unwrap_or_else(|error| panic!("fixture all-account search must succeed: {error}"));

    login.assert_async().await;
    accounts.assert_async().await;
    all_accounts.assert_async().await;
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].id.get(), 42);
    assert_eq!(result[0].balance, Some(Decimal::new(123_450, 2)));
    assert!(all.is_empty());
}

#[tokio::test]
async fn application_authentication_ping_and_logout_match_provider_contracts() {
    let server = MockServer::start_async().await;
    let ping = server
        .mock_async(|when, then| {
            when.method(GET).path("/api/Status/ping");
            then.status(200).body("pong");
        })
        .await;
    let login = server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/api/Auth/loginApp")
                .json_body(json!({
                    "userName": "synthetic-app-user",
                    "password": "synthetic-password",
                    "deviceId": "synthetic-device",
                    "appId": "synthetic-app",
                    "verifyKey": "synthetic-verify-key"
                }));
            then.status(200).json_body(json!({
                "success": true,
                "errorCode": 0,
                "token": "synthetic-app-token"
            }));
        })
        .await;
    let logout = server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/api/Auth/logout")
                .header("authorization", "Bearer synthetic-app-token")
                .body("");
            then.status(200)
                .json_body(json!({"success": true, "errorCode": 0}));
        })
        .await;
    let credentials = ApplicationCredentials::builder("synthetic-app-user", "synthetic-password")
        .device_id("synthetic-device")
        .app_id("synthetic-app")
        .verify_key("synthetic-verify-key")
        .build()
        .unwrap_or_else(|error| panic!("fixture application credentials must be valid: {error}"));
    let endpoints = Endpoints::custom(&server.base_url(), &server.base_url())
        .unwrap_or_else(|error| panic!("fixture URL must be valid: {error}"));
    let client = Client::application_builder(credentials)
        .endpoints(endpoints)
        .build()
        .unwrap_or_else(|error| panic!("fixture application client must build: {error}"));

    client
        .ping()
        .await
        .unwrap_or_else(|error| panic!("fixture ping must succeed: {error}"));
    client
        .authenticate()
        .await
        .unwrap_or_else(|error| panic!("fixture application login must succeed: {error}"));
    client
        .logout()
        .await
        .unwrap_or_else(|error| panic!("fixture logout must succeed: {error}"));

    ping.assert_async().await;
    login.assert_async().await;
    logout.assert_async().await;
    assert!(matches!(
        client.search_active_accounts().await,
        Err(Error::NotAuthenticated)
    ));
}

#[tokio::test]
async fn cancelled_logout_invalidates_only_its_admitted_session() {
    let server = MockServer::start_async().await;
    let _login = server
        .mock_async(|when, then| {
            when.method(POST).path("/api/Auth/loginKey");
            then.status(200)
                .json_body(json!({"success": true, "errorCode": 0, "token": "logout-token"}));
        })
        .await;
    let _logout = server
        .mock_async(|when, then| {
            when.method(POST).path("/api/Auth/logout");
            then.status(200)
                .delay(Duration::from_millis(200))
                .json_body(json!({"success": true, "errorCode": 0}));
        })
        .await;
    let client = fixture_client(&server);
    client
        .authenticate()
        .await
        .unwrap_or_else(|error| panic!("fixture login must succeed: {error}"));

    assert!(
        tokio::time::timeout(Duration::from_millis(20), client.logout())
            .await
            .is_err()
    );
    assert!(matches!(
        client.search_active_accounts().await,
        Err(Error::NotAuthenticated)
    ));
}

#[tokio::test]
async fn provider_rate_limited_logout_retains_the_session() {
    let server = MockServer::start_async().await;
    let _login = server
        .mock_async(|when, then| {
            when.method(POST).path("/api/Auth/loginKey");
            then.status(200)
                .json_body(json!({"success": true, "errorCode": 0, "token": "logout-token"}));
        })
        .await;
    let logout = server
        .mock_async(|when, then| {
            when.method(POST).path("/api/Auth/logout");
            then.status(429).header("retry-after", "0");
        })
        .await;
    let accounts = server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/api/Account/search")
                .header("authorization", "Bearer logout-token");
            then.status(200).json_body(json!({
                "accounts": [],
                "success": true,
                "errorCode": 0
            }));
        })
        .await;
    let client = fixture_client(&server);
    client
        .authenticate()
        .await
        .unwrap_or_else(|error| panic!("fixture login must succeed: {error}"));

    assert!(matches!(
        client.logout().await,
        Err(Error::ProviderRateLimited { .. })
    ));
    client
        .search_active_accounts()
        .await
        .unwrap_or_else(|error| panic!("rate-limited logout must retain the session: {error}"));

    logout.assert_async().await;
    accounts.assert_async().await;
}

#[test]
fn credentials_are_redacted_and_identifiers_validate() {
    let credentials = Credentials::new("private-user", "private-key")
        .unwrap_or_else(|error| panic!("fixture credentials must be valid: {error}"));
    let debug = format!("{credentials:?}");
    assert!(!debug.contains("private-user"));
    assert!(!debug.contains("private-key"));
    assert!(debug.contains("[REDACTED]"));

    let builder = ApplicationCredentials::builder("private-app-user", "private-password")
        .device_id("private-device")
        .app_id("private-app")
        .verify_key("private-verify-key");
    let builder_debug = format!("{builder:?}");
    for secret in [
        "private-app-user",
        "private-password",
        "private-device",
        "private-app",
        "private-verify-key",
    ] {
        assert!(!builder_debug.contains(secret));
    }
    let application = builder
        .build()
        .unwrap_or_else(|error| panic!("fixture application credentials must be valid: {error}"));
    let application_debug = format!("{application:?}");
    assert!(!application_debug.contains("private-password"));
    assert!(!application_debug.contains("private-verify-key"));
    assert!(matches!(
        ApplicationCredentials::builder("private-app-user", "private-password").build(),
        Err(Error::Configuration(_))
    ));
    assert!(matches!(
        ApplicationCredentials::builder(" padded-user ", "private-password")
            .device_id("private-device")
            .app_id("private-app")
            .verify_key("private-verify-key")
            .build(),
        Err(Error::Configuration(_))
    ));

    assert!(matches!(
        AccountId::new(0),
        Err(Error::InvalidIdentifier { .. })
    ));
    assert!(matches!(
        ContractId::new(" padded "),
        Err(Error::InvalidIdentifier { .. })
    ));
}

#[test]
fn unknown_provider_enum_codes_remain_observable() {
    let side: Side = serde_json::from_str("99")
        .unwrap_or_else(|error| panic!("unknown wire code must remain decodable: {error}"));
    assert_eq!(side, Side::Unknown(99));
    assert_eq!(side.code(), 99);
}

#[test]
fn provider_enums_preserve_known_and_future_wire_codes() {
    let stop_limit: OrderType = serde_json::from_str("3")
        .unwrap_or_else(|error| panic!("stop-limit code must decode: {error}"));
    let future_status: OrderStatus = serde_json::from_str("99")
        .unwrap_or_else(|error| panic!("future order status must decode: {error}"));
    let buy: TradeLogType = serde_json::from_str("0")
        .unwrap_or_else(|error| panic!("buy trade-log code must decode: {error}"));
    let sell: TradeLogType = serde_json::from_str("1")
        .unwrap_or_else(|error| panic!("sell trade-log code must decode: {error}"));
    let future_trade: TradeLogType = serde_json::from_str("42")
        .unwrap_or_else(|error| panic!("future trade-log code must decode: {error}"));

    assert_eq!(stop_limit, OrderType::StopLimit);
    for (code, expected) in [
        (0, OrderStatus::None),
        (1, OrderStatus::Open),
        (2, OrderStatus::Filled),
        (3, OrderStatus::Cancelled),
        (4, OrderStatus::Expired),
        (5, OrderStatus::Rejected),
        (6, OrderStatus::Pending),
        (7, OrderStatus::PendingCancellation),
        (8, OrderStatus::Suspended),
    ] {
        let status: OrderStatus = serde_json::from_value(json!(code))
            .unwrap_or_else(|error| panic!("known order-status code must decode: {error}"));
        assert_eq!(status, expected);
        assert_eq!(status.code(), code);
    }
    assert_eq!(future_status, OrderStatus::Unknown(99));
    assert_eq!(buy, TradeLogType::Buy);
    assert_eq!(sell, TradeLogType::Sell);
    assert_eq!(future_trade, TradeLogType::Unknown(42));
    let tick_code = serde_json::to_value(BarUnit::Tick)
        .unwrap_or_else(|error| panic!("tick unit must encode: {error}"));
    assert_eq!(tick_code, json!(7));
    assert!(serde_json::from_str::<BarUnit>("0").is_err());
}

#[test]
fn request_builders_reject_invalid_states() {
    let account_id =
        AccountId::new(42).unwrap_or_else(|error| panic!("fixture account must be valid: {error}"));
    let contract_id = ContractId::new("CON.F.US.MNQ.M26")
        .unwrap_or_else(|error| panic!("fixture contract must be valid: {error}"));
    let order_id =
        OrderId::new(84).unwrap_or_else(|error| panic!("fixture order must be valid: {error}"));

    assert_eq!(
        PlaceOrder::builder(account_id, contract_id, OrderType::Market, Side::Bid, 0,).build(),
        Err(RequestValidationError::NonPositiveOrderSize)
    );
    assert_eq!(
        ModifyOrder::builder(account_id, order_id).build(),
        Err(RequestValidationError::EmptyModification)
    );
    assert_eq!(
        ModifyOrder::builder(account_id, order_id).size(-1).build(),
        Err(RequestValidationError::NonPositiveReplacementSize)
    );
    assert_eq!(
        Bracket::new(0, OrderType::Stop),
        Err(RequestValidationError::NonPositiveBracketTicks)
    );
    assert_eq!(
        Bracket::new(4, OrderType::Unknown(99)),
        Err(RequestValidationError::UnsupportedOrderType { code: 99 })
    );
    assert_eq!(
        Bracket::new(4, OrderType::StopLimit),
        Err(RequestValidationError::UnsupportedOrderType { code: 3 })
    );
    let stop_limit_contract = ContractId::new("CON.F.US.MNQ.M26")
        .unwrap_or_else(|error| panic!("fixture contract must be valid: {error}"));
    assert_eq!(
        PlaceOrder::builder(
            account_id,
            stop_limit_contract,
            OrderType::StopLimit,
            Side::Bid,
            1,
        )
        .build(),
        Err(RequestValidationError::UnsupportedOrderType { code: 3 })
    );
    let unknown_contract = ContractId::new("CON.F.US.MNQ.M26")
        .unwrap_or_else(|error| panic!("fixture contract must be valid: {error}"));
    assert_eq!(
        PlaceOrder::builder(
            account_id,
            unknown_contract,
            OrderType::Market,
            Side::Unknown(7),
            1,
        )
        .build(),
        Err(RequestValidationError::UnsupportedOrderSide { code: 7 })
    );

    let history_contract = ContractId::new("CON.F.US.MNQ.M26")
        .unwrap_or_else(|error| panic!("fixture contract must be valid: {error}"));
    let history = || {
        HistoryRequest::builder(
            history_contract.clone(),
            false,
            Timestamp::new("2026-01-01T00:00:00Z")
                .unwrap_or_else(|error| panic!("fixture timestamp must be valid: {error}")),
            Timestamp::new("2026-01-02T00:00:00Z")
                .unwrap_or_else(|error| panic!("fixture timestamp must be valid: {error}")),
            BarUnit::Minute,
        )
    };
    assert_eq!(
        history().unit_number(0).build(),
        Err(RequestValidationError::NonPositiveHistoryUnitNumber)
    );
    assert_eq!(
        history().limit(20_001).build(),
        Err(RequestValidationError::HistoryLimitOutOfRange)
    );
    assert_eq!(
        PartialCloseContract::new(account_id, history_contract.clone(), 0),
        Err(RequestValidationError::NonPositivePartialCloseSize)
    );
}

#[tokio::test]
async fn bodyless_provider_rejection_is_a_typed_provider_error() {
    let server = MockServer::start_async().await;
    let _login = server
        .mock_async(|when, then| {
            when.method(POST).path("/api/Auth/loginKey");
            then.status(200)
                .json_body(json!({"success": true, "errorCode": 0, "token": "synthetic-token"}));
        })
        .await;
    let accounts = server
        .mock_async(|when, then| {
            when.method(POST).path("/api/Account/search");
            then.status(200).json_body(json!({
                "success": false,
                "errorCode": 17,
                "errorMessage": "synthetic rejection"
            }));
        })
        .await;

    let client = fixture_client(&server);
    client
        .authenticate()
        .await
        .unwrap_or_else(|error| panic!("fixture login must succeed: {error}"));
    let result = client.search_active_accounts().await;

    accounts.assert_async().await;
    assert!(matches!(
        result,
        Err(Error::Provider(provider_error)) if provider_error.code == 17
    ));
}

#[tokio::test]
async fn authenticated_calls_fail_closed_before_login() {
    let server = MockServer::start_async().await;
    let client = fixture_client(&server);
    assert!(matches!(
        client.search_active_accounts().await,
        Err(Error::NotAuthenticated)
    ));
}

#[tokio::test]
async fn response_limit_is_enforced_while_streaming() {
    let server = MockServer::start_async().await;
    let _login = server
        .mock_async(|when, then| {
            when.method(POST).path("/api/Auth/loginKey");
            then.status(200)
                .json_body(json!({"success": true, "errorCode": 0, "token": "token"}));
        })
        .await;
    let _accounts = server
        .mock_async(|when, then| {
            when.method(POST).path("/api/Account/search");
            then.status(200).body("{\"accounts\":[],\"success\":true}");
        })
        .await;

    let credentials = Credentials::new("user", "key")
        .unwrap_or_else(|error| panic!("fixture credentials must be valid: {error}"));
    let endpoints = Endpoints::custom(&server.base_url(), &server.base_url())
        .unwrap_or_else(|error| panic!("fixture URL must be valid: {error}"));
    let client = Client::builder(credentials)
        .endpoints(endpoints)
        .response_limit(8)
        .build()
        .unwrap_or_else(|error| panic!("fixture client must build: {error}"));

    assert!(matches!(
        client.authenticate().await,
        Err(Error::ResponseTooLarge { limit_bytes: 8 })
    ));
}
