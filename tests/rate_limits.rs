// SPDX-FileCopyrightText: 2026 Kevin Monaghan
// SPDX-License-Identifier: MIT

//! Public rate-limit contract tests using synthetic local fixtures.

use std::time::Duration;

use httpmock::prelude::*;
use projectx_client::{
    AccountId, BarUnit, Client, ContractId, Credentials, Endpoints, Error, HistoryRequest,
    OrderType, PlaceOrder, RateLimit, RateLimitConfig, RateLimitKind, Side, Timestamp,
};
use serde_json::json;

fn limits(max_requests: usize, window: Duration) -> RateLimitConfig {
    let limit = RateLimit::new(max_requests, window)
        .unwrap_or_else(|error| panic!("fixture rate limit must be valid: {error}"));
    RateLimitConfig::new(limit, limit)
}

fn fixture_client(server: &MockServer, rate_limits: RateLimitConfig, max_retries: u32) -> Client {
    let credentials = Credentials::new("synthetic-user", "synthetic-key")
        .unwrap_or_else(|error| panic!("fixture credentials must be valid: {error}"));
    let endpoints = Endpoints::custom(&server.base_url(), &server.base_url())
        .unwrap_or_else(|error| panic!("fixture endpoints must be valid: {error}"));
    Client::builder(credentials)
        .endpoints(endpoints)
        .rate_limits(rate_limits)
        .max_retries(max_retries)
        .retry_delays(Duration::from_millis(1), Duration::from_millis(2))
        .build()
        .unwrap_or_else(|error| panic!("fixture client must build: {error}"))
}

async fn authenticate(client: &Client, server: &MockServer) {
    let login = server
        .mock_async(|when, then| {
            when.method(POST).path("/api/Auth/loginKey");
            then.status(200)
                .json_body(json!({"success": true, "errorCode": 0, "token": "synthetic-token"}));
        })
        .await;
    client
        .authenticate()
        .await
        .unwrap_or_else(|error| panic!("fixture authentication must succeed: {error}"));
    login.assert_calls_async(1).await;
}

fn place_order() -> PlaceOrder {
    let account_id =
        AccountId::new(42).unwrap_or_else(|error| panic!("fixture account must be valid: {error}"));
    let contract_id = ContractId::new("CON.F.US.MNQ.M26")
        .unwrap_or_else(|error| panic!("fixture contract must be valid: {error}"));
    PlaceOrder::builder(account_id, contract_id, OrderType::Market, Side::Bid, 1)
        .custom_tag("rate-limit-fixture")
        .build()
        .unwrap_or_else(|error| panic!("fixture order must be valid: {error}"))
}

fn history_request() -> HistoryRequest {
    let contract_id = ContractId::new("CON.F.US.MNQ.M26")
        .unwrap_or_else(|error| panic!("fixture contract must be valid: {error}"));
    HistoryRequest::builder(
        contract_id,
        false,
        Timestamp::new("2026-01-01T00:00:00Z")
            .unwrap_or_else(|error| panic!("fixture timestamp must be valid: {error}")),
        Timestamp::new("2026-01-01T01:00:00Z")
            .unwrap_or_else(|error| panic!("fixture timestamp must be valid: {error}")),
        BarUnit::Minute,
    )
    .limit(60)
    .build()
    .unwrap_or_else(|error| panic!("fixture history request must be valid: {error}"))
}

#[tokio::test]
async fn cloned_clients_share_capacity_and_local_mutation_rejection_sends_nothing() {
    let server = MockServer::start_async().await;
    let accounts = server
        .mock_async(|when, then| {
            when.method(POST).path("/api/Account/search");
            then.status(200)
                .json_body(json!({"accounts": [], "success": true, "errorCode": 0}));
        })
        .await;
    let placement = server
        .mock_async(|when, then| {
            when.method(POST).path("/api/Order/place");
            then.status(200)
                .json_body(json!({"orderId": 84, "success": true, "errorCode": 0}));
        })
        .await;
    let window = Duration::from_mins(1);
    let client = fixture_client(&server, limits(1, window), 0);
    authenticate(&client, &server).await;

    client
        .search_active_accounts()
        .await
        .unwrap_or_else(|error| panic!("first general request must succeed: {error}"));
    let Err(error) = client.clone().place_order(&place_order()).await else {
        panic!("the shared general budget must reject the mutation locally");
    };

    assert!(matches!(
        error,
        Error::LocallyRateLimited {
            kind: RateLimitKind::General,
            retry_after
        } if retry_after <= window && !retry_after.is_zero()
    ));
    accounts.assert_calls_async(1).await;
    placement.assert_calls_async(0).await;
}

#[tokio::test]
async fn cloned_clients_reach_nontrivial_general_and_history_limits() {
    let server = MockServer::start_async().await;
    let accounts = server
        .mock_async(|when, then| {
            when.method(POST).path("/api/Account/search");
            then.status(200)
                .json_body(json!({"accounts": [], "success": true, "errorCode": 0}));
        })
        .await;
    let history = server
        .mock_async(|when, then| {
            when.method(POST).path("/api/History/retrieveBars");
            then.status(200)
                .json_body(json!({"bars": [], "success": true, "errorCode": 0}));
        })
        .await;
    let placement = server
        .mock_async(|when, then| {
            when.method(POST).path("/api/Order/place");
            then.status(200)
                .json_body(json!({"orderId": 84, "success": true, "errorCode": 0}));
        })
        .await;
    let window = Duration::from_mins(1);
    let client = fixture_client(&server, limits(3, window), 0);
    authenticate(&client, &server).await;
    let clients = [client.clone(), client.clone(), client.clone()];
    let request = history_request();

    for client_clone in &clients {
        client_clone
            .search_active_accounts()
            .await
            .unwrap_or_else(|error| panic!("every allowed general request must succeed: {error}"));
    }
    for client_clone in &clients {
        client_clone
            .retrieve_bars(&request)
            .await
            .unwrap_or_else(|error| panic!("every allowed history request must succeed: {error}"));
    }
    let Err(error) = client.place_order(&place_order()).await else {
        panic!("the fourth general request must be rejected locally");
    };

    assert!(matches!(
        error,
        Error::LocallyRateLimited {
            kind: RateLimitKind::General,
            retry_after
        } if retry_after <= window && !retry_after.is_zero()
    ));
    accounts.assert_calls_async(3).await;
    history.assert_calls_async(3).await;
    placement.assert_calls_async(0).await;
}

#[tokio::test]
async fn retrieve_bars_uses_a_budget_independent_from_general_queries() {
    let server = MockServer::start_async().await;
    let accounts = server
        .mock_async(|when, then| {
            when.method(POST).path("/api/Account/search");
            then.status(200)
                .json_body(json!({"accounts": [], "success": true, "errorCode": 0}));
        })
        .await;
    let history = server
        .mock_async(|when, then| {
            when.method(POST).path("/api/History/retrieveBars");
            then.status(200)
                .json_body(json!({"bars": [], "success": true, "errorCode": 0}));
        })
        .await;
    let client = fixture_client(&server, limits(1, Duration::from_mins(1)), 0);
    authenticate(&client, &server).await;

    client
        .search_active_accounts()
        .await
        .unwrap_or_else(|error| panic!("general query must succeed: {error}"));
    let request = history_request();
    client
        .retrieve_bars(&request)
        .await
        .unwrap_or_else(|error| panic!("history query must use its own budget: {error}"));

    accounts.assert_calls_async(1).await;
    history.assert_calls_async(1).await;
}

#[tokio::test]
async fn provider_retry_after_cools_the_shared_budget() {
    let server = MockServer::start_async().await;
    let accounts = server
        .mock_async(|when, then| {
            when.method(POST).path("/api/Account/search");
            then.status(429).header("Retry-After", "60");
        })
        .await;
    let placement = server
        .mock_async(|when, then| {
            when.method(POST).path("/api/Order/place");
            then.status(200)
                .json_body(json!({"orderId": 84, "success": true, "errorCode": 0}));
        })
        .await;
    let client = fixture_client(&server, limits(10, Duration::from_mins(1)), 0);
    authenticate(&client, &server).await;

    let Err(query_error) = client.search_active_accounts().await else {
        panic!("provider HTTP 429 must remain observable after retries are exhausted");
    };
    assert!(matches!(
        query_error,
        Error::ProviderRateLimited {
            kind: RateLimitKind::General,
            retry_after
        } if retry_after == Duration::from_mins(1)
    ));

    let Err(mutation_error) = client.place_order(&place_order()).await else {
        panic!("the shared provider cooldown must reject a mutation before sending");
    };
    assert!(matches!(
        mutation_error,
        Error::LocallyRateLimited {
            kind: RateLimitKind::General,
            retry_after
        } if !retry_after.is_zero()
    ));
    accounts.assert_calls_async(1).await;
    placement.assert_calls_async(0).await;
}

#[tokio::test]
async fn provider_rate_limit_after_mutation_send_is_ambiguous_and_never_retried() {
    let server = MockServer::start_async().await;
    let placement = server
        .mock_async(|when, then| {
            when.method(POST).path("/api/Order/place");
            then.status(429).header("Retry-After", "1");
        })
        .await;
    let client = fixture_client(&server, limits(10, Duration::from_mins(1)), 3);
    authenticate(&client, &server).await;

    let Err(error) = client.place_order(&place_order()).await else {
        panic!("a provider response after mutation submission must be reconciled");
    };

    assert!(matches!(
        error,
        Error::AmbiguousMutation {
            operation: "order placement"
        }
    ));
    placement.assert_calls_async(1).await;
}

#[tokio::test]
async fn session_validator_shutdown_cancels_a_rate_limit_wait() {
    let server = MockServer::start_async().await;
    let login = server
        .mock_async(|when, then| {
            when.method(POST).path("/api/Auth/loginKey");
            then.status(200)
                .json_body(json!({"success": true, "errorCode": 0, "token": "synthetic-token"}));
        })
        .await;
    let accounts = server
        .mock_async(|when, then| {
            when.method(POST).path("/api/Account/search");
            then.status(200)
                .json_body(json!({"accounts": [], "success": true, "errorCode": 0}));
        })
        .await;
    let validation = server
        .mock_async(|when, then| {
            when.method(POST).path("/api/Auth/validate").body("");
            then.status(200)
                .json_body(json!({"success": true, "errorCode": 0}));
        })
        .await;
    let client = fixture_client(&server, limits(1, Duration::from_mins(1)), 0);
    let validator = client
        .authenticate_with_validation(Duration::from_secs(1))
        .await
        .unwrap_or_else(|error| panic!("fixture validation must start: {error}"));
    client
        .search_active_accounts()
        .await
        .unwrap_or_else(|error| panic!("query must consume the general budget: {error}"));

    tokio::time::sleep(Duration::from_millis(1_050)).await;
    tokio::time::timeout(Duration::from_millis(100), validator.shutdown())
        .await
        .unwrap_or_else(|_| panic!("validator shutdown must cancel the limiter wait"))
        .unwrap_or_else(|error| panic!("validator task must shut down cleanly: {error}"));
    login.assert_calls_async(1).await;
    accounts.assert_calls_async(1).await;
    validation.assert_calls_async(0).await;
}
