//! Public-API contract tests using synthetic local fixtures.

use httpmock::prelude::*;
use projectx_client::{AccountId, Client, ContractId, Credentials, Decimal, Endpoints, Error};
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

    let client = fixture_client(&server);
    client
        .authenticate()
        .await
        .unwrap_or_else(|error| panic!("fixture login must succeed: {error}"));
    let result = client
        .search_active_accounts()
        .await
        .unwrap_or_else(|error| panic!("fixture account search must succeed: {error}"));

    login.assert_async().await;
    accounts.assert_async().await;
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].id.get(), 42);
    assert_eq!(result[0].balance, Some(Decimal::new(123_450, 2)));
}

#[test]
fn credentials_are_redacted_and_identifiers_validate() {
    let credentials = Credentials::new("private-user", "private-key")
        .unwrap_or_else(|error| panic!("fixture credentials must be valid: {error}"));
    let debug = format!("{credentials:?}");
    assert!(!debug.contains("private-user"));
    assert!(!debug.contains("private-key"));
    assert!(debug.contains("[REDACTED]"));

    assert!(matches!(
        AccountId::new(0),
        Err(Error::InvalidIdentifier { .. })
    ));
    assert!(matches!(
        ContractId::new(" padded "),
        Err(Error::InvalidIdentifier { .. })
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
                .json_body(json!({"success": true, "token": "token"}));
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
