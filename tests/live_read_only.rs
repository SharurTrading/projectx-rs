// SPDX-FileCopyrightText: 2026 Kevin Monaghan
// SPDX-License-Identifier: MIT

//! Deliberate, ignored, read-only live validation.

#![cfg(feature = "live-tests")]

use std::{env, time::Duration};

use projectx_client::{Client, Credentials, Hub, RealtimeEvent};

#[tokio::test]
#[ignore = "requires deliberate ProjectX credentials and performs live read-only network I/O"]
async fn authenticates_discovers_and_handshakes_market_hub() {
    let user_name = env::var("PROJECTX_USERNAME")
        .unwrap_or_else(|_| panic!("PROJECTX_USERNAME must be supplied deliberately"));
    let api_key = env::var("PROJECTX_API_KEY")
        .unwrap_or_else(|_| panic!("PROJECTX_API_KEY must be supplied deliberately"));
    let credentials = Credentials::new(user_name, api_key)
        .unwrap_or_else(|error| panic!("live credential shape is invalid: {error}"));
    let client = Client::builder(credentials)
        .build()
        .unwrap_or_else(|error| panic!("live client configuration failed: {error}"));
    client
        .authenticate()
        .await
        .unwrap_or_else(|error| panic!("live authentication failed: {error}"));
    let accounts = client
        .search_active_accounts()
        .await
        .unwrap_or_else(|error| panic!("live active-account discovery failed: {error}"));
    let contracts = client
        .available_contracts(false)
        .await
        .unwrap_or_else(|error| panic!("live contract discovery failed: {error}"));
    assert!(!accounts.is_empty(), "provider returned no active accounts");
    assert!(!contracts.is_empty(), "provider returned no contracts");

    let realtime = client.realtime(Hub::Market);
    let mut events = realtime
        .take_event_receiver()
        .unwrap_or_else(|| panic!("market event receiver must be available"));
    realtime
        .connect()
        .await
        .unwrap_or_else(|error| panic!("live market handshake failed: {error}"));
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), events.recv()).await,
        Ok(Some(RealtimeEvent::Connected))
    ));
    realtime
        .disconnect()
        .await
        .unwrap_or_else(|error| panic!("live market disconnect failed: {error}"));
}
