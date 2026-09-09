// SPDX-FileCopyrightText: 2026 Kevin Monaghan
// SPDX-License-Identifier: MIT-0

//! Public real-time lifecycle tests using synthetic local fixtures.

use std::time::Duration;

use futures_util::{SinkExt as _, StreamExt as _};
use httpmock::prelude::*;
use projectx_client::{
    Client, ContractId, Credentials, Endpoints, Hub, RealtimeError, RealtimeEvent,
    SignalRInvocation,
};
use serde_json::{Value, json};
use tokio::{
    net::TcpListener,
    sync::{mpsc, oneshot},
};
use tokio_tungstenite::{
    WebSocketStream, accept_hdr_async,
    tungstenite::{Message, handshake::server::Request},
};

const TERMINATOR: char = '\u{001e}';

fn fixture_client(http: &MockServer, realtime_address: std::net::SocketAddr) -> Client {
    let credentials = Credentials::new("synthetic-user", "synthetic-key")
        .unwrap_or_else(|error| panic!("synthetic credentials must be valid: {error}"));
    let endpoints = Endpoints::custom(&http.base_url(), &format!("http://{realtime_address}"))
        .unwrap_or_else(|error| panic!("fixture endpoints must be valid: {error}"));
    Client::builder(credentials)
        .endpoints(endpoints)
        .build()
        .unwrap_or_else(|error| panic!("fixture client must build: {error}"))
}

// `accept_hdr_async` fixes the callback's large HTTP response error type; this
// helper cannot narrow the third-party signature.
#[allow(clippy::result_large_err)]
async fn accept_socket(listener: &TcpListener) -> (WebSocketStream<tokio::net::TcpStream>, String) {
    let (stream, _) = listener
        .accept()
        .await
        .unwrap_or_else(|error| panic!("fixture socket must accept: {error}"));
    let (uri_tx, uri_rx) = oneshot::channel();
    let mut uri_tx = Some(uri_tx);
    let socket = accept_hdr_async(stream, move |request: &Request, response| {
        if let Some(sender) = uri_tx.take() {
            let _ = sender.send(request.uri().to_string());
        }
        Ok(response)
    })
    .await
    .unwrap_or_else(|error| panic!("fixture websocket must upgrade: {error}"));
    let uri = uri_rx
        .await
        .unwrap_or_else(|error| panic!("fixture URI must arrive: {error}"));
    (socket, uri)
}

async fn complete_handshake(socket: &mut WebSocketStream<tokio::net::TcpStream>) {
    let Some(Ok(Message::Text(handshake))) = socket.next().await else {
        panic!("fixture must receive the SignalR handshake");
    };
    assert_eq!(
        handshake.as_str(),
        "{\"protocol\":\"json\",\"version\":1}\u{001e}"
    );
    socket
        .send(Message::Text("{}\u{001e}".into()))
        .await
        .unwrap_or_else(|error| panic!("fixture handshake response must send: {error}"));
}

fn has_token(uri: &str, expected: &str) -> bool {
    url::Url::parse(&format!("ws://fixture{uri}"))
        .ok()
        .and_then(|url| {
            url.query_pairs()
                .find(|(key, _)| key == "access_token")
                .map(|(_, value)| value == expected)
        })
        .unwrap_or(false)
}

async fn authenticate_fixture(client: &Client) {
    client
        .authenticate()
        .await
        .unwrap_or_else(|error| panic!("fixture login must succeed: {error}"));
}

async fn wait_for_client_close(socket: &mut WebSocketStream<tokio::net::TcpStream>) -> bool {
    while let Some(message) = socket.next().await {
        match message {
            Ok(Message::Close(_)) => {
                socket
                    .flush()
                    .await
                    .unwrap_or_else(|error| panic!("close response must flush: {error}"));
                return true;
            }
            Err(_) => return false,
            Ok(_) => {}
        }
    }
    false
}

fn spawn_reconnect_server(
    listener: TcpListener,
    close_first_rx: oneshot::Receiver<()>,
) -> tokio::task::JoinHandle<(String, String)> {
    tokio::spawn(async move {
        let (mut first, first_uri) = accept_socket(&listener).await;
        complete_handshake(&mut first).await;
        close_first_rx
            .await
            .unwrap_or_else(|error| panic!("fixture close signal must arrive: {error}"));
        first
            .close(None)
            .await
            .unwrap_or_else(|error| panic!("fixture first socket must close: {error}"));

        let (mut second, second_uri) = accept_socket(&listener).await;
        complete_handshake(&mut second).await;
        while let Some(message) = second.next().await {
            match message {
                Ok(Message::Text(frame)) => {
                    let payload: Value = serde_json::from_str(frame.trim_end_matches(TERMINATOR))
                        .unwrap_or_else(|error| panic!("invocation must be JSON: {error}"));
                    if let Some(invocation_id) = payload.get("invocationId").and_then(Value::as_str)
                    {
                        second
                            .send(Message::Text(
                                format!(
                                    "{}{}",
                                    json!({"type": 3, "invocationId": invocation_id}),
                                    TERMINATOR
                                )
                                .into(),
                            ))
                            .await
                            .unwrap_or_else(|error| {
                                panic!("fixture completion must send: {error}")
                            });
                    }
                }
                Ok(Message::Close(_)) => {
                    second
                        .flush()
                        .await
                        .unwrap_or_else(|error| panic!("close response must flush: {error}"));
                    break;
                }
                Err(_) => break,
                Ok(_) => {}
            }
        }
        (first_uri, second_uri)
    })
}

fn spawn_uncompleted_invocation_server(
    listener: TcpListener,
    seen_tx: oneshot::Sender<()>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let (mut socket, _) = accept_socket(&listener).await;
        complete_handshake(&mut socket).await;
        let mut seen_tx = Some(seen_tx);
        while let Some(message) = socket.next().await {
            match message {
                Ok(Message::Text(frame)) => {
                    let payload: Value = serde_json::from_str(frame.trim_end_matches(TERMINATOR))
                        .unwrap_or_else(|error| panic!("invocation fixture must be JSON: {error}"));
                    if payload.get("target").and_then(Value::as_str) == Some("Healthy") {
                        let completion =
                            json!({"type": 3, "invocationId": payload["invocationId"]});
                        socket
                            .send(Message::Text(format!("{completion}{TERMINATOR}").into()))
                            .await
                            .unwrap_or_else(|error| {
                                panic!("healthy completion must send: {error}")
                            });
                    }
                    if payload.get("type").and_then(Value::as_u64) == Some(1)
                        && let Some(seen_tx) = seen_tx.take()
                    {
                        seen_tx
                            .send(())
                            .unwrap_or_else(|()| panic!("invocation signal must send"));
                    }
                }
                Ok(Message::Close(_)) => {
                    socket
                        .flush()
                        .await
                        .unwrap_or_else(|error| panic!("close response must flush: {error}"));
                    break;
                }
                Err(_) => break,
                Ok(_) => {}
            }
        }
    })
}

#[tokio::test]
async fn reconnect_snapshots_rotated_token_and_requires_subscription_replay() {
    let http = MockServer::start_async().await;
    let _login = http
        .mock_async(|when, then| {
            when.method(POST).path("/api/Auth/loginKey");
            then.status(200).json_body(json!({
                "success": true,
                "errorCode": 0,
                "token": "initial-synthetic-token+/="
            }));
        })
        .await;
    let _validate = http
        .mock_async(|when, then| {
            when.method(POST).path("/api/Auth/validate");
            then.status(200).json_body(json!({
                "success": true,
                "errorCode": 0,
                "newToken": "rotated/synthetic-token=="
            }));
        })
        .await;

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap_or_else(|error| panic!("fixture listener must bind: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("fixture listener must have an address: {error}"));
    let (close_first_tx, close_first_rx) = oneshot::channel();
    let server = spawn_reconnect_server(listener, close_first_rx);

    let client = fixture_client(&http, address);
    client
        .authenticate()
        .await
        .unwrap_or_else(|error| panic!("fixture login must succeed: {error}"));
    let realtime = client.realtime(Hub::Market);
    let mut events = realtime
        .take_event_receiver()
        .unwrap_or_else(|| panic!("event receiver must be available once"));
    realtime
        .connect()
        .await
        .unwrap_or_else(|error| panic!("initial websocket must connect: {error}"));
    assert!(matches!(
        events.recv().await,
        Some(RealtimeEvent::Connected)
    ));

    let old_session = realtime
        .session()
        .unwrap_or_else(|e| panic!("old session: {e}"));
    client
        .validate_session()
        .await
        .unwrap_or_else(|error| panic!("fixture token rotation must succeed: {error}"));
    close_first_tx
        .send(())
        .unwrap_or_else(|()| panic!("fixture close signal must send"));
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), events.recv()).await,
        Ok(Some(RealtimeEvent::Disconnected))
    ));
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(8), events.recv()).await,
        Ok(Some(RealtimeEvent::Reconnected))
    ));

    let contract = ContractId::new("CON.F.US.MNQ.M26")
        .unwrap_or_else(|error| panic!("fixture contract must be valid: {error}"));
    assert!(matches!(
        old_session.subscribe_contract_trades(&contract).await,
        Err(RealtimeError::StaleGeneration)
    ));
    assert_ne!(
        old_session.generation(),
        realtime
            .session()
            .unwrap_or_else(|e| panic!("new session: {e}"))
            .generation()
    );
    realtime
        .subscribe_contract_trades(&contract)
        .await
        .unwrap_or_else(|error| panic!("post-reconnect subscription must complete: {error}"));
    realtime
        .disconnect()
        .await
        .unwrap_or_else(|error| panic!("fixture websocket must disconnect: {error}"));
    let (first_uri, second_uri) = server
        .await
        .unwrap_or_else(|error| panic!("fixture server must join: {error}"));
    assert!(has_token(&first_uri, "initial-synthetic-token+/="));
    assert!(has_token(&second_uri, "rotated/synthetic-token=="));
}

#[tokio::test]
async fn real_time_connect_fails_closed_before_authentication() {
    let http = MockServer::start_async().await;
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap_or_else(|error| panic!("fixture listener must bind: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("fixture listener must have an address: {error}"));
    let client = fixture_client(&http, address);
    let realtime = client.realtime(Hub::Market);
    assert!(matches!(
        realtime.connect().await,
        Err(RealtimeError::MissingAuthToken)
    ));
}

#[test]
fn invocation_decodes_exact_decimal_payload() {
    let invocation = SignalRInvocation::from_json(
        r#"{
            "type": 1,
            "target": "GatewayTrade",
            "arguments": [
                "CON.F.US.MNQ.M26",
                {
                    "symbolId": "F.US.MNQ",
                    "price": 0.1000000000000000000000000001,
                    "timestamp": "2026-01-01T00:00:00Z",
                    "type": 0,
                    "volume": 1
                }
            ]
        }"#,
    )
    .unwrap_or_else(|error| panic!("fixture JSON must decode: {error}"))
    .ok_or(RealtimeError::Protocol("missing invocation"))
    .unwrap_or_else(|error| panic!("fixture invocation must decode: {error}"));
    let trade: projectx_client::MarketTrade = invocation
        .decode()
        .unwrap_or_else(|error| panic!("fixture market trade must decode: {error}"));
    assert_eq!(trade.price.to_string(), "0.1000000000000000000000000001");
    assert_eq!(trade.trade_type, projectx_client::TradeLogType::Buy);
}

#[test]
fn invocation_decodes_documented_quote_with_exact_prices() {
    let invocation = SignalRInvocation::from_json(
        r#"{
            "type": 1,
            "target": "GatewayQuote",
            "arguments": [
                "CON.F.US.MNQ.M26",
                {
                    "symbol": "F.US.MNQ",
                    "symbolName": "/MNQ",
                    "lastPrice": 0.1000000000000000000000000001,
                    "bestBid": 0.1000000000000000000000000002,
                    "bestAsk": 0.1000000000000000000000000003,
                    "change": 1.25,
                    "changePercent": 0.5,
                    "open": 21000.25,
                    "high": 21100.50,
                    "low": 20900.75,
                    "volume": 12000,
                    "lastUpdated": "2026-01-01T00:00:00Z",
                    "timestamp": "2026-01-01T00:00:00Z"
                }
            ]
        }"#,
    )
    .unwrap_or_else(|error| panic!("fixture JSON must decode: {error}"))
    .ok_or(RealtimeError::Protocol("missing invocation"))
    .unwrap_or_else(|error| panic!("fixture invocation must decode: {error}"));
    let quote: projectx_client::MarketQuote = invocation
        .decode()
        .unwrap_or_else(|error| panic!("fixture market quote must decode: {error}"));

    assert_eq!(quote.raw_symbol.as_str(), "F.US.MNQ");
    assert_eq!(quote.symbol_name.as_deref(), Some("/MNQ"));
    assert_eq!(
        quote
            .last_price
            .as_ref()
            .map(ToString::to_string)
            .as_deref(),
        Some("0.1000000000000000000000000001")
    );
    assert_eq!(
        quote.best_bid.as_ref().map(ToString::to_string).as_deref(),
        Some("0.1000000000000000000000000002")
    );
    assert_eq!(
        quote.best_ask.as_ref().map(ToString::to_string).as_deref(),
        Some("0.1000000000000000000000000003")
    );
    assert_eq!(quote.volume, Some(12000));
    assert_eq!(
        quote.timestamp.map(|value| value.to_string()).as_deref(),
        Some("2026-01-01T00:00:00Z")
    );
}

const SPARSE_QUOTE_BATCH_JSON: &str = r#"{
            "type": 1,
            "target": "GatewayQuote",
            "arguments": [
                "CON.F.US.MNQ.M26",
                [
                    null,
                    {
                        "symbol": "F.US.MNQ",
                        "contract": "MNQM6",
                        "lastPrice": 21000.25,
                        "bestAsk": 21000.50,
                        "change": 25.50,
                        "changePercent": 0.14,
                        "volume": 12000,
                        "lastUpdated": "2026-01-01T00:00:00Z",
                        "timestamp": "2026-01-01T00:00:00Z"
                    },
                    {
                        "symbol": "F.US.MNQ",
                        "lastPrice": null,
                        "bestBid": null,
                        "bestAsk": 21000.50,
                        "change": null,
                        "changePercent": null,
                        "volume": null,
                        "lastUpdated": "2026-01-01T00:00:01Z",
                        "timestamp": "2026-01-01T00:00:01Z"
                    },
                    {
                        "symbol": "F.US.MNQ",
                        "lastPrice": 21000.25,
                        "bestBid": 21000.00,
                        "bestAsk": null,
                        "change": 25.50,
                        "changePercent": 0.14,
                        "volume": 12000,
                        "lastUpdated": "2026-01-01T00:00:02Z",
                        "timestamp": "2026-01-01T00:00:02Z"
                    },
                    {
                        "symbol": "F.US.MNQ",
                        "contract": "MNQM6",
                        "bestBid": 21000.00,
                        "lastUpdated": "2026-01-01T00:00:03Z",
                        "timestamp": "2026-01-01T00:00:03Z"
                    },
                    null
                ]
            ]
        }"#;

#[test]
fn invocation_batch_decodes_sparse_quotes_and_omits_null_padding() {
    let invocation = SignalRInvocation::from_json(SPARSE_QUOTE_BATCH_JSON)
        .unwrap_or_else(|error| panic!("fixture JSON must decode: {error}"))
        .ok_or(RealtimeError::Protocol("missing invocation"))
        .unwrap_or_else(|error| panic!("fixture invocation must decode: {error}"));
    let decoded = invocation.decode_batch::<projectx_client::MarketQuote>();
    let [missing_bid, null_bid, null_ask, missing_ask] = decoded.as_slice() else {
        panic!("fixture must omit null padding and produce exactly four quotes");
    };
    let missing_bid = missing_bid
        .as_ref()
        .unwrap_or_else(|error| panic!("missing-bid quote must decode: {error}"));
    let null_bid = null_bid
        .as_ref()
        .unwrap_or_else(|error| panic!("null-bid quote must decode: {error}"));
    let null_ask = null_ask
        .as_ref()
        .unwrap_or_else(|error| panic!("null-ask quote must decode: {error}"));
    let missing_ask = missing_ask
        .as_ref()
        .unwrap_or_else(|error| panic!("missing-ask quote must decode: {error}"));

    assert!(missing_bid.best_bid.is_none());
    assert_eq!(
        missing_bid
            .best_ask
            .as_ref()
            .map(ToString::to_string)
            .as_deref(),
        Some("21000.50")
    );
    assert!(null_bid.best_bid.is_none());
    assert_eq!(
        null_bid
            .best_ask
            .as_ref()
            .map(ToString::to_string)
            .as_deref(),
        Some("21000.50")
    );
    assert!(null_bid.last_price.is_none());
    assert!(null_bid.change.is_none());
    assert!(null_bid.change_percent.is_none());
    assert!(null_bid.volume.is_none());
    assert_eq!(
        null_ask
            .best_bid
            .as_ref()
            .map(ToString::to_string)
            .as_deref(),
        Some("21000.00")
    );
    assert!(null_ask.best_ask.is_none());
    assert_eq!(
        missing_ask
            .best_bid
            .as_ref()
            .map(ToString::to_string)
            .as_deref(),
        Some("21000.00")
    );
    assert!(missing_ask.best_ask.is_none());
    assert!(missing_ask.last_price.is_none());
    assert!(missing_ask.change.is_none());
    assert!(missing_ask.change_percent.is_none());
    assert!(missing_ask.open.is_none());
    assert!(missing_ask.high.is_none());
    assert!(missing_ask.low.is_none());
    assert!(missing_ask.volume.is_none());
}

#[test]
fn invocation_decodes_sparse_quote_without_event_timestamp() {
    let invocation = SignalRInvocation::from_json(
        r#"{
            "type": 1,
            "target": "GatewayQuote",
            "arguments": [
                "CON.F.US.MNQ.M26",
                {
                    "symbol": "F.US.MNQ",
                    "contract": "MNQM6",
                    "open": 21000.25,
                    "high": 21100.50,
                    "low": 20900.75,
                    "lastUpdated": "2026-01-01T00:00:04Z"
                }
            ]
        }"#,
    )
    .unwrap_or_else(|error| panic!("fixture JSON must decode: {error}"))
    .ok_or(RealtimeError::Protocol("missing invocation"))
    .unwrap_or_else(|error| panic!("fixture invocation must decode: {error}"));
    let quote: projectx_client::MarketQuote = invocation
        .decode()
        .unwrap_or_else(|error| panic!("sparse OHLC quote must decode: {error}"));

    assert!(quote.timestamp.is_none());
    assert_eq!(quote.last_updated.to_string(), "2026-01-01T00:00:04Z");
    assert_eq!(
        quote.open.as_ref().map(ToString::to_string).as_deref(),
        Some("21000.25")
    );
    assert_eq!(
        quote.high.as_ref().map(ToString::to_string).as_deref(),
        Some("21100.50")
    );
    assert_eq!(
        quote.low.as_ref().map(ToString::to_string).as_deref(),
        Some("20900.75")
    );
}

#[test]
fn invocation_batch_decodes_exact_entries_independently() {
    let invocation = SignalRInvocation::from_json(
        r#"{
            "type": 1,
            "target": "GatewayTrade",
            "arguments": [
                "CON.F.US.MNQ.M26",
                [
                    null,
                    {
                        "symbolId": "F.US.MNQ",
                        "price": 0.1000000000000000000000000001,
                        "timestamp": "2026-01-01T00:00:00Z",
                        "type": 0,
                        "volume": 1
                    },
                    {"symbolId": "malformed"}
                ]
            ]
        }"#,
    )
    .unwrap_or_else(|error| panic!("fixture JSON must decode: {error}"))
    .ok_or(RealtimeError::Protocol("missing invocation"))
    .unwrap_or_else(|error| panic!("fixture invocation must decode: {error}"));
    let decoded = invocation.decode_batch::<projectx_client::MarketTrade>();
    assert_eq!(decoded.len(), 2);
    assert_eq!(
        decoded[0]
            .as_ref()
            .unwrap_or_else(|error| panic!("first trade must decode: {error}"))
            .price
            .to_string(),
        "0.1000000000000000000000000001"
    );
    assert!(decoded[1].is_err());
}

#[tokio::test]
async fn concurrent_connect_installs_exactly_one_session() {
    let http = MockServer::start_async().await;
    let _login = http
        .mock_async(|when, then| {
            when.method(POST).path("/api/Auth/loginKey");
            then.status(200)
                .json_body(json!({"success": true, "errorCode": 0, "token": "synthetic-token"}));
        })
        .await;
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap_or_else(|error| panic!("fixture listener must bind: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("fixture listener must have an address: {error}"));
    let server = tokio::spawn(async move {
        let (mut socket, _) = accept_socket(&listener).await;
        complete_handshake(&mut socket).await;
        wait_for_client_close(&mut socket).await
    });

    let client = fixture_client(&http, address);
    authenticate_fixture(&client).await;
    let realtime = client.realtime(Hub::Market);
    let (first, second) = tokio::join!(realtime.connect(), realtime.connect());
    assert!(
        matches!(
            (&first, &second),
            (Ok(()), Err(RealtimeError::AlreadyConnected))
        ) || matches!(
            (&first, &second),
            (Err(RealtimeError::AlreadyConnected), Ok(()))
        )
    );
    assert!(realtime.is_connected());
    realtime
        .disconnect()
        .await
        .unwrap_or_else(|error| panic!("fixture websocket must disconnect: {error}"));
    assert!(
        server
            .await
            .unwrap_or_else(|error| panic!("fixture server must join: {error}"))
    );
}

#[tokio::test]
async fn disconnect_during_handshake_fences_connected_publication() {
    let http = MockServer::start_async().await;
    let _login = http
        .mock_async(|when, then| {
            when.method(POST).path("/api/Auth/loginKey");
            then.status(200)
                .json_body(json!({"success": true, "errorCode": 0, "token": "synthetic-token"}));
        })
        .await;
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap_or_else(|error| panic!("fixture listener must bind: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("fixture listener must have an address: {error}"));
    let (handshake_tx, handshake_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = accept_socket(&listener).await;
        let Some(Ok(Message::Text(_))) = socket.next().await else {
            panic!("fixture must receive a handshake");
        };
        handshake_tx
            .send(())
            .unwrap_or_else(|()| panic!("handshake signal must send"));
        while socket.next().await.is_some() {}
    });

    let client = fixture_client(&http, address);
    authenticate_fixture(&client).await;
    let realtime = client.realtime(Hub::Market);
    let mut events = realtime
        .take_event_receiver()
        .unwrap_or_else(|| panic!("event receiver must be available"));
    let connecting = realtime.clone();
    let connect_task = tokio::spawn(async move { connecting.connect().await });
    handshake_rx
        .await
        .unwrap_or_else(|error| panic!("handshake signal must arrive: {error}"));
    realtime
        .disconnect()
        .await
        .unwrap_or_else(|error| panic!("connecting session must cancel cleanly: {error}"));
    assert!(matches!(
        connect_task
            .await
            .unwrap_or_else(|error| panic!("connect task must join: {error}")),
        Err(RealtimeError::ConnectionCancelled)
    ));
    assert!(!realtime.is_connected());
    assert!(
        tokio::time::timeout(Duration::from_millis(50), events.recv())
            .await
            .is_err()
    );
    server
        .await
        .unwrap_or_else(|error| panic!("fixture server must join: {error}"));
}

#[tokio::test]
async fn close_timeout_always_clears_the_session() {
    let http = MockServer::start_async().await;
    let _login = http
        .mock_async(|when, then| {
            when.method(POST).path("/api/Auth/loginKey");
            then.status(200)
                .json_body(json!({"success": true, "errorCode": 0, "token": "synthetic-token"}));
        })
        .await;
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap_or_else(|error| panic!("fixture listener must bind: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("fixture listener must have an address: {error}"));
    let server = tokio::spawn(async move {
        let (mut socket, _) = accept_socket(&listener).await;
        complete_handshake(&mut socket).await;
        std::future::pending::<()>().await;
    });

    let client = fixture_client(&http, address);
    authenticate_fixture(&client).await;
    let realtime = client.realtime(Hub::Market);
    realtime
        .connect()
        .await
        .unwrap_or_else(|error| panic!("fixture websocket must connect: {error}"));
    tokio::time::pause();
    assert!(matches!(
        realtime.disconnect().await,
        Err(RealtimeError::Close)
    ));
    assert!(!realtime.is_connected());
    assert!(matches!(
        realtime.invoke("AfterClose", Vec::new()).await,
        Err(RealtimeError::NotConnected)
    ));
    server.abort();
}

#[tokio::test]
async fn websocket_upgrade_has_a_bounded_timeout() {
    let http = MockServer::start_async().await;
    let _login = http
        .mock_async(|when, then| {
            when.method(POST).path("/api/Auth/loginKey");
            then.status(200)
                .json_body(json!({"success": true, "errorCode": 0, "token": "synthetic-token"}));
        })
        .await;
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap_or_else(|error| panic!("fixture listener must bind: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("fixture listener must have an address: {error}"));
    let server = tokio::spawn(async move {
        let (_stream, _) = listener
            .accept()
            .await
            .unwrap_or_else(|error| panic!("fixture socket must accept: {error}"));
        std::future::pending::<()>().await;
    });

    let client = fixture_client(&http, address);
    authenticate_fixture(&client).await;
    let realtime = client.realtime(Hub::Market);
    tokio::time::pause();
    assert!(matches!(
        realtime.connect().await,
        Err(RealtimeError::ConnectionTimedOut)
    ));
    assert!(!realtime.is_connected());
    server.abort();
}

#[tokio::test]
async fn cancelled_admitted_invocation_preserves_the_generation() {
    let http = MockServer::start_async().await;
    let _login = http
        .mock_async(|when, then| {
            when.method(POST).path("/api/Auth/loginKey");
            then.status(200)
                .json_body(json!({"success": true, "errorCode": 0, "token": "synthetic-token"}));
        })
        .await;
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap_or_else(|error| panic!("fixture listener must bind: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("fixture listener must have an address: {error}"));
    let (seen_tx, seen_rx) = oneshot::channel();
    let server = spawn_uncompleted_invocation_server(listener, seen_tx);

    let client = fixture_client(&http, address);
    authenticate_fixture(&client).await;
    let realtime = client.realtime(Hub::Market);
    let mut events = realtime
        .take_event_receiver()
        .unwrap_or_else(|| panic!("event receiver must be available"));
    realtime
        .connect()
        .await
        .unwrap_or_else(|error| panic!("fixture websocket must connect: {error}"));
    assert_eq!(events.recv().await, Some(RealtimeEvent::Connected));
    let invoking = realtime.clone();
    let task = tokio::spawn(async move { invoking.invoke("Cancelled", Vec::new()).await });
    seen_rx
        .await
        .unwrap_or_else(|error| panic!("invocation signal must arrive: {error}"));
    task.abort();
    assert!(task.await.is_err());
    assert!(
        realtime.is_connected(),
        "cancelling one invocation must preserve its socket"
    );
    assert!(realtime.invoke("Healthy", Vec::new()).await.is_ok());
    realtime
        .disconnect()
        .await
        .unwrap_or_else(|error| panic!("ended generation cleanup must be idempotent: {error}"));
    server
        .await
        .unwrap_or_else(|error| panic!("fixture server must join: {error}"));
}

#[tokio::test]
async fn invocation_timeout_preserves_the_ambiguous_generation() {
    let http = MockServer::start_async().await;
    let _login = http
        .mock_async(|when, then| {
            when.method(POST).path("/api/Auth/loginKey");
            then.status(200)
                .json_body(json!({"success": true, "errorCode": 0, "token": "synthetic-token"}));
        })
        .await;
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap_or_else(|error| panic!("fixture listener must bind: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("fixture listener must have an address: {error}"));
    let (seen_tx, seen_rx) = oneshot::channel();
    let server = spawn_uncompleted_invocation_server(listener, seen_tx);

    let client = fixture_client(&http, address);
    authenticate_fixture(&client).await;
    let realtime = client.realtime(Hub::Market);
    let mut events = realtime
        .take_event_receiver()
        .unwrap_or_else(|| panic!("event receiver must be available"));
    realtime
        .connect()
        .await
        .unwrap_or_else(|error| panic!("fixture websocket must connect: {error}"));
    assert_eq!(events.recv().await, Some(RealtimeEvent::Connected));
    let invoking = realtime.clone();
    let task = tokio::spawn(async move { invoking.invoke("NoCompletion", Vec::new()).await });
    seen_rx
        .await
        .unwrap_or_else(|error| panic!("invocation signal must arrive: {error}"));

    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(16)).await;
    for _ in 0..4 {
        tokio::task::yield_now().await;
    }
    assert!(matches!(
        task.await
            .unwrap_or_else(|error| panic!("invocation task must join: {error}")),
        Err(RealtimeError::InvocationTimedOut { .. })
    ));
    assert!(
        realtime.is_connected(),
        "one completion deadline must preserve its socket"
    );
    tokio::time::resume();
    assert!(realtime.invoke("Healthy", Vec::new()).await.is_ok());
    realtime
        .disconnect()
        .await
        .unwrap_or_else(|error| panic!("timed-out generation cleanup must be idempotent: {error}"));
    assert_eq!(events.recv().await, Some(RealtimeEvent::Disconnected));
    assert!(!realtime.is_connected());
    server
        .await
        .unwrap_or_else(|error| panic!("fixture server must join: {error}"));
}

#[tokio::test]
async fn dropping_last_client_handle_closes_the_transport() {
    let http = MockServer::start_async().await;
    let _login = http
        .mock_async(|when, then| {
            when.method(POST).path("/api/Auth/loginKey");
            then.status(200)
                .json_body(json!({"success": true, "errorCode": 0, "token": "synthetic-token"}));
        })
        .await;
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap_or_else(|error| panic!("fixture listener must bind: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("fixture listener must have an address: {error}"));
    let server = tokio::spawn(async move {
        let (mut socket, _) = accept_socket(&listener).await;
        complete_handshake(&mut socket).await;
        while let Some(message) = socket.next().await {
            if matches!(message, Ok(Message::Close(_)) | Err(_)) {
                break;
            }
        }
    });

    let client = fixture_client(&http, address);
    authenticate_fixture(&client).await;
    let realtime = client.realtime(Hub::Market);
    realtime
        .connect()
        .await
        .unwrap_or_else(|error| panic!("fixture websocket must connect: {error}"));
    drop(realtime);
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), server).await,
        Ok(Ok(()))
    ));
}

#[tokio::test]
async fn reader_failure_clears_the_writer_session() {
    let http = MockServer::start_async().await;
    let _login = http
        .mock_async(|when, then| {
            when.method(POST).path("/api/Auth/loginKey");
            then.status(200)
                .json_body(json!({"success": true, "errorCode": 0, "token": "synthetic-token"}));
        })
        .await;
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap_or_else(|error| panic!("fixture listener must bind: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("fixture listener must have an address: {error}"));
    let server = tokio::spawn(async move {
        let (mut socket, _) = accept_socket(&listener).await;
        complete_handshake(&mut socket).await;
    });

    let client = fixture_client(&http, address);
    authenticate_fixture(&client).await;
    let realtime = client.realtime(Hub::Market);
    let mut events = realtime
        .take_event_receiver()
        .unwrap_or_else(|| panic!("event receiver must be available"));
    realtime
        .connect()
        .await
        .unwrap_or_else(|error| panic!("fixture websocket must connect: {error}"));
    assert!(matches!(
        events.recv().await,
        Some(RealtimeEvent::Connected)
    ));
    server
        .await
        .unwrap_or_else(|error| panic!("fixture server must join: {error}"));
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), events.recv()).await,
        Ok(Some(RealtimeEvent::Disconnected))
    ));
    assert!(!realtime.is_connected());
    assert!(matches!(
        realtime.invoke("AfterReaderFailure", Vec::new()).await,
        Err(RealtimeError::NotConnected)
    ));
    realtime
        .disconnect()
        .await
        .unwrap_or_else(|error| panic!("disconnected session cleanup must be idempotent: {error}"));
}

#[tokio::test]
async fn signalr_keepalive_is_internal_and_close_ends_the_generation() {
    let http = MockServer::start_async().await;
    let _login = http
        .mock_async(|when, then| {
            when.method(POST).path("/api/Auth/loginKey");
            then.status(200)
                .json_body(json!({"success": true, "errorCode": 0, "token": "synthetic-token"}));
        })
        .await;
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap_or_else(|error| panic!("fixture listener must bind: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("fixture listener must have an address: {error}"));
    let server = tokio::spawn(async move {
        let (mut socket, _) = accept_socket(&listener).await;
        complete_handshake(&mut socket).await;
        socket
            .send(Message::Text(
                format!(
                    "{}{}{}{}",
                    json!({"type": 6}),
                    TERMINATOR,
                    json!({"type": 7}),
                    TERMINATOR
                )
                .into(),
            ))
            .await
            .unwrap_or_else(|error| panic!("control frames must send: {error}"));
    });

    let client = fixture_client(&http, address);
    authenticate_fixture(&client).await;
    let realtime = client.realtime(Hub::Market);
    let mut events = realtime
        .take_event_receiver()
        .unwrap_or_else(|| panic!("event receiver must be available"));
    realtime
        .connect()
        .await
        .unwrap_or_else(|error| panic!("fixture websocket must connect: {error}"));
    assert!(matches!(
        events.recv().await,
        Some(RealtimeEvent::Connected)
    ));
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), events.recv()).await,
        Ok(Some(RealtimeEvent::Disconnected))
    ));
    assert!(!realtime.is_connected());
    server
        .await
        .unwrap_or_else(|error| panic!("fixture server must join: {error}"));
}

#[tokio::test]
async fn idle_session_sends_client_signalr_keepalive() {
    let http = MockServer::start_async().await;
    let _login = http
        .mock_async(|when, then| {
            when.method(POST).path("/api/Auth/loginKey");
            then.status(200)
                .json_body(json!({"success": true, "errorCode": 0, "token": "synthetic-token"}));
        })
        .await;
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap_or_else(|error| panic!("fixture listener must bind: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("fixture listener must have an address: {error}"));
    let (keepalive_tx, mut keepalive_rx) = mpsc::channel(1);
    let server = tokio::spawn(async move {
        let (mut socket, _) = accept_socket(&listener).await;
        complete_handshake(&mut socket).await;
        while let Some(message) = socket.next().await {
            match message {
                Ok(Message::Text(frame)) => {
                    let payload: Value = serde_json::from_str(frame.trim_end_matches(TERMINATOR))
                        .unwrap_or_else(|error| panic!("control frame must be JSON: {error}"));
                    if payload.get("type").and_then(Value::as_u64) == Some(6) {
                        keepalive_tx
                            .send(())
                            .await
                            .unwrap_or_else(|error| panic!("keepalive signal must send: {error}"));
                    }
                }
                Ok(Message::Close(_)) => {
                    socket
                        .flush()
                        .await
                        .unwrap_or_else(|error| panic!("close response must flush: {error}"));
                    break;
                }
                Ok(_) => {}
                Err(_) => break,
            }
        }
    });

    let client = fixture_client(&http, address);
    authenticate_fixture(&client).await;
    let realtime = client.realtime(Hub::Market);
    realtime
        .connect()
        .await
        .unwrap_or_else(|error| panic!("fixture websocket must connect: {error}"));
    tokio::time::pause();
    for _ in 0..4 {
        tokio::task::yield_now().await;
    }
    let mut keepalive_seen = false;
    for _ in 0..20 {
        tokio::time::advance(Duration::from_secs(1)).await;
        for _ in 0..4 {
            tokio::task::yield_now().await;
        }
        if keepalive_rx.try_recv().is_ok() {
            keepalive_seen = true;
            break;
        }
    }
    assert!(keepalive_seen, "idle client must send a SignalR ping");
    tokio::time::resume();
    realtime
        .disconnect()
        .await
        .unwrap_or_else(|error| panic!("fixture websocket must disconnect: {error}"));
    server
        .await
        .unwrap_or_else(|error| panic!("fixture server must join: {error}"));
}

#[tokio::test]
async fn ping_and_pong_traffic_keeps_the_session_live() {
    let http = MockServer::start_async().await;
    let _login = http
        .mock_async(|when, then| {
            when.method(POST).path("/api/Auth/loginKey");
            then.status(200)
                .json_body(json!({"success": true, "errorCode": 0, "token": "synthetic-token"}));
        })
        .await;
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap_or_else(|error| panic!("fixture listener must bind: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("fixture listener must have an address: {error}"));
    let (ping_tx, mut ping_rx) = mpsc::channel(1);
    let (pong_tx, mut pong_rx) = mpsc::channel(1);
    let server = tokio::spawn(async move {
        let (mut socket, _) = accept_socket(&listener).await;
        complete_handshake(&mut socket).await;
        loop {
            tokio::select! {
                command = ping_rx.recv() => {
                    if command.is_none() {
                        break;
                    }
                    socket
                        .send(Message::Ping(Vec::new().into()))
                        .await
                        .unwrap_or_else(|error| panic!("ping must send: {error}"));
                }
                message = socket.next() => match message {
                    Some(Ok(Message::Pong(_))) => {
                        match pong_tx.try_send(()) {
                            Ok(()) | Err(mpsc::error::TrySendError::Full(())) => {}
                            Err(mpsc::error::TrySendError::Closed(())) => break,
                        }
                    }
                    Some(Ok(Message::Close(_))) => {
                        socket
                            .flush()
                            .await
                            .unwrap_or_else(|error| panic!("close response must flush: {error}"));
                        break;
                    }
                    None | Some(Err(_)) => break,
                    Some(Ok(_)) => {}
                }
            }
        }
    });

    let client = fixture_client(&http, address);
    authenticate_fixture(&client).await;
    let realtime = client.realtime(Hub::Market);
    realtime
        .connect()
        .await
        .unwrap_or_else(|error| panic!("fixture websocket must connect: {error}"));
    tokio::time::pause();
    for _ in 0..4 {
        tokio::time::advance(Duration::from_secs(10)).await;
        ping_tx
            .send(())
            .await
            .unwrap_or_else(|error| panic!("ping command must send: {error}"));
        pong_rx
            .recv()
            .await
            .unwrap_or_else(|| panic!("pong must arrive"));
    }
    assert!(realtime.is_connected());
    assert!(!server.is_finished());
    tokio::time::resume();
    drop(realtime);
    drop(ping_tx);
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), server).await,
        Ok(Ok(()))
    ));
}

async fn exercise_nonterminal_gap(records: String, expected_prefix: usize) {
    let http = MockServer::start_async().await;
    let _login = http
        .mock_async(|when, then| {
            when.method(POST).path("/api/Auth/loginKey");
            then.status(200)
                .json_body(json!({"success":true,"errorCode":0,"token":"synthetic"}));
        })
        .await;
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap_or_else(|e| panic!("bind: {e}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|e| panic!("address: {e}"));
    let server = tokio::spawn(async move {
        let (mut socket, _) = accept_socket(&listener).await;
        complete_handshake(&mut socket).await;
        while let Some(Ok(message)) = socket.next().await {
            match message {
                Message::Text(text) => {
                    let value: Value = serde_json::from_str(text.trim_end_matches(TERMINATOR))
                        .unwrap_or_else(|e| panic!("request: {e}"));
                    let Some(id) = value.get("invocationId") else {
                        continue;
                    };
                    let completion = json!({"type":3,"invocationId":id});
                    let data = if value["target"] == "Trigger" {
                        records.clone()
                    } else {
                        format!("{}{TERMINATOR}", json!({"type":42,"after":true}))
                    };
                    socket
                        .send(Message::Text(
                            format!("{data}{completion}{TERMINATOR}").into(),
                        ))
                        .await
                        .unwrap_or_else(|e| panic!("response: {e}"));
                }
                Message::Close(_) => {
                    let _ = socket.flush().await;
                    break;
                }
                _ => {}
            }
        }
    });
    let client = fixture_client(&http, address);
    authenticate_fixture(&client).await;
    let realtime = client.realtime(Hub::Market);
    let mut events = realtime
        .take_event_receiver()
        .unwrap_or_else(|| panic!("receiver"));
    assert!(realtime.connect().await.is_ok());
    let first = events
        .recv_message()
        .await
        .unwrap_or_else(|| panic!("connected"));
    assert_eq!(first.event, RealtimeEvent::Connected);
    let session = realtime
        .session()
        .unwrap_or_else(|e| panic!("session: {e}"));
    assert_eq!(session.generation(), first.generation);
    // Completion must pass even though earlier data saturated or broke delivery.
    assert!(session.invoke("Trigger", Vec::new()).await.is_ok());
    for _ in 0..expected_prefix {
        let message = events
            .recv_message()
            .await
            .unwrap_or_else(|| panic!("prefix"));
        assert_eq!(message.generation, first.generation);
        assert!(matches!(message.event, RealtimeEvent::Message(_)));
    }
    let gap = events.recv_message().await.unwrap_or_else(|| panic!("gap"));
    assert_eq!(gap.generation, first.generation);
    assert_eq!(gap.event, RealtimeEvent::TransportGap);
    assert!(realtime.is_connected());
    events.acknowledge_transport_gap();
    assert!(session.invoke("Healthy", Vec::new()).await.is_ok());
    let after = events
        .recv_message()
        .await
        .unwrap_or_else(|| panic!("continued data"));
    assert_eq!(after.generation, first.generation);
    assert_eq!(
        after.event,
        RealtimeEvent::Message(json!({"type":42,"after":true}))
    );
    assert!(realtime.disconnect().await.is_ok());
    assert_eq!(events.recv().await, Some(RealtimeEvent::Disconnected));
    server.await.unwrap_or_else(|e| panic!("server: {e}"));
}

#[tokio::test]
async fn malformed_record_keeps_socket_and_processes_later_completion_in_batch() {
    exercise_nonterminal_gap(
        "{broken}\u{001e}{\"type\":3,\"invocationId\":null}\u{001e}".to_owned(),
        0,
    )
    .await;
}

#[tokio::test]
async fn saturated_events_keep_socket_and_process_completions_until_gap_ack() {
    use std::fmt::Write as _;
    let mut records = String::new();
    for sequence in 0..513 {
        write!(
            records,
            "{}{}",
            json!({"type":42,"sequence":sequence}),
            TERMINATOR
        )
        .unwrap_or_else(|e| panic!("fixture format: {e}"));
    }
    exercise_nonterminal_gap(records, 512).await;
}

#[tokio::test]
async fn quiet_socket_survives_inactivity_watchdog_interval() {
    let http = MockServer::start_async().await;
    let _login = http
        .mock_async(|when, then| {
            when.method(POST).path("/api/Auth/loginKey");
            then.status(200)
                .json_body(json!({"success":true,"errorCode":0,"token":"synthetic"}));
        })
        .await;
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap_or_else(|e| panic!("bind: {e}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|e| panic!("address: {e}"));
    let (seen, _seen) = oneshot::channel();
    let server = spawn_uncompleted_invocation_server(listener, seen);
    let client = fixture_client(&http, address);
    authenticate_fixture(&client).await;
    let realtime = client.realtime(Hub::Market);
    assert!(realtime.connect().await.is_ok());
    let generation = realtime
        .session()
        .unwrap_or_else(|e| panic!("session: {e}"))
        .generation();
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(90)).await;
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    tokio::time::resume();
    assert_eq!(
        realtime
            .session()
            .unwrap_or_else(|e| panic!("quiet session: {e}"))
            .generation(),
        generation
    );
    assert!(realtime.invoke("Healthy", Vec::new()).await.is_ok());
    assert!(realtime.disconnect().await.is_ok());
    server.await.unwrap_or_else(|e| panic!("server: {e}"));
}

#[tokio::test]
async fn failed_active_ping_ends_socket_and_explicit_disconnect_cancels_retry() {
    let http = MockServer::start_async().await;
    let _login = http
        .mock_async(|when, then| {
            when.method(POST).path("/api/Auth/loginKey");
            then.status(200)
                .json_body(json!({"success":true,"errorCode":0,"token":"synthetic"}));
        })
        .await;
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap_or_else(|e| panic!("bind: {e}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|e| panic!("address: {e}"));
    let (ready, ready_rx) = oneshot::channel();
    let (release, release_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = accept_socket(&listener).await;
        complete_handshake(&mut socket).await;
        let _ = ready.send(());
        // Hold TCP open without reading: an active WebSocket probe gets no pong.
        let _ = release_rx.await;
        drop(socket);
        // User Disconnect must have cancelled recovery, so there is no new socket.
        assert!(
            tokio::time::timeout(Duration::from_millis(100), listener.accept())
                .await
                .is_err()
        );
    });
    let client = fixture_client(&http, address);
    authenticate_fixture(&client).await;
    let realtime = client.realtime(Hub::Market);
    let mut events = realtime
        .take_event_receiver()
        .unwrap_or_else(|| panic!("receiver"));
    assert!(realtime.connect().await.is_ok());
    assert_eq!(events.recv().await, Some(RealtimeEvent::Connected));
    ready_rx.await.unwrap_or_else(|e| panic!("ready: {e}"));
    tokio::time::pause();
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }
    for _ in 0..60 {
        tokio::time::advance(Duration::from_secs(1)).await;
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        if !realtime.is_connected() {
            break;
        }
    }
    // The active probe deadline, rather than ordinary silence, ends this socket.
    assert!(!realtime.is_connected());
    assert!(realtime.disconnect().await.is_ok());
    tokio::time::advance(Duration::from_mins(1)).await;
    tokio::time::resume();
    let _ = release.send(());
    server.await.unwrap_or_else(|e| panic!("server: {e}"));
}

#[tokio::test]
async fn failed_probe_reconnects_with_a_fresh_generation() {
    let http = MockServer::start_async().await;
    let _login = http
        .mock_async(|when, then| {
            when.method(POST).path("/api/Auth/loginKey");
            then.status(200)
                .json_body(json!({"success":true,"errorCode":0,"token":"synthetic"}));
        })
        .await;
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap_or_else(|e| panic!("bind: {e}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|e| panic!("address: {e}"));
    let (release, release_rx) = oneshot::channel();
    // The first socket stops reading after handshake, leaving the active probe unanswered.
    let server = spawn_reconnect_server(listener, release_rx);
    let client = fixture_client(&http, address);
    authenticate_fixture(&client).await;
    let realtime = client.realtime(Hub::Market);
    let mut events = realtime
        .take_event_receiver()
        .unwrap_or_else(|| panic!("receiver"));
    assert!(realtime.connect().await.is_ok());
    let old = realtime
        .session()
        .unwrap_or_else(|e| panic!("session: {e}"));
    assert_eq!(events.recv().await, Some(RealtimeEvent::Connected));
    tokio::time::pause();
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }
    for _ in 0..60 {
        tokio::time::advance(Duration::from_secs(1)).await;
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        if !realtime.is_connected() {
            break;
        }
    }
    assert!(!realtime.is_connected());
    tokio::time::resume();
    let ended = events
        .recv_message()
        .await
        .unwrap_or_else(|| panic!("ended"));
    assert_eq!(ended.generation, old.generation());
    assert_eq!(ended.event, RealtimeEvent::Disconnected);
    let _ = release.send(());
    let replacement = tokio::time::timeout(Duration::from_secs(8), events.recv_message())
        .await
        .unwrap_or_else(|e| panic!("reconnect deadline: {e}"))
        .unwrap_or_else(|| panic!("replacement"));
    assert_eq!(replacement.event, RealtimeEvent::Reconnected);
    assert_ne!(replacement.generation, old.generation());
    assert!(matches!(
        old.invoke("Stale", Vec::new()).await,
        Err(RealtimeError::StaleGeneration)
    ));
    assert!(realtime.invoke("Healthy", Vec::new()).await.is_ok());
    assert!(realtime.disconnect().await.is_ok());
    server.await.unwrap_or_else(|e| panic!("server: {e}"));
}
