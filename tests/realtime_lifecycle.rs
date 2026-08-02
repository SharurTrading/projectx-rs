// SPDX-FileCopyrightText: 2026 Kevin Monaghan
// SPDX-License-Identifier: MIT

//! Public real-time lifecycle tests using synthetic local fixtures.

use std::time::Duration;

use futures_util::{SinkExt as _, StreamExt as _};
use httpmock::prelude::*;
use projectx_client::{
    Client, ContractId, Credentials, Endpoints, Hub, RealtimeError, RealtimeEvent,
    SignalRInvocation,
};
use serde_json::{Value, json};
use tokio::{net::TcpListener, sync::oneshot};
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
                Ok(Message::Close(_)) | Err(_) => break,
                Ok(_) => {}
            }
        }
        (first_uri, second_uri)
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
                "token": "initial synthetic token+/="
            }));
        })
        .await;
    let _validate = http
        .mock_async(|when, then| {
            when.method(POST).path("/api/Auth/validate");
            then.status(200).json_body(json!({
                "success": true,
                "newToken": "rotated/synthetic token=="
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
        .await
        .unwrap_or_else(|| panic!("event receiver must be available once"));
    realtime
        .connect()
        .await
        .unwrap_or_else(|error| panic!("initial websocket must connect: {error}"));
    assert!(matches!(
        events.recv().await,
        Some(RealtimeEvent::Connected)
    ));

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
    assert!(has_token(&first_uri, "initial synthetic token+/="));
    assert!(has_token(&second_uri, "rotated/synthetic token=="));
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
    let value: Value = serde_json::from_str(
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
    .unwrap_or_else(|error| panic!("fixture JSON must decode: {error}"));
    let invocation = SignalRInvocation::from_value(&value)
        .and_then(|value| value.ok_or(RealtimeError::Protocol("missing invocation")))
        .unwrap_or_else(|error| panic!("fixture invocation must decode: {error}"));
    let trade: projectx_client::MarketTrade = invocation
        .decode()
        .unwrap_or_else(|error| panic!("fixture market trade must decode: {error}"));
    assert_eq!(trade.price.to_string(), "0.1000000000000000000000000001");
}
