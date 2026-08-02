// SPDX-FileCopyrightText: 2026 Kevin Monaghan
// SPDX-License-Identifier: MIT

//! Regression test proving dependency logging cannot expose the WebSocket bearer.

use std::sync::Mutex;

use futures_util::{SinkExt as _, StreamExt as _};
use log::{LevelFilter, Log, Metadata, Record};
use projectx_client::{Client, Credentials, Endpoints, Hub};
use sha1::{Digest as _, Sha1};
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::TcpListener,
};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{Message, protocol::Role},
};

const SYNTHETIC_BEARER: &str = "synthetic-bearer-never-log-4f328b";

struct CapturingLogger {
    records: Mutex<Vec<String>>,
}

impl Log for CapturingLogger {
    fn enabled(&self, _metadata: &Metadata<'_>) -> bool {
        true
    }

    fn log(&self, record: &Record<'_>) {
        let mut records = self
            .records
            .lock()
            .unwrap_or_else(|error| panic!("fixture log capture must lock: {error}"));
        records.push(format!("{} {}", record.target(), record.args()));
    }

    fn flush(&self) {}
}

static LOGGER: CapturingLogger = CapturingLogger {
    records: Mutex::new(Vec::new()),
};

type FixtureServer = (std::net::SocketAddr, tokio::task::JoinHandle<()>);

async fn read_http_request(stream: &mut tokio::net::TcpStream) -> String {
    let mut request = Vec::new();
    let mut chunk = [0_u8; 2_048];
    loop {
        let read = stream
            .read(&mut chunk)
            .await
            .unwrap_or_else(|error| panic!("fixture HTTP request must read: {error}"));
        if read == 0 {
            break;
        }
        request.extend_from_slice(&chunk[..read]);
        let Some(header_end) = request.windows(4).position(|window| window == b"\r\n\r\n") else {
            continue;
        };
        let header_end = header_end + 4;
        let headers = String::from_utf8_lossy(&request[..header_end]);
        let content_length = headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().ok())
                    .flatten()
            })
            .unwrap_or(0);
        if request.len() >= header_end.saturating_add(content_length) {
            break;
        }
    }
    String::from_utf8(request)
        .unwrap_or_else(|error| panic!("fixture HTTP request must be UTF-8: {error}"))
}

async fn spawn_rest_server() -> FixtureServer {
    let rest_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap_or_else(|error| panic!("fixture REST listener must bind: {error}"));
    let rest_address = rest_listener
        .local_addr()
        .unwrap_or_else(|error| panic!("fixture REST listener must have an address: {error}"));
    let rest_server = tokio::spawn(async move {
        let (mut stream, _) = rest_listener
            .accept()
            .await
            .unwrap_or_else(|error| panic!("fixture login must accept: {error}"));
        let request = read_http_request(&mut stream).await;
        assert!(request.starts_with("POST /api/Auth/loginKey "));
        let body = format!(r#"{{"success":true,"errorCode":0,"token":"{SYNTHETIC_BEARER}"}}"#);
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        stream
            .write_all(response.as_bytes())
            .await
            .unwrap_or_else(|error| panic!("fixture login response must write: {error}"));
    });
    (rest_address, rest_server)
}

async fn spawn_realtime_server() -> FixtureServer {
    let realtime_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap_or_else(|error| panic!("fixture realtime listener must bind: {error}"));
    let realtime_address = realtime_listener
        .local_addr()
        .unwrap_or_else(|error| panic!("fixture realtime listener must have an address: {error}"));
    let realtime_server = tokio::spawn(async move {
        let (mut stream, _) = realtime_listener
            .accept()
            .await
            .unwrap_or_else(|error| panic!("fixture realtime socket must accept: {error}"));
        let request = read_http_request(&mut stream).await;
        let expected_request_prefix = format!("GET /hubs/market?access_token={SYNTHETIC_BEARER} ");
        assert!(request.starts_with(expected_request_prefix.as_str()));
        let key = request
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("sec-websocket-key")
                    .then(|| value.trim())
            })
            .unwrap_or_else(|| panic!("fixture upgrade must contain a WebSocket key"));
        let mut digest = Sha1::new();
        digest.update(key.as_bytes());
        digest.update(b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
        let accept = data_encoding::BASE64.encode(&digest.finalize());
        let mut response = format!(
            "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: {accept}\r\n\r\n"
        )
        .into_bytes();
        // Coalesce an unmasked server text frame containing the SignalR
        // handshake response with the HTTP upgrade. Hyper's upgraded stream
        // must preserve these post-header bytes for tungstenite.
        response.extend_from_slice(&[0x81, 0x03, b'{', b'}', 0x1e]);
        stream
            .write_all(&response)
            .await
            .unwrap_or_else(|error| panic!("fixture upgrade response must write: {error}"));
        let mut socket = WebSocketStream::from_raw_socket(stream, Role::Server, None).await;
        let handshake = socket
            .next()
            .await
            .unwrap_or_else(|| panic!("SignalR handshake request must arrive"))
            .unwrap_or_else(|error| panic!("SignalR handshake request must decode: {error}"));
        assert!(matches!(handshake, Message::Text(_)));
        while let Some(message) = socket.next().await {
            match message {
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
    (realtime_address, realtime_server)
}

async fn exercise_client(
    rest_address: std::net::SocketAddr,
    realtime_address: std::net::SocketAddr,
) {
    let endpoints = Endpoints::custom(
        &format!("http://{rest_address}"),
        &format!("http://{realtime_address}"),
    )
    .unwrap_or_else(|error| panic!("fixture endpoints must be valid: {error}"));
    let credentials = Credentials::new("synthetic-user", "synthetic-key")
        .unwrap_or_else(|error| panic!("fixture credentials must be valid: {error}"));
    let client = Client::builder(credentials)
        .endpoints(endpoints)
        .max_retries(0)
        .build()
        .unwrap_or_else(|error| panic!("fixture client must build: {error}"));
    client
        .authenticate()
        .await
        .unwrap_or_else(|error| panic!("fixture authentication must succeed: {error}"));
    let realtime = client.realtime(Hub::Market);
    realtime
        .connect()
        .await
        .unwrap_or_else(|error| panic!("fixture realtime connection must succeed: {error}"));
    realtime
        .disconnect()
        .await
        .unwrap_or_else(|error| panic!("fixture realtime connection must close: {error}"));
}

fn assert_bearer_was_not_logged() {
    let records = LOGGER
        .records
        .lock()
        .unwrap_or_else(|error| panic!("fixture log capture must lock: {error}"));
    assert!(
        records
            .iter()
            .all(|record| !record.contains(SYNTHETIC_BEARER)),
        "no dependency log record may expose the synthetic bearer"
    );
}

#[tokio::test]
async fn trace_logging_never_contains_the_realtime_bearer() {
    log::set_logger(&LOGGER)
        .unwrap_or_else(|error| panic!("fixture logger must install exactly once: {error}"));
    log::set_max_level(LevelFilter::Trace);

    let (rest_address, rest_server) = spawn_rest_server().await;
    let (realtime_address, realtime_server) = spawn_realtime_server().await;
    exercise_client(rest_address, realtime_address).await;
    rest_server
        .await
        .unwrap_or_else(|error| panic!("fixture REST server must join: {error}"));
    realtime_server
        .await
        .unwrap_or_else(|error| panic!("fixture realtime server must join: {error}"));
    assert_bearer_was_not_logged();
}
