// SPDX-FileCopyrightText: 2026 Kevin Monaghan
// SPDX-License-Identifier: MIT-0

//! REST connection reuse across a quiet stretch, against a loopback fixture
//! that counts the connections it accepts.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use projectx_client::{Client, Credentials, Endpoints};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const PONG: &[u8] = b"HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\ncontent-length: 4\r\n\r\npong";

/// A keep-alive HTTP/1.1 server that answers every request with `pong` and
/// counts the connections it accepts.
async fn counting_ping_server() -> (String, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap_or_else(|error| panic!("loopback listener must bind: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("loopback listener must have an address: {error}"));
    let accepted = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&accepted);
    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            counter.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                let mut buffer = [0_u8; 4096];
                let mut pending = Vec::new();
                loop {
                    let read = match socket.read(&mut buffer).await {
                        Ok(0) | Err(_) => return,
                        Ok(read) => read,
                    };
                    pending.extend_from_slice(&buffer[..read]);
                    while let Some(end) =
                        pending.windows(4).position(|window| window == b"\r\n\r\n")
                    {
                        pending.drain(..end + 4);
                        if socket.write_all(PONG).await.is_err() {
                            return;
                        }
                    }
                }
            });
        }
    });
    (format!("http://{address}"), accepted)
}

fn fixture_client(base: &str) -> Client {
    let credentials = Credentials::new("synthetic-user", "synthetic-key")
        .unwrap_or_else(|error| panic!("fixture credentials must be valid: {error}"));
    let endpoints = Endpoints::custom(base, base)
        .unwrap_or_else(|error| panic!("fixture URL must be valid: {error}"));
    Client::builder(credentials)
        .endpoints(endpoints)
        .max_retries(0)
        .build()
        .unwrap_or_else(|error| panic!("fixture client must build: {error}"))
}

#[tokio::test]
async fn a_rest_connection_left_idle_for_two_minutes_is_reused() {
    let (base, accepted) = counting_ping_server().await;
    let client = fixture_client(&base);
    client
        .ping()
        .await
        .unwrap_or_else(|error| panic!("first ping must succeed: {error}"));

    tokio::time::pause();
    tokio::time::advance(Duration::from_mins(2)).await;
    tokio::time::resume();

    client
        .ping()
        .await
        .unwrap_or_else(|error| panic!("second ping must succeed: {error}"));
    assert_eq!(accepted.load(Ordering::SeqCst), 1);
}
