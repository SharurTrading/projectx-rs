// SPDX-FileCopyrightText: 2026 Kevin Monaghan
// SPDX-License-Identifier: MIT-0

//! WebSocket upgrade, `SignalR` handshake, and the socket reader and writer tasks.

use std::{sync::Weak, time::Duration};

use data_encoding::BASE64;
use futures_util::{SinkExt as _, StreamExt as _};
use rand::{TryRng as _, rngs::SysRng};
use reqwest::{StatusCode, Version, header};
use serde_json::Value;
use sha1::{Digest as _, Sha1};
use tokio::{sync::mpsc, time::Instant};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{
        Error as TungsteniteError, Message,
        protocol::{Role, WebSocketConfig},
    },
};
use tokio_util::sync::CancellationToken;

use super::error::RealtimeError;
use super::invocation::SIGNALR_TERMINATOR;
use super::lifecycle::{CLOSE_TIMEOUT, ProcessOutcome, RealtimeInner, Session};

const SIGNALR_PING: &str = "{\"type\":6}\u{001e}";
// Fixed I/O chunk sizes are throughput choices, not message-size ceilings.
pub(super) const WEBSOCKET_READ_BUFFER_SIZE: usize = 64 * 1_024;
pub(super) const WEBSOCKET_WRITE_BUFFER_SIZE: usize = 64 * 1_024;
const CLIENT_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(15);
// Active WebSocket liveness: probe every 15 seconds; one unanswered probe
// for that interval proves a failed ping/pong check, never mere data silence.
const SOCKET_PROBE_INTERVAL: Duration = Duration::from_secs(15);
const WEBSOCKET_GUID: &[u8] = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

pub(super) async fn upgrade_websocket(
    http: &reqwest::Client,
    mut url: url::Url,
) -> Result<WebSocketStream<reqwest::Upgraded>, RealtimeError> {
    let http_scheme = match url.scheme() {
        "wss" => "https",
        "ws" => "http",
        _ => return Err(RealtimeError::Transport),
    };
    url.set_scheme(http_scheme)
        .map_err(|()| RealtimeError::Transport)?;

    let key = websocket_key()?;
    let expected_accept = websocket_accept(&key);
    let response = http
        .get(url)
        .version(Version::HTTP_11)
        .header(header::CONNECTION, "Upgrade")
        .header(header::UPGRADE, "websocket")
        .header(header::SEC_WEBSOCKET_VERSION, "13")
        .header(header::SEC_WEBSOCKET_KEY, key)
        .send()
        .await
        .map_err(|_| RealtimeError::Transport)?;
    validate_websocket_upgrade(
        response.status(),
        response.version(),
        response.headers(),
        &expected_accept,
    )?;
    let upgraded = response
        .upgrade()
        .await
        .map_err(|_| RealtimeError::Transport)?;
    Ok(WebSocketStream::from_raw_socket(upgraded, Role::Client, Some(websocket_config())).await)
}

pub(super) fn websocket_key() -> Result<String, RealtimeError> {
    let mut nonce = [0_u8; 16];
    SysRng
        .try_fill_bytes(&mut nonce)
        .map_err(|_| RealtimeError::Transport)?;
    Ok(BASE64.encode(&nonce))
}

pub(super) fn websocket_accept(key: &str) -> String {
    let mut digest = Sha1::new();
    digest.update(key.as_bytes());
    digest.update(WEBSOCKET_GUID);
    BASE64.encode(&digest.finalize())
}

pub(super) fn validate_websocket_upgrade(
    status: StatusCode,
    version: Version,
    headers: &header::HeaderMap,
    expected_accept: &str,
) -> Result<(), RealtimeError> {
    let mut accept_values = headers.get_all(header::SEC_WEBSOCKET_ACCEPT).iter();
    let accept_matches = accept_values
        .next()
        .is_some_and(|value| value.as_bytes() == expected_accept.as_bytes())
        && accept_values.next().is_none();
    if status != StatusCode::SWITCHING_PROTOCOLS
        || version != Version::HTTP_11
        || !header_contains_token(headers, &header::CONNECTION, "upgrade")
        || !header_contains_token(headers, &header::UPGRADE, "websocket")
        || !accept_matches
        || headers.contains_key(header::SEC_WEBSOCKET_EXTENSIONS)
        || headers.contains_key(header::SEC_WEBSOCKET_PROTOCOL)
    {
        return Err(RealtimeError::Transport);
    }
    Ok(())
}

fn header_contains_token(
    headers: &header::HeaderMap,
    name: &header::HeaderName,
    expected: &str,
) -> bool {
    headers.get_all(name).iter().any(|value| {
        value.to_str().is_ok_and(|value| {
            value
                .split(',')
                .any(|token| token.trim().eq_ignore_ascii_case(expected))
        })
    })
}

pub(super) fn websocket_config() -> WebSocketConfig {
    WebSocketConfig::default()
        .read_buffer_size(WEBSOCKET_READ_BUFFER_SIZE)
        .write_buffer_size(WEBSOCKET_WRITE_BUFFER_SIZE)
        // The writer flushes every dequeued message. Bounded control admission
        // limits queued work; provider payloads have no invented byte ceilings.
        .max_message_size(None)
        .max_frame_size(None)
}

pub(super) fn send_queued(
    writer: &mpsc::Sender<Message>,
    message: Message,
) -> Result<(), RealtimeError> {
    writer.try_send(message).map_err(|error| match error {
        mpsc::error::TrySendError::Full(_) => RealtimeError::SendQueueFull,
        mpsc::error::TrySendError::Closed(_) => RealtimeError::SendClosed,
    })
}

pub(super) async fn join_socket_tasks(session: &mut Session, graceful: bool) -> bool {
    let deadline = tokio::time::sleep(CLOSE_TIMEOUT);
    tokio::pin!(deadline);
    let mut reader_done = false;
    let mut writer_done = false;
    let mut completed_cleanly = true;
    loop {
        if reader_done && writer_done {
            return completed_cleanly;
        }
        tokio::select! {
            result = &mut session.reader_task, if !reader_done => {
                reader_done = true;
                // Forced retirement promises joined teardown, while graceful
                // disconnect also requires both socket operations to succeed.
                completed_cleanly &= !graceful || matches!(result, Ok(Ok(())));
            }
            result = &mut session.writer_task, if !writer_done => {
                writer_done = true;
                completed_cleanly &= !graceful || matches!(result, Ok(Ok(())));
            }
            () = &mut deadline => break,
        }
    }
    if !reader_done {
        session.reader_task.abort();
        let _ = (&mut session.reader_task).await;
    }
    if !writer_done {
        session.writer_task.abort();
        let _ = (&mut session.writer_task).await;
    }
    false
}

pub(super) async fn negotiate_handshake<S>(stream: &mut S) -> Result<Option<String>, RealtimeError>
where
    S: futures_util::Stream<Item = Result<Message, TungsteniteError>>
        + futures_util::Sink<Message, Error = TungsteniteError>
        + Unpin,
{
    const SIGNALR_HANDSHAKE: &str = "{\"protocol\":\"json\",\"version\":1}\u{001e}";
    stream
        .send(Message::Text(SIGNALR_HANDSHAKE.into()))
        .await
        .map_err(|_| RealtimeError::Transport)?;
    loop {
        match stream.next().await {
            Some(Ok(Message::Text(text))) => return validate_handshake(text.as_ref()),
            Some(Ok(Message::Binary(bytes))) => {
                let text = std::str::from_utf8(bytes.as_ref())
                    .map_err(|_| RealtimeError::Handshake("response was not UTF-8"))?;
                return validate_handshake(text);
            }
            // Tungstenite queues and flushes the matching Pong automatically on
            // the next read. Sending one here would duplicate control traffic.
            Some(Ok(Message::Ping(_) | Message::Pong(_) | Message::Frame(_))) => {}
            Some(Ok(Message::Close(_))) | None => {
                return Err(RealtimeError::Handshake(
                    "connection closed before the response",
                ));
            }
            Some(Err(_)) => return Err(RealtimeError::Transport),
        }
    }
}

pub(super) async fn run_writer<S>(
    inner: Weak<RealtimeInner>,
    generation: u64,
    mut write: S,
    mut messages: mpsc::Receiver<Message>,
    cancellation: CancellationToken,
    start: CancellationToken,
) -> Result<(), RealtimeError>
where
    S: futures_util::Sink<Message, Error = TungsteniteError> + Unpin,
{
    tokio::select! {
        biased;
        () = cancellation.cancelled() => {
            let _ = tokio::time::timeout(
                Duration::from_secs(1),
                write.send(Message::Close(None)),
            )
            .await;
            return Ok(());
        }
        () = start.cancelled() => {}
    }
    let keepalive = tokio::time::sleep(CLIENT_KEEPALIVE_INTERVAL);
    tokio::pin!(keepalive);
    let probe_tick = tokio::time::sleep(SOCKET_PROBE_INTERVAL);
    tokio::pin!(probe_tick);
    let mut probe_id = 0_u64;
    let result = loop {
        let message = tokio::select! {
            biased;
            () = cancellation.cancelled() => {
                let _ = tokio::time::timeout(
                    Duration::from_secs(1),
                    write.send(Message::Close(None)),
                )
                .await;
                break Ok(());
            },
            () = &mut probe_tick => {
                let Some(inner) = inner.upgrade() else { break Ok(()); };
                let mut probe = inner.socket_probe.lock();
                if probe_id != 0 && *probe == Some((generation, probe_id)) {
                    break Err(RealtimeError::PingTimedOut);
                }
                let Some(next) = probe_id.checked_add(1) else { break Err(RealtimeError::IdentifierCapacity); };
                probe_id = next;
                *probe = Some((generation, probe_id));
                let mut payload = Vec::with_capacity(16);
                payload.extend_from_slice(&generation.to_be_bytes());
                payload.extend_from_slice(&probe_id.to_be_bytes());
                probe_tick.as_mut().reset(Instant::now() + SOCKET_PROBE_INTERVAL);
                Message::Ping(payload.into())
            }
            message = messages.recv() => {
                match message {
                    Some(message) => message,
                    None => break Err(RealtimeError::SendClosed),
                }
            }
            () = &mut keepalive => Message::Text(SIGNALR_PING.into()),
        };
        let is_close = matches!(message, Message::Close(_));
        let is_signalr = matches!(message, Message::Text(_) | Message::Binary(_));
        if write.send(message).await.is_err() {
            break Err(RealtimeError::Transport);
        }
        if is_close {
            break Ok(());
        }
        if is_signalr {
            keepalive
                .as_mut()
                .reset(Instant::now() + CLIENT_KEEPALIVE_INTERVAL);
        }
    };
    if result.is_err() && !cancellation.is_cancelled() {
        cancellation.cancel();
        if let Some(inner) = inner.upgrade() {
            inner.end_generation(generation);
        }
    }
    result
}

pub(super) async fn run_reader<S>(
    inner: Weak<RealtimeInner>,
    generation: u64,
    mut read: S,
    handshake_tail: Option<String>,
    cancellation: CancellationToken,
    start: CancellationToken,
) -> Result<(), RealtimeError>
where
    S: futures_util::Stream<Item = Result<Message, TungsteniteError>> + Unpin,
{
    tokio::select! {
        biased;
        () = cancellation.cancelled() => return Ok(()),
        () = start.cancelled() => {}
    }
    let result = async {
        if let Some(tail) = handshake_tail {
            let Some(inner) = inner.upgrade() else {
                return Ok(());
            };
            if inner.process_text(generation, &tail).await? == ProcessOutcome::Close {
                return Ok(());
            }
        }
        loop {
            let message = tokio::select! {
                biased;
                () = cancellation.cancelled() => return Ok(()),
                message = read.next() => message,
            };
            if let Some(owner) = inner.upgrade() {
                owner.ensure_event_receiver()?;
            }
            match message {
                Some(Ok(Message::Text(text))) => {
                    let Some(inner) = inner.upgrade() else {
                        return Ok(());
                    };
                    let outcome = inner.process_text(generation, text.as_ref()).await?;
                    if outcome == ProcessOutcome::Close {
                        return Ok(());
                    }
                }
                Some(Ok(Message::Binary(bytes))) => {
                    let Some(inner) = inner.upgrade() else {
                        return Ok(());
                    };
                    let Ok(text) = std::str::from_utf8(bytes.as_ref()) else {
                        inner.event_flow.mark_gap(generation);
                        continue;
                    };
                    let outcome = inner.process_text(generation, text).await?;
                    if outcome == ProcessOutcome::Close {
                        return Ok(());
                    }
                }
                // Tungstenite automatically queues the matching Pong.
                Some(Ok(Message::Ping(_) | Message::Frame(_))) => {}
                Some(Ok(Message::Close(_))) | None => return Ok(()),
                Some(Err(_)) => return Err(RealtimeError::Transport),
                Some(Ok(Message::Pong(payload))) => {
                    let Some(inner) = inner.upgrade() else {
                        return Ok(());
                    };
                    inner.acknowledge_probe(&payload);
                }
            }
        }
    }
    .await;
    if !cancellation.is_cancelled() {
        cancellation.cancel();
        if let Some(inner) = inner.upgrade() {
            inner.end_generation(generation);
        }
    }
    result
}

pub(super) fn validate_handshake(text: &str) -> Result<Option<String>, RealtimeError> {
    let (payload, tail) = text
        .split_once(SIGNALR_TERMINATOR)
        .ok_or(RealtimeError::Handshake("response frame was incomplete"))?;
    let response: Value = serde_json::from_str(payload)
        .map_err(|_| RealtimeError::Handshake("response was invalid JSON"))?;
    if response
        .get("error")
        .and_then(Value::as_str)
        .is_some_and(|detail| !detail.trim().is_empty())
    {
        return Err(RealtimeError::Handshake("provider rejected the handshake"));
    }
    if response.as_object().is_some_and(serde_json::Map::is_empty) {
        Ok((!tail.is_empty()).then(|| tail.to_owned()))
    } else {
        Err(RealtimeError::Handshake(
            "provider returned an unexpected response shape",
        ))
    }
}
