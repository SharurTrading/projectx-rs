// SPDX-FileCopyrightText: 2026 Kevin Monaghan
// SPDX-License-Identifier: MIT-0

use thiserror::Error;

use crate::Error as ClientError;

/// Errors returned by the real-time transport.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum RealtimeError {
    /// Authentication has not produced a bearer token.
    #[error("authentication is required before connecting a real-time hub")]
    MissingAuthToken,
    /// Endpoint configuration could not form the hub URL.
    #[error("real-time endpoint configuration is invalid")]
    Endpoint(#[source] ClientError),
    /// The WebSocket transport failed. Details are intentionally omitted
    /// because the connection URL contains a bearer token.
    #[error("WebSocket transport failed")]
    Transport,
    /// The peer did not answer the active WebSocket ping within its deadline.
    #[error("WebSocket ping/pong check timed out")]
    PingTimedOut,
    /// The `SignalR` handshake failed.
    #[error("SignalR handshake failed: {0}")]
    Handshake(&'static str),
    /// A `SignalR` or event payload was malformed.
    #[error("SignalR protocol error: {0}")]
    Protocol(&'static str),
    /// JSON decoding failed.
    #[error("SignalR JSON decoding failed")]
    Decode(#[source] serde_json::Error),
    /// JSON encoding failed before an invocation reached the writer queue.
    #[error("SignalR invocation JSON encoding failed")]
    Encode(#[source] serde_json::Error),
    /// The client is already connected.
    #[error("real-time client is already connected")]
    AlreadyConnected,
    /// Connection setup was cancelled by a concurrent disconnect or shutdown.
    #[error("real-time connection setup was cancelled")]
    ConnectionCancelled,
    /// The WebSocket upgrade did not complete within its bounded deadline.
    #[error("real-time WebSocket upgrade timed out")]
    ConnectionTimedOut,
    /// The client is not connected.
    #[error("real-time client is not connected")]
    NotConnected,
    /// The captured socket ended before queue admission; nothing was sent.
    #[error("real-time socket generation is no longer current")]
    StaleGeneration,
    /// The typed subscription does not belong to this client's hub.
    #[error("subscription is not valid for this real-time hub")]
    WrongHub,
    /// The bounded writer queue is full.
    #[error("real-time writer queue is full")]
    SendQueueFull,
    /// The writer task or channel closed.
    #[error("real-time writer is closed")]
    SendClosed,
    /// Reconnection is fenced until the caller acknowledges a transport gap.
    #[error("real-time reconnect is fenced by an unacknowledged transport gap")]
    TransportGapPending,
    /// The single event receiver was dropped.
    #[error("real-time event receiver is closed")]
    EventReceiverClosed,
    /// The pending invocation bound was reached.
    #[error("pending SignalR invocation capacity is exhausted")]
    PendingInvocationCapacity,
    /// A monotonic transport identifier reached its numeric bound.
    #[error("real-time transport identifier capacity is exhausted")]
    IdentifierCapacity,
    /// A `SignalR` invocation was rejected by the provider.
    #[error("SignalR invocation `{target}` was rejected")]
    InvocationRejected {
        /// Provider invocation target.
        target: String,
    },
    /// A `SignalR` invocation timed out.
    #[error("SignalR invocation `{target}` timed out")]
    InvocationTimedOut {
        /// Provider invocation target.
        target: String,
    },
    /// The session ended before an invocation completed.
    #[error("SignalR invocation `{target}` ended with its session")]
    InvocationSessionEnded {
        /// Provider invocation target.
        target: String,
    },
    /// Graceful close did not complete.
    #[error("real-time close handshake did not complete")]
    Close,
}
