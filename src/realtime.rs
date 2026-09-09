// SPDX-FileCopyrightText: 2026 Kevin Monaghan
// SPDX-License-Identifier: MIT-0

//! `ProjectX` `SignalR`-over-WebSocket transport.

use std::{
    collections::BTreeMap,
    fmt,
    io::{self, Write as _},
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use data_encoding::BASE64;
use futures_util::{SinkExt as _, StreamExt as _};
use parking_lot::Mutex as ParkingMutex;
use rand::{TryRng as _, rngs::SysRng};
use reqwest::{StatusCode, Version, header};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use serde_json::value::RawValue;
use sha1::{Digest as _, Sha1};
use thiserror::Error;
use tokio::{
    sync::{Notify, mpsc, oneshot},
    task::JoinHandle,
    time::Instant,
};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{
        Error as TungsteniteError, Message,
        protocol::{Role, WebSocketConfig},
    },
};
use tokio_util::sync::CancellationToken;

use crate::{AccountId, ContractId, Endpoints, Error as ClientError, token::TokenStore};

const SIGNALR_TERMINATOR: char = '\u{001e}';
const SIGNALR_PING: &str = "{\"type\":6}\u{001e}";
const WRITER_CAPACITY: usize = 256;
const EVENT_CAPACITY: usize = 512;
const PENDING_INVOCATION_CAPACITY: usize = WRITER_CAPACITY;
const EVENT_BYTE_BUDGET: usize = 32 * 1_024 * 1_024;
const EVENT_DECODED_WEIGHT_MULTIPLIER: usize = 16;
const EVENT_BASE_WEIGHT: usize = 256;
// ProjectX messages are normally small JSON frames. These ceilings leave ample room for
// provider-side batching while preventing a peer or stalled socket from growing memory without
// bound. The write ceiling accommodates the target buffer plus multiple maximum-size invocations.
const WEBSOCKET_READ_BUFFER_SIZE: usize = 64 * 1_024;
const WEBSOCKET_WRITE_BUFFER_SIZE: usize = 64 * 1_024;
const WEBSOCKET_MAX_WRITE_BUFFER_SIZE: usize = 256 * 1_024;
const WEBSOCKET_MAX_MESSAGE_SIZE: usize = 1_024 * 1_024;
const WEBSOCKET_MAX_FRAME_SIZE: usize = 256 * 1_024;
const MAX_OUTBOUND_INVOCATION_SIZE: usize = 64 * 1_024;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const COMPLETION_TIMEOUT: Duration = Duration::from_secs(15);
const CLOSE_TIMEOUT: Duration = Duration::from_secs(5);
const CLIENT_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(15);
// Active WebSocket liveness: probe every 15 seconds; one unanswered probe
// for that interval proves a failed ping/pong check, never mere data silence.
const SOCKET_PROBE_INTERVAL: Duration = Duration::from_secs(15);
const WATCHDOG_INTERVAL: Duration = Duration::from_secs(5);
const WEBSOCKET_GUID: &[u8] = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

type PendingInvocation = oneshot::Sender<Result<(), ()>>;

struct PendingEntry {
    generation: u64,
    reply: PendingInvocation,
}

type PendingInvocations = Arc<ParkingMutex<BTreeMap<String, PendingEntry>>>;

struct BoundedBuffer {
    bytes: Vec<u8>,
    limit: usize,
    exceeded: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct OutboundInvocation<'a> {
    #[serde(rename = "type")]
    message_type: u8,
    invocation_id: &'a str,
    target: &'a str,
    arguments: &'a [Value],
}

impl BoundedBuffer {
    fn new(limit: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(limit.min(4 * 1_024)),
            limit,
            exceeded: false,
        }
    }

    fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}

impl io::Write for BoundedBuffer {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        if self
            .bytes
            .len()
            .checked_add(buffer.len())
            .is_none_or(|length| length > self.limit)
        {
            self.exceeded = true;
            return Err(io::Error::other("bounded SignalR message limit reached"));
        }
        self.bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// `ProjectX` real-time hub.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum Hub {
    /// Market quotes, trades, and depth.
    Market,
    /// Account, order, position, and execution updates.
    User,
}

impl Hub {
    const fn path(self) -> &'static str {
        match self {
            Self::Market => "market",
            Self::User => "user",
        }
    }
}

/// A decoded `SignalR` invocation frame.
#[derive(Clone, Debug)]
pub struct SignalRInvocation {
    target: String,
    contract_id: Option<ContractId>,
    payload: Value,
    raw_entity: Box<RawValue>,
}

impl PartialEq for SignalRInvocation {
    fn eq(&self, other: &Self) -> bool {
        self.target == other.target
            && self.contract_id == other.contract_id
            && self.payload == other.payload
            && self.raw_entity.get() == other.raw_entity.get()
    }
}

impl SignalRInvocation {
    /// Decodes a type-1 `SignalR` invocation.
    ///
    /// Non-invocation frames return `Ok(None)`.
    ///
    /// # Errors
    ///
    /// Returns an error when a type-1 frame has a missing or malformed target,
    /// contract identifier, or payload argument list.
    pub fn from_value(value: Value) -> Result<Option<Self>, RealtimeError> {
        if value.get("type").and_then(Value::as_i64) != Some(1) {
            return Ok(None);
        }
        let Value::Object(mut object) = value else {
            return Err(RealtimeError::Protocol(
                "invocation frame was not an object",
            ));
        };
        let target = object
            .remove("target")
            .and_then(|target| target.as_str().map(str::to_owned))
            .ok_or(RealtimeError::Protocol("invocation target is missing"))?;
        let Value::Array(arguments) = object
            .remove("arguments")
            .ok_or(RealtimeError::Protocol("invocation arguments are missing"))?
        else {
            return Err(RealtimeError::Protocol(
                "invocation arguments were not an array",
            ));
        };
        let mut arguments = arguments.into_iter();
        let first = arguments
            .next()
            .ok_or(RealtimeError::Protocol("invocation payload is missing"))?;
        let (contract_id, payload) = match arguments.next() {
            Some(second) => {
                let contract_id = ContractId::new(first.as_str().ok_or(
                    RealtimeError::Protocol("market contract identifier was not a string"),
                )?)
                .map_err(|_| RealtimeError::Protocol("contract identifier is invalid"))?;
                (Some(contract_id), second)
            }
            None => (None, first),
        };
        if arguments.next().is_some() {
            return Err(RealtimeError::Protocol(
                "invocation contained too many arguments",
            ));
        }
        let entity = payload
            .get("data")
            .filter(|data| data.is_object() || data.is_array())
            .unwrap_or(&payload);
        let raw_entity =
            RawValue::from_string(serde_json::to_string(entity).map_err(RealtimeError::Decode)?)
                .map_err(RealtimeError::Decode)?;
        Ok(Some(Self {
            target,
            contract_id,
            payload,
            raw_entity,
        }))
    }

    /// Decodes a type-1 `SignalR` invocation directly from its JSON record.
    ///
    /// Unlike [`Self::from_value`], this path retains the original JSON token
    /// for the event entity so exact provider decimals do not first pass
    /// through a floating-point `serde_json::Value`.
    ///
    /// # Errors
    ///
    /// Returns an error when the record is malformed or has an invalid target,
    /// contract identifier, or payload argument list.
    pub fn from_json(json: &str) -> Result<Option<Self>, RealtimeError> {
        #[derive(Deserialize)]
        struct InvocationFrame {
            #[serde(rename = "type")]
            message_type: u64,
            target: Option<String>,
            arguments: Option<Vec<Box<RawValue>>>,
        }

        let frame: InvocationFrame = serde_json::from_str(json).map_err(RealtimeError::Decode)?;
        if frame.message_type != 1 {
            return Ok(None);
        }
        let target = frame
            .target
            .ok_or(RealtimeError::Protocol("invocation target is missing"))?;
        let mut arguments = frame
            .arguments
            .ok_or(RealtimeError::Protocol("invocation arguments are missing"))?
            .into_iter();
        let first = arguments
            .next()
            .ok_or(RealtimeError::Protocol("invocation payload is missing"))?;
        let (contract_id, raw_payload) = match arguments.next() {
            Some(second) => {
                let contract = serde_json::from_str::<String>(first.get()).map_err(|_| {
                    RealtimeError::Protocol("market contract identifier was not a string")
                })?;
                let contract_id = ContractId::new(contract)
                    .map_err(|_| RealtimeError::Protocol("contract identifier is invalid"))?;
                (Some(contract_id), second)
            }
            None => (None, first),
        };
        if arguments.next().is_some() {
            return Err(RealtimeError::Protocol(
                "invocation contained too many arguments",
            ));
        }
        let payload: Value =
            serde_json::from_str(raw_payload.get()).map_err(RealtimeError::Decode)?;
        let raw_entity = if payload
            .get("data")
            .is_some_and(|data| data.is_object() || data.is_array())
        {
            let mut object =
                serde_json::from_str::<BTreeMap<String, Box<RawValue>>>(raw_payload.get())
                    .map_err(RealtimeError::Decode)?;
            object.remove("data").ok_or(RealtimeError::Protocol(
                "invocation data envelope is missing",
            ))?
        } else {
            raw_payload
        };
        Ok(Some(Self {
            target,
            contract_id,
            payload,
            raw_entity,
        }))
    }

    /// Returns the provider invocation target.
    #[must_use]
    pub fn target(&self) -> &str {
        &self.target
    }

    /// Returns the market contract argument, when supplied.
    #[must_use]
    pub fn contract_id(&self) -> Option<&ContractId> {
        self.contract_id.as_ref()
    }

    /// Returns the raw provider payload.
    #[must_use]
    pub fn payload(&self) -> &Value {
        &self.payload
    }

    /// Returns the event entity, unwrapping a provider `{ "data": ... }`
    /// envelope when present.
    #[must_use]
    pub fn entity(&self) -> &Value {
        self.payload
            .get("data")
            .filter(|data| data.is_object() || data.is_array())
            .unwrap_or(&self.payload)
    }

    /// Deserializes the event entity into a provider model.
    ///
    /// # Errors
    ///
    /// Returns an error when the provider payload does not match `T`.
    pub fn decode<T>(&self) -> Result<T, RealtimeError>
    where
        T: DeserializeOwned,
    {
        serde_json::from_str(self.raw_entity.get()).map_err(RealtimeError::Decode)
    }

    /// Deserializes a single entity or every non-null entry in an entity array.
    ///
    /// Each array entry is decoded independently so one malformed provider
    /// record does not hide the other valid records in the same invocation.
    /// `ProjectX`'s null padding entries are omitted.
    #[must_use]
    pub fn decode_batch<T>(&self) -> Vec<Result<T, RealtimeError>>
    where
        T: DeserializeOwned,
    {
        if !self.entity().is_array() {
            return vec![self.decode()];
        }
        let values = match serde_json::from_str::<Vec<Box<RawValue>>>(self.raw_entity.get()) {
            Ok(values) => values,
            Err(error) => return vec![Err(RealtimeError::Decode(error))],
        };
        values
            .into_iter()
            .filter(|value| value.get() != "null")
            .map(|value| serde_json::from_str(value.get()).map_err(RealtimeError::Decode))
            .collect()
    }
}

/// Events emitted by a [`RealtimeClient`].
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum RealtimeEvent {
    /// The initial connection completed its `SignalR` handshake.
    Connected,
    /// The transport disconnected.
    Disconnected,
    /// A replacement connection completed its `SignalR` handshake.
    ///
    /// Callers must replay their canonical subscription set after receiving
    /// this event.
    Reconnected,
    /// At least one provider frame could not enter the bounded event queue.
    ///
    /// Callers must fence recovery and then call
    /// [`RealtimeEventReceiver::acknowledge_transport_gap`].
    TransportGap,
    /// A provider type-1 invocation with an exact raw entity retained for typed decoding.
    Invocation(SignalRInvocation),
    /// A decoded `SignalR` JSON message.
    ///
    /// Type-1 invocations are emitted through [`Self::Invocation`]; this
    /// variant carries other application-visible message families.
    Message(Value),
}

mod event_flow;
mod session_handle;
use event_flow::{EventEnvelope, EventFlow, PublishOutcome};
pub use event_flow::{RealtimeEventReceiver, RealtimeGeneration, RealtimeMessage};
pub use session_handle::RealtimeSession;

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
    /// The bounded event queue is full and a transport gap was latched.
    #[error("real-time event queue is full; transport gap latched")]
    EventQueueFull,
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
    /// An invocation exceeded the bounded outbound message size.
    #[error("real-time invocation exceeds the {max_bytes}-byte outbound limit")]
    OutboundMessageTooLarge {
        /// Maximum encoded invocation size, including the `SignalR` terminator.
        max_bytes: usize,
    },
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

/// `SignalR`-over-WebSocket client for one `ProjectX` hub.
///
/// The transport owns no subscription truth. Reconnects emit
/// [`RealtimeEvent::Reconnected`], after which the caller replays its current
/// subscription set.
pub struct RealtimeClient {
    inner: Arc<RealtimeInner>,
}

struct RealtimeInner {
    hub: Hub,
    endpoints: Endpoints,
    http: reqwest::Client,
    token: Arc<TokenStore>,
    lifecycle: ParkingMutex<Lifecycle>,
    lifecycle_changed: Notify,
    generation: AtomicU64,
    reconnect_enabled: AtomicBool,
    owner_cancel: CancellationToken,
    client_handles: AtomicUsize,
    watchdog_task: ParkingMutex<Option<JoinHandle<()>>>,
    retirement_task: ParkingMutex<Option<JoinHandle<()>>>,
    failed_close: AtomicU64,
    last_activity: ParkingMutex<Instant>,
    socket_probe: ParkingMutex<Option<(u64, u64)>>,
    request_counter: AtomicU64,
    pending: PendingInvocations,
    event_tx: mpsc::Sender<EventEnvelope>,
    event_rx: ParkingMutex<Option<RealtimeEventReceiver>>,
    event_flow: Arc<EventFlow>,
}

enum Lifecycle {
    Disconnected,
    Connecting {
        generation: u64,
        cancellation: CancellationToken,
    },
    Connected(Session),
    Closing {
        generation: u64,
        was_connected: bool,
    },
}

impl Lifecycle {
    fn generation(&self) -> Option<u64> {
        match self {
            Self::Disconnected => None,
            Self::Connecting { generation, .. } | Self::Closing { generation, .. } => {
                Some(*generation)
            }
            Self::Connected(session) => Some(session.generation),
        }
    }
}

struct Session {
    runtime: tokio::runtime::Handle,
    generation: u64,
    writer: mpsc::Sender<Message>,
    cancellation: CancellationToken,
    reader_task: JoinHandle<Result<(), RealtimeError>>,
    writer_task: JoinHandle<Result<(), RealtimeError>>,
}

struct ConnectClaim {
    inner: Weak<RealtimeInner>,
    generation: u64,
    cancellation: CancellationToken,
    complete: bool,
}

impl ConnectClaim {
    fn complete(&mut self) {
        self.complete = true;
    }
}

impl Drop for ConnectClaim {
    fn drop(&mut self) {
        if !self.complete
            && let Some(inner) = self.inner.upgrade()
        {
            inner.cancel_connect(self.generation);
        }
    }
}

struct PendingGuard {
    pending: PendingInvocations,
    invocation_id: String,
    generation: u64,
}

impl Drop for PendingGuard {
    fn drop(&mut self) {
        {
            let mut pending = self.pending.lock();
            if pending
                .get(&self.invocation_id)
                .is_some_and(|entry| entry.generation == self.generation)
            {
                pending.remove(&self.invocation_id);
            }
        }
    }
}

enum DisconnectAction {
    None,
    Wait(u64),
    Close(Session),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProcessOutcome {
    Continue,
    Close,
}

impl Drop for Session {
    fn drop(&mut self) {
        self.cancellation.cancel();
        self.reader_task.abort();
        self.writer_task.abort();
    }
}

impl RealtimeClient {
    pub(crate) fn new(
        hub: Hub,
        endpoints: Endpoints,
        http: reqwest::Client,
        token: Arc<TokenStore>,
    ) -> Self {
        let (event_tx, event_rx) = mpsc::channel(EVENT_CAPACITY);
        let event_flow = EventFlow::new();
        Self {
            inner: Arc::new(RealtimeInner {
                hub,
                endpoints,
                http,
                token,
                lifecycle: ParkingMutex::new(Lifecycle::Disconnected),
                lifecycle_changed: Notify::new(),
                generation: AtomicU64::new(0),
                reconnect_enabled: AtomicBool::new(false),
                owner_cancel: CancellationToken::new(),
                client_handles: AtomicUsize::new(1),
                watchdog_task: ParkingMutex::new(None),
                retirement_task: ParkingMutex::new(None),
                failed_close: AtomicU64::new(0),
                last_activity: ParkingMutex::new(Instant::now()),
                socket_probe: ParkingMutex::new(None),
                request_counter: AtomicU64::new(1),
                pending: Arc::default(),
                event_tx,
                event_rx: ParkingMutex::new(Some(RealtimeEventReceiver {
                    events: event_rx,
                    flow: Arc::clone(&event_flow),
                    gap_reported: false,
                })),
                event_flow,
            }),
        }
    }

    /// Claims this client's single event receiver.
    #[must_use]
    pub fn take_event_receiver(&self) -> Option<RealtimeEventReceiver> {
        self.inner.event_rx.lock().take()
    }

    /// Connects and validates the `SignalR` handshake.
    ///
    /// # Errors
    ///
    /// Returns an error when authentication, URL construction, WebSocket
    /// upgrade, or the `SignalR` handshake fails.
    pub async fn connect(&self) -> Result<(), RealtimeError> {
        self.inner.connect_once(false).await
    }

    /// Gracefully disconnects and stops background tasks.
    ///
    /// # Errors
    ///
    /// Returns an error if the close handshake does not complete within the
    /// bounded timeout.
    pub async fn disconnect(&self) -> Result<(), RealtimeError> {
        self.inner.disconnect().await
    }

    /// Returns whether the latest connection completed its `SignalR` handshake.
    #[must_use]
    pub fn is_connected(&self) -> bool {
        self.inner.is_connected()
    }

    /// Captures the current socket for generation-scoped subscription admission.
    ///
    /// # Errors
    /// Returns [`RealtimeError::NotConnected`] unless a socket is ready.
    pub fn session(&self) -> Result<RealtimeSession, RealtimeError> {
        let lifecycle = self.inner.lifecycle.lock();
        let Lifecycle::Connected(session) = &*lifecycle else {
            return Err(RealtimeError::NotConnected);
        };
        Ok(RealtimeSession {
            inner: Arc::downgrade(&self.inner),
            generation: RealtimeGeneration(session.generation),
        })
    }

    /// Invokes an arbitrary provider target and waits for its completion frame.
    ///
    /// # Errors
    ///
    /// Returns an error when disconnected, a bounded capacity is exhausted,
    /// the provider rejects the invocation, or completion times out. A timeout
    /// or cancellation after queue admission has an unknown outcome and reclaims
    /// only that invocation slot. It never closes the connection or retries the call.
    pub async fn invoke(
        &self,
        target: impl Into<String>,
        arguments: Vec<Value>,
    ) -> Result<(), RealtimeError> {
        self.inner
            .send_invocation(None, target.into(), arguments)
            .await
    }

    /// Subscribes to market trades for a contract.
    ///
    /// # Errors
    ///
    /// Returns a real-time invocation error.
    pub async fn subscribe_contract_trades(
        &self,
        contract: &ContractId,
    ) -> Result<(), RealtimeError> {
        if self.inner.hub != Hub::Market {
            return Err(RealtimeError::WrongHub);
        }
        self.session()?.subscribe_contract_trades(contract).await
    }

    /// Unsubscribes from market trades for a contract.
    ///
    /// # Errors
    ///
    /// Returns a real-time invocation error.
    pub async fn unsubscribe_contract_trades(
        &self,
        contract: &ContractId,
    ) -> Result<(), RealtimeError> {
        if self.inner.hub != Hub::Market {
            return Err(RealtimeError::WrongHub);
        }
        self.session()?.unsubscribe_contract_trades(contract).await
    }

    /// Subscribes to market quotes for a contract.
    ///
    /// # Errors
    ///
    /// Returns a real-time invocation error.
    pub async fn subscribe_contract_quotes(
        &self,
        contract: &ContractId,
    ) -> Result<(), RealtimeError> {
        if self.inner.hub != Hub::Market {
            return Err(RealtimeError::WrongHub);
        }
        self.session()?.subscribe_contract_quotes(contract).await
    }

    /// Unsubscribes from market quotes for a contract.
    ///
    /// # Errors
    ///
    /// Returns a real-time invocation error.
    pub async fn unsubscribe_contract_quotes(
        &self,
        contract: &ContractId,
    ) -> Result<(), RealtimeError> {
        if self.inner.hub != Hub::Market {
            return Err(RealtimeError::WrongHub);
        }
        self.session()?.unsubscribe_contract_quotes(contract).await
    }

    /// Subscribes to market depth for a contract.
    ///
    /// # Errors
    ///
    /// Returns a real-time invocation error.
    pub async fn subscribe_contract_depth(
        &self,
        contract: &ContractId,
    ) -> Result<(), RealtimeError> {
        if self.inner.hub != Hub::Market {
            return Err(RealtimeError::WrongHub);
        }
        self.session()?.subscribe_contract_depth(contract).await
    }

    /// Unsubscribes from market depth for a contract.
    ///
    /// # Errors
    ///
    /// Returns a real-time invocation error.
    pub async fn unsubscribe_contract_depth(
        &self,
        contract: &ContractId,
    ) -> Result<(), RealtimeError> {
        if self.inner.hub != Hub::Market {
            return Err(RealtimeError::WrongHub);
        }
        self.session()?.unsubscribe_contract_depth(contract).await
    }

    /// Subscribes to account updates.
    ///
    /// # Errors
    ///
    /// Returns a real-time invocation error.
    pub async fn subscribe_accounts(&self) -> Result<(), RealtimeError> {
        if self.inner.hub != Hub::User {
            return Err(RealtimeError::WrongHub);
        }
        self.session()?.subscribe_accounts().await
    }

    /// Unsubscribes from account updates.
    ///
    /// # Errors
    ///
    /// Returns a real-time invocation error.
    pub async fn unsubscribe_accounts(&self) -> Result<(), RealtimeError> {
        if self.inner.hub != Hub::User {
            return Err(RealtimeError::WrongHub);
        }
        self.session()?.unsubscribe_accounts().await
    }

    /// Subscribes to order updates for an account.
    ///
    /// # Errors
    ///
    /// Returns a real-time invocation error.
    pub async fn subscribe_orders(&self, account: AccountId) -> Result<(), RealtimeError> {
        if self.inner.hub != Hub::User {
            return Err(RealtimeError::WrongHub);
        }
        self.session()?.subscribe_orders(account).await
    }

    /// Unsubscribes from order updates for an account.
    ///
    /// # Errors
    ///
    /// Returns a real-time invocation error.
    pub async fn unsubscribe_orders(&self, account: AccountId) -> Result<(), RealtimeError> {
        if self.inner.hub != Hub::User {
            return Err(RealtimeError::WrongHub);
        }
        self.session()?.unsubscribe_orders(account).await
    }

    /// Subscribes to position updates for an account.
    ///
    /// # Errors
    ///
    /// Returns a real-time invocation error.
    pub async fn subscribe_positions(&self, account: AccountId) -> Result<(), RealtimeError> {
        if self.inner.hub != Hub::User {
            return Err(RealtimeError::WrongHub);
        }
        self.session()?.subscribe_positions(account).await
    }

    /// Unsubscribes from position updates for an account.
    ///
    /// # Errors
    ///
    /// Returns a real-time invocation error.
    pub async fn unsubscribe_positions(&self, account: AccountId) -> Result<(), RealtimeError> {
        if self.inner.hub != Hub::User {
            return Err(RealtimeError::WrongHub);
        }
        self.session()?.unsubscribe_positions(account).await
    }

    /// Subscribes to trade updates for an account.
    ///
    /// # Errors
    ///
    /// Returns a real-time invocation error.
    pub async fn subscribe_trades(&self, account: AccountId) -> Result<(), RealtimeError> {
        if self.inner.hub != Hub::User {
            return Err(RealtimeError::WrongHub);
        }
        self.session()?.subscribe_trades(account).await
    }

    /// Unsubscribes from trade updates for an account.
    ///
    /// # Errors
    ///
    /// Returns a real-time invocation error.
    pub async fn unsubscribe_trades(&self, account: AccountId) -> Result<(), RealtimeError> {
        if self.inner.hub != Hub::User {
            return Err(RealtimeError::WrongHub);
        }
        self.session()?.unsubscribe_trades(account).await
    }
}

impl Clone for RealtimeClient {
    fn clone(&self) -> Self {
        self.inner.client_handles.fetch_add(1, Ordering::Relaxed);
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl Drop for RealtimeClient {
    fn drop(&mut self) {
        if self.inner.client_handles.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.inner.shutdown_now();
        }
    }
}

impl fmt::Debug for RealtimeClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RealtimeClient")
            .field("hub", &self.inner.hub)
            .field("endpoints", &self.inner.endpoints)
            .field("connected", &self.is_connected())
            .finish_non_exhaustive()
    }
}

impl RealtimeInner {
    fn is_connected(&self) -> bool {
        matches!(*self.lifecycle.lock(), Lifecycle::Connected(_))
    }

    fn begin_connect(self: &Arc<Self>, reconnecting: bool) -> Result<ConnectClaim, RealtimeError> {
        self.begin_connect_with_pre_lock(reconnecting, || {})
    }

    fn begin_connect_with_pre_lock<F>(
        self: &Arc<Self>,
        reconnecting: bool,
        before_lifecycle_lock: F,
    ) -> Result<ConnectClaim, RealtimeError>
    where
        F: FnOnce(),
    {
        before_lifecycle_lock();
        let mut lifecycle = self.lifecycle.lock();
        if self.event_flow.has_unacknowledged_gap() {
            return Err(RealtimeError::TransportGapPending);
        }
        if !matches!(*lifecycle, Lifecycle::Disconnected) {
            return Err(RealtimeError::AlreadyConnected);
        }
        if !reconnecting {
            self.reconnect_enabled.store(true, Ordering::Release);
        } else if !self.reconnect_enabled.load(Ordering::Acquire) {
            return Err(RealtimeError::ConnectionCancelled);
        }
        let previous = self
            .generation
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                value.checked_add(1)
            })
            .map_err(|_| RealtimeError::IdentifierCapacity)?;
        let generation = previous
            .checked_add(1)
            .ok_or(RealtimeError::IdentifierCapacity)?;
        let cancellation = self.owner_cancel.child_token();
        *lifecycle = Lifecycle::Connecting {
            generation,
            cancellation: cancellation.clone(),
        };
        drop(lifecycle);
        self.lifecycle_changed.notify_waiters();
        Ok(ConnectClaim {
            inner: Arc::downgrade(self),
            generation,
            cancellation,
            complete: false,
        })
    }

    async fn connect_once(self: &Arc<Self>, reconnecting: bool) -> Result<(), RealtimeError> {
        let mut claim = self.begin_connect(reconnecting)?;
        let url = self.connection_url()?;
        let mut stream = tokio::select! {
            () = claim.cancellation.cancelled() => {
                return Err(RealtimeError::ConnectionCancelled);
            }
            result = tokio::time::timeout(
                CONNECT_TIMEOUT,
                upgrade_websocket(&self.http, url),
            ) => {
                match result {
                    Ok(result) => result?,
                    Err(_) => return Err(RealtimeError::ConnectionTimedOut),
                }
            }
        };
        let handshake_tail = tokio::select! {
            () = claim.cancellation.cancelled() => {
                return Err(RealtimeError::ConnectionCancelled);
            }
            result = tokio::time::timeout(HANDSHAKE_TIMEOUT, negotiate_handshake(&mut stream)) => {
                match result {
                    Ok(result) => result?,
                    Err(_) => return Err(RealtimeError::Handshake("response timed out")),
                }
            }
        };

        let (write, read) = stream.split();
        let (writer, writer_rx) = mpsc::channel(WRITER_CAPACITY);
        let start = CancellationToken::new();
        let session_cancellation = claim.cancellation.child_token();
        let writer_task = tokio::spawn(run_writer(
            Arc::downgrade(self),
            claim.generation,
            write,
            writer_rx,
            session_cancellation.clone(),
            start.clone(),
        ));
        let reader_task = tokio::spawn(run_reader(
            Arc::downgrade(self),
            claim.generation,
            read,
            handshake_tail,
            session_cancellation.clone(),
            start.clone(),
        ));
        let session = Session {
            runtime: tokio::runtime::Handle::current(),
            generation: claim.generation,
            writer,
            cancellation: session_cancellation,
            reader_task,
            writer_task,
        };
        let event = if reconnecting {
            RealtimeEvent::Reconnected
        } else {
            RealtimeEvent::Connected
        };
        self.install_ready(claim.generation, session, event)?;
        self.record_activity(claim.generation);
        start.cancel();
        self.start_watchdog();
        claim.complete();
        Ok(())
    }

    fn install_ready(
        self: &Arc<Self>,
        generation: u64,
        session: Session,
        event: RealtimeEvent,
    ) -> Result<(), RealtimeError> {
        let mut lifecycle = self.lifecycle.lock();
        let can_install = matches!(
            &*lifecycle,
            Lifecycle::Connecting {
                generation: active,
                cancellation,
            } if *active == generation && !cancellation.is_cancelled()
        );
        if !can_install {
            return Err(RealtimeError::ConnectionCancelled);
        }
        self.event_flow.start_generation(generation)?;
        *lifecycle = Lifecycle::Connected(session);
        let published = self.publish(generation, event, 0);
        drop(lifecycle);
        self.lifecycle_changed.notify_waiters();
        match published {
            Ok(PublishOutcome::Published) => Ok(()),
            Ok(PublishOutcome::StaleGeneration) => {
                self.end_generation(generation);
                Err(RealtimeError::ConnectionCancelled)
            }
            Err(error) => {
                self.end_generation(generation);
                Err(error)
            }
        }
    }

    fn cancel_connect(self: &Arc<Self>, generation: u64) {
        self.end_generation(generation);
    }

    fn begin_disconnect(&self) -> DisconnectAction {
        let mut lifecycle = self.lifecycle.lock();
        self.reconnect_enabled.store(false, Ordering::Release);
        self.stop_watchdog();
        let previous = std::mem::replace(&mut *lifecycle, Lifecycle::Disconnected);
        let action = match previous {
            Lifecycle::Disconnected => DisconnectAction::None,
            Lifecycle::Connecting {
                generation,
                cancellation,
            } => {
                cancellation.cancel();
                *lifecycle = Lifecycle::Closing {
                    generation,
                    was_connected: false,
                };
                DisconnectAction::Wait(generation)
            }
            Lifecycle::Connected(session) => {
                let generation = session.generation;
                *lifecycle = Lifecycle::Closing {
                    generation,
                    was_connected: true,
                };
                DisconnectAction::Close(session)
            }
            closing @ Lifecycle::Closing { generation, .. } => {
                *lifecycle = closing;
                DisconnectAction::Wait(generation)
            }
        };
        drop(lifecycle);
        self.lifecycle_changed.notify_waiters();
        action
    }

    async fn disconnect(self: &Arc<Self>) -> Result<(), RealtimeError> {
        match self.begin_disconnect() {
            DisconnectAction::None => Ok(()),
            DisconnectAction::Wait(generation) => {
                let waited = tokio::time::timeout(
                    CLOSE_TIMEOUT,
                    self.wait_until_generation_ends(generation),
                )
                .await;
                if waited.is_err() {
                    Err(RealtimeError::Close)
                } else {
                    Ok(())
                }
            }
            DisconnectAction::Close(session) => {
                let generation = session.generation;
                self.retire_session(session, true);
                self.wait_until_generation_ends(generation).await;
                if self.failed_close.load(Ordering::Acquire) == generation {
                    Err(RealtimeError::Close)
                } else {
                    Ok(())
                }
            }
        }
    }

    async fn wait_until_generation_ends(&self, generation: u64) {
        loop {
            let changed = self.lifecycle_changed.notified();
            tokio::pin!(changed);
            let _ = changed.as_mut().enable();
            let is_active = match &*self.lifecycle.lock() {
                Lifecycle::Connecting {
                    generation: active, ..
                }
                | Lifecycle::Closing {
                    generation: active, ..
                } => *active == generation,
                Lifecycle::Connected(session) => session.generation == generation,
                Lifecycle::Disconnected => false,
            };
            if !is_active {
                return;
            }
            changed.await;
        }
    }

    fn finish_closing(&self, generation: u64) {
        let mut lifecycle = self.lifecycle.lock();
        let should_publish = match &*lifecycle {
            Lifecycle::Closing {
                generation: active,
                was_connected,
            } if *active == generation => *was_connected,
            _ => return,
        };
        self.finish_event_generation_locked(generation, should_publish);
        *lifecycle = Lifecycle::Disconnected;
        drop(lifecycle);
        self.fail_pending_generation(generation);
        self.lifecycle_changed.notify_waiters();
    }

    fn end_generation(self: &Arc<Self>, generation: u64) {
        let mut lifecycle = self.lifecycle.lock();
        let previous = std::mem::replace(&mut *lifecycle, Lifecycle::Disconnected);
        match previous {
            Lifecycle::Connected(session) if session.generation == generation => {
                *lifecycle = Lifecycle::Closing {
                    generation,
                    was_connected: true,
                };
                drop(lifecycle);
                self.retire_session(session, false);
            }
            Lifecycle::Connecting {
                generation: active,
                cancellation,
            } if active == generation => {
                cancellation.cancel();
                drop(lifecycle);
                self.lifecycle_changed.notify_waiters();
            }
            Lifecycle::Closing {
                generation: active,
                was_connected: false,
            } if active == generation => {
                drop(lifecycle);
                self.lifecycle_changed.notify_waiters();
            }
            other => {
                *lifecycle = other;
            }
        }
    }

    // The retained supervisor owns both task handles through their joins. A
    // reader may request this transition without ever attempting to join itself.
    fn retire_session(self: &Arc<Self>, mut session: Session, graceful: bool) {
        let generation = session.generation;
        if !graceful {
            session.cancellation.cancel();
        }
        self.fail_pending_generation(generation);
        let inner = Arc::clone(self);
        let mut slot = self.retirement_task.lock();
        // Drop can run on an ordinary thread. Retire on the socket's original
        // runtime, preserving joined evidence without requiring ambient context.
        let runtime = session.runtime.clone();
        *slot = Some(runtime.spawn(async move {
            // A stuck writer cannot delay a real transport-loss boundary forever.
            // Abort only after cancellation, then join before publishing evidence.
            if graceful && send_queued(&session.writer, Message::Close(None)).is_err() {
                session.cancellation.cancel();
                inner.failed_close.store(generation, Ordering::Release);
            }
            if !join_socket_tasks(&mut session).await {
                inner.failed_close.store(generation, Ordering::Release);
            }
            inner.finish_closing(generation);
        }));
    }

    fn shutdown_now(self: &Arc<Self>) {
        self.owner_cancel.cancel();
        self.reconnect_enabled.store(false, Ordering::Release);
        self.stop_watchdog();
        let generation = self.lifecycle.lock().generation();
        if let Some(generation) = generation {
            self.end_generation(generation);
        }
        self.pending.lock().clear();
    }

    // The caller holds `lifecycle`, preventing a replacement from becoming
    // visible until the terminal event is ordered and old event admission is closed.
    fn finish_event_generation_locked(&self, generation: u64, publish_disconnected: bool) {
        if publish_disconnected {
            let _ = self.publish(generation, RealtimeEvent::Disconnected, 0);
        }
        self.event_flow.finish_generation(generation);
    }

    fn start_watchdog(self: &Arc<Self>) {
        let mut watchdog = self.watchdog_task.lock();
        if !self.reconnect_enabled.load(Ordering::Acquire) || self.owner_cancel.is_cancelled() {
            return;
        }
        if watchdog.as_ref().is_some_and(|task| !task.is_finished()) {
            return;
        }
        if let Some(task) = watchdog.take() {
            task.abort();
        }
        let weak = Arc::downgrade(self);
        let cancellation = self.owner_cancel.clone();
        *watchdog = Some(tokio::spawn(async move {
            let mut ticker = tokio::time::interval(WATCHDOG_INTERVAL);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    biased;
                    () = cancellation.cancelled() => break,
                    _ = ticker.tick() => {
                        let Some(inner) = weak.upgrade() else {
                            break;
                        };
                        inner.watchdog_tick().await;
                    }
                }
            }
        }));
    }

    fn stop_watchdog(&self) {
        if let Some(task) = self.watchdog_task.lock().take() {
            task.abort();
        }
    }

    async fn watchdog_tick(self: &Arc<Self>) {
        if !self.reconnect_enabled.load(Ordering::Acquire)
            || self.event_flow.has_unacknowledged_gap()
            || self.owner_cancel.is_cancelled()
        {
            return;
        }
        let disconnected = matches!(*self.lifecycle.lock(), Lifecycle::Disconnected);
        if disconnected
            && let Err(error) = self.connect_once(true).await
            && !matches!(
                error,
                RealtimeError::ConnectionCancelled | RealtimeError::AlreadyConnected
            )
        {
            tracing::warn!(%error, hub = ?self.hub, "ProjectX real-time reconnect failed");
        }
    }

    fn connection_url(&self) -> Result<url::Url, RealtimeError> {
        let token = self
            .token
            .snapshot()
            .filter(|token| !token.trim().is_empty())
            .ok_or(RealtimeError::MissingAuthToken)?;
        let mut url = self
            .endpoints
            .hub_url(self.hub.path())
            .map_err(RealtimeError::Endpoint)?;
        url.query_pairs_mut().append_pair("access_token", &token);
        Ok(url)
    }

    async fn send_invocation(
        self: &Arc<Self>,
        expected: Option<RealtimeGeneration>,
        target: String,
        arguments: Vec<Value>,
    ) -> Result<(), RealtimeError> {
        let invocation_id = self
            .request_counter
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                value.checked_add(1)
            })
            .map_err(|_| RealtimeError::IdentifierCapacity)?
            .to_string();
        let message = encode_invocation(&invocation_id, &target, &arguments)?;
        let (reply_tx, reply_rx) = oneshot::channel();
        let (generation, writer) = self.register_invocation(expected, &invocation_id, reply_tx)?;
        let _pending = PendingGuard {
            pending: Arc::clone(&self.pending),
            invocation_id: invocation_id.clone(),
            generation,
        };
        send_queued(&writer, message)?;

        match tokio::time::timeout(COMPLETION_TIMEOUT, reply_rx).await {
            Ok(Ok(Ok(()))) => Ok(()),
            Ok(Ok(Err(()))) => Err(RealtimeError::InvocationRejected { target }),
            Ok(Err(_)) => Err(RealtimeError::InvocationSessionEnded { target }),
            Err(_) => Err(RealtimeError::InvocationTimedOut { target }),
        }
    }

    fn register_invocation(
        &self,
        expected: Option<RealtimeGeneration>,
        invocation_id: &str,
        reply: PendingInvocation,
    ) -> Result<(u64, mpsc::Sender<Message>), RealtimeError> {
        let lifecycle = self.lifecycle.lock();
        let Lifecycle::Connected(session) = &*lifecycle else {
            return Err(RealtimeError::NotConnected);
        };
        if expected.is_some_and(|expected| expected.0 != session.generation) {
            return Err(RealtimeError::StaleGeneration);
        }
        let mut pending = self.pending.lock();
        if pending.len() >= PENDING_INVOCATION_CAPACITY {
            return Err(RealtimeError::PendingInvocationCapacity);
        }
        pending.insert(
            invocation_id.to_owned(),
            PendingEntry {
                generation: session.generation,
                reply,
            },
        );
        Ok((session.generation, session.writer.clone()))
    }

    fn process_text(&self, generation: u64, text: &str) -> Result<ProcessOutcome, RealtimeError> {
        let mut records = text.split(SIGNALR_TERMINATOR).peekable();
        while let Some(frame) = records.next() {
            if records.peek().is_none() {
                if !frame.is_empty() {
                    self.event_flow.mark_gap(generation);
                }
                break;
            }
            match self.process_record(generation, frame) {
                Ok(ProcessOutcome::Continue) => {}
                Ok(ProcessOutcome::Close) => return Ok(ProcessOutcome::Close),
                Err(
                    RealtimeError::Decode(_)
                    | RealtimeError::Protocol(_)
                    | RealtimeError::EventQueueFull,
                ) => {
                    self.event_flow.mark_gap(generation);
                }
                Err(error) => return Err(error),
            }
        }
        Ok(ProcessOutcome::Continue)
    }

    fn process_record(
        &self,
        generation: u64,
        frame: &str,
    ) -> Result<ProcessOutcome, RealtimeError> {
        let value: Value = serde_json::from_str(frame).map_err(RealtimeError::Decode)?;
        let message_type = value
            .get("type")
            .ok_or(RealtimeError::Protocol("message type is missing"))?
            .as_u64()
            .ok_or(RealtimeError::Protocol(
                "message type was not an unsigned integer",
            ))?;
        match message_type {
            1 => {
                let invocation = SignalRInvocation::from_json(frame)?.ok_or(
                    RealtimeError::Protocol("type-1 frame was not an invocation"),
                )?;
                let weight = frame
                    .len()
                    .saturating_mul(EVENT_DECODED_WEIGHT_MULTIPLIER)
                    .saturating_add(EVENT_BASE_WEIGHT);
                if self.publish(generation, RealtimeEvent::Invocation(invocation), weight)?
                    == PublishOutcome::StaleGeneration
                {
                    return Ok(ProcessOutcome::Close);
                }
            }
            3 => {
                let (invocation_id, result) = completion(&value)?;
                let reply = {
                    let mut pending = self.pending.lock();
                    if pending
                        .get(invocation_id)
                        .is_some_and(|entry| entry.generation == generation)
                    {
                        pending.remove(invocation_id).map(|entry| entry.reply)
                    } else {
                        None
                    }
                };
                if let Some(reply) = reply {
                    let _ = reply.send(result);
                }
            }
            6 => {}
            7 => {
                if value.get("error").is_some_and(|error| !error.is_string()) {
                    return Err(RealtimeError::Protocol("close error was not a string"));
                }
                let _provider_hint = match value.get("allowReconnect") {
                    Some(flag) => flag.as_bool().ok_or(RealtimeError::Protocol(
                        "close allowReconnect flag was not a boolean",
                    ))?,
                    None => false,
                };
                return Ok(ProcessOutcome::Close);
            }
            _ => {
                let weight = frame
                    .len()
                    .saturating_mul(EVENT_DECODED_WEIGHT_MULTIPLIER)
                    .saturating_add(EVENT_BASE_WEIGHT);
                if self.publish(generation, RealtimeEvent::Message(value), weight)?
                    == PublishOutcome::StaleGeneration
                {
                    return Ok(ProcessOutcome::Close);
                }
            }
        }
        Ok(ProcessOutcome::Continue)
    }

    fn publish(
        &self,
        generation: u64,
        event: RealtimeEvent,
        weight: usize,
    ) -> Result<PublishOutcome, RealtimeError> {
        let weight = weight.max(EVENT_BASE_WEIGHT);
        match self
            .event_flow
            .publish(&self.event_tx, generation, event, weight)
        {
            Err(RealtimeError::EventReceiverClosed) => {
                self.reconnect_enabled.store(false, Ordering::Release);
                self.stop_watchdog();
                Err(RealtimeError::EventReceiverClosed)
            }
            result => result,
        }
    }

    fn fail_pending_generation(&self, generation: u64) {
        self.pending
            .lock()
            .retain(|_, entry| entry.generation != generation);
    }

    fn acknowledge_probe(&self, payload: &[u8]) {
        let Ok(payload) = <[u8; 16]>::try_from(payload) else {
            return;
        };
        let (left, right) = payload.split_at(8);
        let (Ok(left), Ok(right)) = (left.try_into(), right.try_into()) else {
            return;
        };
        let answer = (u64::from_be_bytes(left), u64::from_be_bytes(right));
        let mut probe = self.socket_probe.lock();
        if *probe == Some(answer) {
            *probe = None;
        }
    }

    fn record_activity(&self, generation: u64) {
        let lifecycle = self.lifecycle.lock();
        if matches!(
            &*lifecycle,
            Lifecycle::Connected(session) if session.generation == generation
        ) {
            *self.last_activity.lock() = Instant::now();
        }
    }
}

impl Drop for RealtimeInner {
    fn drop(&mut self) {
        self.owner_cancel.cancel();
        if let Some(task) = self.watchdog_task.get_mut().take() {
            task.abort();
        }
        match std::mem::replace(self.lifecycle.get_mut(), Lifecycle::Disconnected) {
            Lifecycle::Connecting { cancellation, .. } => cancellation.cancel(),
            Lifecycle::Connected(session) => drop(session),
            Lifecycle::Disconnected | Lifecycle::Closing { .. } => {}
        }
    }
}

async fn upgrade_websocket(
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

fn websocket_key() -> Result<String, RealtimeError> {
    let mut nonce = [0_u8; 16];
    SysRng
        .try_fill_bytes(&mut nonce)
        .map_err(|_| RealtimeError::Transport)?;
    Ok(BASE64.encode(&nonce))
}

fn websocket_accept(key: &str) -> String {
    let mut digest = Sha1::new();
    digest.update(key.as_bytes());
    digest.update(WEBSOCKET_GUID);
    BASE64.encode(&digest.finalize())
}

fn validate_websocket_upgrade(
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

fn websocket_config() -> WebSocketConfig {
    WebSocketConfig::default()
        .read_buffer_size(WEBSOCKET_READ_BUFFER_SIZE)
        .write_buffer_size(WEBSOCKET_WRITE_BUFFER_SIZE)
        .max_write_buffer_size(WEBSOCKET_MAX_WRITE_BUFFER_SIZE)
        .max_message_size(Some(WEBSOCKET_MAX_MESSAGE_SIZE))
        .max_frame_size(Some(WEBSOCKET_MAX_FRAME_SIZE))
}

fn encode_invocation(
    invocation_id: &str,
    target: &str,
    arguments: &[Value],
) -> Result<Message, RealtimeError> {
    let payload = OutboundInvocation {
        message_type: 1,
        invocation_id,
        target,
        arguments,
    };
    let mut encoded = BoundedBuffer::new(MAX_OUTBOUND_INVOCATION_SIZE);
    if let Err(error) = serde_json::to_writer(&mut encoded, &payload) {
        return if encoded.exceeded {
            Err(RealtimeError::OutboundMessageTooLarge {
                max_bytes: MAX_OUTBOUND_INVOCATION_SIZE,
            })
        } else {
            Err(RealtimeError::Encode(error))
        };
    }
    if encoded.write_all(&[0x1e]).is_err() {
        return Err(RealtimeError::OutboundMessageTooLarge {
            max_bytes: MAX_OUTBOUND_INVOCATION_SIZE,
        });
    }
    let text = String::from_utf8(encoded.into_bytes())
        .map_err(|_| RealtimeError::Protocol("encoded invocation was not UTF-8"))?;
    Ok(Message::Text(text.into()))
}

fn send_queued(writer: &mpsc::Sender<Message>, message: Message) -> Result<(), RealtimeError> {
    writer.try_send(message).map_err(|error| match error {
        mpsc::error::TrySendError::Full(_) => RealtimeError::SendQueueFull,
        mpsc::error::TrySendError::Closed(_) => RealtimeError::SendClosed,
    })
}

async fn join_socket_tasks(session: &mut Session) -> bool {
    let deadline = tokio::time::sleep(CLOSE_TIMEOUT);
    tokio::pin!(deadline);
    let mut reader_done = false;
    let mut writer_done = false;
    loop {
        if reader_done && writer_done {
            return true;
        }
        tokio::select! {
            _ = &mut session.reader_task, if !reader_done => reader_done = true,
            _ = &mut session.writer_task, if !writer_done => writer_done = true,
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

async fn negotiate_handshake<S>(stream: &mut S) -> Result<Option<String>, RealtimeError>
where
    S: futures_util::Stream<Item = Result<Message, TungsteniteError>>
        + futures_util::Sink<Message, Error = TungsteniteError>
        + Unpin,
{
    let payload = format!(
        "{}{}",
        serde_json::json!({"protocol":"json","version":1}),
        SIGNALR_TERMINATOR
    );
    stream
        .send(Message::Text(payload.into()))
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

async fn run_writer<S>(
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

async fn run_reader<S>(
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
            if inner.process_text(generation, &tail)? == ProcessOutcome::Close {
                return Ok(());
            }
        }
        loop {
            let message = tokio::select! {
                biased;
                () = cancellation.cancelled() => return Ok(()),
                message = read.next() => message,
            };
            match message {
                Some(Ok(Message::Text(text))) => {
                    let Some(inner) = inner.upgrade() else {
                        return Ok(());
                    };
                    let outcome = inner.process_text(generation, text.as_ref())?;
                    inner.record_activity(generation);
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
                    let outcome = inner.process_text(generation, text)?;
                    inner.record_activity(generation);
                    if outcome == ProcessOutcome::Close {
                        return Ok(());
                    }
                }
                Some(Ok(Message::Ping(_))) => {
                    let Some(inner) = inner.upgrade() else {
                        return Ok(());
                    };
                    inner.record_activity(generation);
                    // Tungstenite automatically queues the matching Pong.
                }
                Some(Ok(Message::Close(_))) | None => return Ok(()),
                Some(Err(_)) => return Err(RealtimeError::Transport),
                Some(Ok(Message::Pong(payload))) => {
                    let Some(inner) = inner.upgrade() else {
                        return Ok(());
                    };
                    inner.acknowledge_probe(&payload);
                    inner.record_activity(generation);
                }
                Some(Ok(Message::Frame(_))) => {}
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

fn validate_handshake(text: &str) -> Result<Option<String>, RealtimeError> {
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

fn completion(value: &Value) -> Result<(&str, Result<(), ()>), RealtimeError> {
    let object = value
        .as_object()
        .ok_or(RealtimeError::Protocol("completion was not an object"))?;
    let invocation_id =
        object
            .get("invocationId")
            .and_then(Value::as_str)
            .ok_or(RealtimeError::Protocol(
                "completion invocationId was not a string",
            ))?;
    if object.contains_key("error") && object.contains_key("result") {
        return Err(RealtimeError::Protocol(
            "completion contained both error and result",
        ));
    }
    let result = match object.get("error") {
        Some(Value::String(_)) => Err(()),
        Some(_) => {
            return Err(RealtimeError::Protocol("completion error was not a string"));
        }
        None => Ok(()),
    };
    Ok((invocation_id, result))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn fixture_realtime() -> RealtimeClient {
        let credentials = crate::Credentials::new("user", "key")
            .unwrap_or_else(|error| panic!("fixture credentials must be valid: {error}"));
        let client = crate::Client::builder(credentials)
            .build()
            .unwrap_or_else(|error| panic!("fixture client must build: {error}"));
        client.realtime(Hub::Market)
    }

    fn install_connected_generation(inner: &Arc<RealtimeInner>, generation: u64) {
        let (writer, _writer_rx) = mpsc::channel(1);
        let session = Session {
            runtime: tokio::runtime::Handle::current(),
            generation,
            writer,
            cancellation: CancellationToken::new(),
            reader_task: tokio::spawn(std::future::pending::<Result<(), RealtimeError>>()),
            writer_task: tokio::spawn(std::future::pending::<Result<(), RealtimeError>>()),
        };
        let mut lifecycle = inner.lifecycle.lock();
        inner
            .event_flow
            .start_generation(generation)
            .unwrap_or_else(|error| panic!("fixture generation must start: {error}"));
        inner.generation.store(generation, Ordering::Release);
        *lifecycle = Lifecycle::Connected(session);
    }

    fn install_paused_connected_generation(
        inner: &Arc<RealtimeInner>,
        generation: u64,
    ) -> mpsc::Receiver<Message> {
        let (writer, writer_rx) = mpsc::channel(1);
        let session = Session {
            runtime: tokio::runtime::Handle::current(),
            generation,
            writer,
            cancellation: CancellationToken::new(),
            reader_task: tokio::spawn(std::future::pending::<Result<(), RealtimeError>>()),
            writer_task: tokio::spawn(std::future::pending::<Result<(), RealtimeError>>()),
        };
        let mut lifecycle = inner.lifecycle.lock();
        inner
            .event_flow
            .start_generation(generation)
            .unwrap_or_else(|error| panic!("fixture generation must start: {error}"));
        inner.generation.store(generation, Ordering::Release);
        *lifecycle = Lifecycle::Connected(session);
        writer_rx
    }

    #[test]
    fn invocation_extracts_market_contract_and_payload() {
        let value = json!({
            "type": 1,
            "target": "GatewayTrade",
            "arguments": ["CON.F.US.MNQ.M26", {"price": 1.25}],
        });
        let invocation = SignalRInvocation::from_value(value)
            .and_then(|value| value.ok_or(RealtimeError::Protocol("missing invocation")))
            .unwrap_or_else(|error| panic!("fixture invocation must decode: {error}"));
        assert_eq!(invocation.target(), "GatewayTrade");
        assert_eq!(
            invocation.contract_id().map(ContractId::as_str),
            Some("CON.F.US.MNQ.M26")
        );
        assert_eq!(invocation.payload(), &json!({"price": 1.25}));
    }

    #[test]
    fn invocation_rejects_malformed_market_arguments() {
        for value in [
            json!({
                "type": 1,
                "target": "GatewayTrade",
                "arguments": [42, {"price": 1.25}],
            }),
            json!({
                "type": 1,
                "target": "GatewayTrade",
                "arguments": ["CON.F.US.MNQ.M26", {"price": 1.25}, "extra"],
            }),
        ] {
            assert!(matches!(
                SignalRInvocation::from_value(value),
                Err(RealtimeError::Protocol(_))
            ));
        }
    }

    #[test]
    fn handshake_accepts_coalesced_tail() {
        let tail = validate_handshake("{}\u{001e}{\"type\":6}\u{001e}")
            .unwrap_or_else(|error| panic!("fixture handshake must decode: {error}"));
        assert_eq!(tail.as_deref(), Some("{\"type\":6}\u{001e}"));
    }

    #[test]
    fn handshake_rejects_provider_error_without_retaining_detail() {
        let result = validate_handshake("{\"error\":\"secret provider detail\"}\u{001e}");
        assert!(matches!(result, Err(RealtimeError::Handshake(_))));
        assert!(!format!("{:?}", result.err()).contains("secret provider detail"));
    }

    #[test]
    fn completion_requires_an_unambiguous_protocol_shape() {
        for value in [
            json!({"type": 3}),
            json!({"type": 3, "invocationId": 1}),
            json!({"type": 3, "invocationId": "1", "error": false}),
            json!({"type": 3, "invocationId": "1", "error": "rejected", "result": null}),
        ] {
            assert!(matches!(
                completion(&value),
                Err(RealtimeError::Protocol(_))
            ));
        }

        assert_eq!(
            completion(&json!({"type": 3, "invocationId": "1"}))
                .unwrap_or_else(|error| panic!("void completion must decode: {error}")),
            ("1", Ok(()))
        );
        assert_eq!(
            completion(&json!({"type": 3, "invocationId": "1", "result": 42}))
                .unwrap_or_else(|error| panic!("result completion must decode: {error}")),
            ("1", Ok(()))
        );
        assert_eq!(
            completion(&json!({"type": 3, "invocationId": "1", "error": "rejected"}))
                .unwrap_or_else(|error| panic!("error completion must decode: {error}")),
            ("1", Err(()))
        );
    }

    #[test]
    fn malformed_transport_frames_are_not_published_as_messages() {
        let credentials = crate::Credentials::new("user", "key")
            .unwrap_or_else(|error| panic!("fixture credentials must be valid: {error}"));
        let client = crate::Client::builder(credentials)
            .build()
            .unwrap_or_else(|error| panic!("fixture client must build: {error}"));
        let realtime = client.realtime(Hub::Market);
        let mut events = realtime
            .take_event_receiver()
            .unwrap_or_else(|| panic!("event receiver must be available"));

        for frame in [
            "{}\u{001e}",
            "{\"type\":\"1\"}\u{001e}",
            "{\"type\":3,\"invocationId\":1}\u{001e}",
            "{\"type\":3,\"invocationId\":\"1\",\"error\":null}\u{001e}",
            "{\"type\":3,\"invocationId\":\"1\",\"error\":\"x\",\"result\":null}\u{001e}",
            "{\"type\":7,\"error\":null}\u{001e}",
        ] {
            assert!(matches!(
                realtime
                    .inner
                    .process_record(1, frame.trim_end_matches(SIGNALR_TERMINATOR)),
                Err(RealtimeError::Protocol(_))
            ));
        }
        assert!(matches!(
            events.events.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
    }

    #[tokio::test]
    async fn malformed_record_framing_is_rejected_before_publication() {
        let realtime = fixture_realtime();
        let mut events = realtime
            .take_event_receiver()
            .unwrap_or_else(|| panic!("event receiver must be available"));
        install_connected_generation(&realtime.inner, 1);

        for batch in [
            "{\"type\":1}",
            "{\"type\":1}\u{001e}\u{001e}{\"type\":1}\u{001e}",
            "\u{001e}{\"type\":1}\u{001e}",
            "\u{001e}",
        ] {
            assert!(matches!(
                realtime.inner.process_text(1, batch),
                Ok(ProcessOutcome::Continue)
            ));
        }
        assert!(matches!(
            events.events.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
    }

    #[test]
    fn connect_claim_rechecks_the_gap_after_waiting_for_lifecycle() {
        let realtime = fixture_realtime();
        let before_lock = Arc::new(std::sync::Barrier::new(2));
        let release_lock = Arc::new(std::sync::Barrier::new(2));
        let worker_inner = Arc::clone(&realtime.inner);
        let worker_before = Arc::clone(&before_lock);
        let worker_release = Arc::clone(&release_lock);
        let claim = std::thread::spawn(move || {
            worker_inner.begin_connect_with_pre_lock(false, || {
                worker_before.wait();
                worker_release.wait();
            })
        });

        before_lock.wait();
        realtime
            .inner
            .event_flow
            .start_generation(1)
            .unwrap_or_else(|error| panic!("fixture generation must start: {error}"));
        assert!(matches!(
            realtime.inner.event_flow.publish(
                &realtime.inner.event_tx,
                1,
                RealtimeEvent::Message(json!({"overflow": true})),
                EVENT_BYTE_BUDGET + 1,
            ),
            Err(RealtimeError::EventQueueFull)
        ));
        realtime.inner.event_flow.finish_generation(1);
        release_lock.wait();

        let result = claim
            .join()
            .unwrap_or_else(|payload| std::panic::resume_unwind(payload));
        assert!(matches!(result, Err(RealtimeError::TransportGapPending)));
        assert!(matches!(
            *realtime.inner.lifecycle.lock(),
            Lifecycle::Disconnected
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn disconnect_waiter_timeout_keeps_the_owner_session_fenced() {
        let realtime = fixture_realtime();
        let _writer_rx = install_paused_connected_generation(&realtime.inner, 1);
        let DisconnectAction::Close(owner_session) = realtime.inner.begin_disconnect() else {
            panic!("fixture owner must claim the connected session");
        };
        let waiter_client = realtime.clone();
        let waiter = tokio::spawn(async move { waiter_client.disconnect().await });
        tokio::task::yield_now().await;

        tokio::time::advance(CLOSE_TIMEOUT).await;
        let result = waiter
            .await
            .unwrap_or_else(|error| panic!("waiter must join: {error}"));
        assert!(matches!(result, Err(RealtimeError::Close)));
        assert!(matches!(
            *realtime.inner.lifecycle.lock(),
            Lifecycle::Closing {
                generation: 1,
                was_connected: true,
            }
        ));
        assert!(matches!(
            realtime.connect().await,
            Err(RealtimeError::AlreadyConnected)
        ));

        drop(owner_session);
        realtime.inner.finish_closing(1);
        assert!(matches!(
            *realtime.inner.lifecycle.lock(),
            Lifecycle::Disconnected
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn disconnect_waiter_timeout_keeps_the_connect_claim_fenced() {
        let realtime = fixture_realtime();
        let cancellation = realtime.inner.owner_cancel.child_token();
        *realtime.inner.lifecycle.lock() = Lifecycle::Connecting {
            generation: 1,
            cancellation: cancellation.clone(),
        };
        realtime.inner.generation.store(1, Ordering::Release);
        let owner_claim = ConnectClaim {
            inner: Arc::downgrade(&realtime.inner),
            generation: 1,
            cancellation,
            complete: false,
        };
        assert!(matches!(
            realtime.inner.begin_disconnect(),
            DisconnectAction::Wait(1)
        ));
        let waiter_client = realtime.clone();
        let waiter = tokio::spawn(async move { waiter_client.disconnect().await });
        tokio::task::yield_now().await;

        tokio::time::advance(CLOSE_TIMEOUT).await;
        let result = waiter
            .await
            .unwrap_or_else(|error| panic!("waiter must join: {error}"));
        assert!(matches!(result, Err(RealtimeError::Close)));
        assert!(matches!(
            *realtime.inner.lifecycle.lock(),
            Lifecycle::Closing {
                generation: 1,
                was_connected: false,
            }
        ));
        assert!(matches!(
            realtime.connect().await,
            Err(RealtimeError::AlreadyConnected)
        ));

        drop(owner_claim);
        assert!(matches!(
            *realtime.inner.lifecycle.lock(),
            Lifecycle::Disconnected
        ));
    }

    #[tokio::test]
    async fn ended_boundary_waits_for_both_socket_producers_to_drop() {
        struct Probe(Arc<AtomicUsize>);
        impl Drop for Probe {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let realtime = fixture_realtime();
        let stopped = Arc::new(AtomicUsize::new(0));
        let (release, wait) = oneshot::channel::<()>();
        let (writer, _writes) = mpsc::channel(1);
        let cancel = CancellationToken::new();
        let reader_probe = Probe(Arc::clone(&stopped));
        let writer_probe = Probe(Arc::clone(&stopped));
        let writer_cancel = cancel.clone();
        let session = Session {
            runtime: tokio::runtime::Handle::current(),
            generation: 1,
            writer,
            cancellation: cancel,
            reader_task: tokio::spawn(async move {
                let _probe = reader_probe;
                let _ = wait.await;
                Ok(())
            }),
            writer_task: tokio::spawn(async move {
                let _probe = writer_probe;
                writer_cancel.cancelled().await;
                Ok(())
            }),
        };
        realtime
            .inner
            .event_flow
            .start_generation(1)
            .unwrap_or_else(|e| panic!("start: {e}"));
        *realtime.inner.lifecycle.lock() = Lifecycle::Connected(session);
        let mut events = realtime
            .take_event_receiver()
            .unwrap_or_else(|| panic!("receiver"));
        realtime.inner.end_generation(1);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), events.recv_message())
                .await
                .is_err()
        );
        assert!(stopped.load(Ordering::SeqCst) < 2);
        assert!(release.send(()).is_ok());
        let ended = events
            .recv_message()
            .await
            .unwrap_or_else(|| panic!("ended boundary"));
        assert_eq!(ended.generation, RealtimeGeneration(1));
        assert_eq!(ended.event, RealtimeEvent::Disconnected);
        assert_eq!(stopped.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn gap_follows_accepted_events_without_waiting_for_generation_end() {
        let flow = EventFlow::new();
        let (events_tx, events_rx) = mpsc::channel(2);
        let mut receiver = RealtimeEventReceiver {
            events: events_rx,
            flow: Arc::clone(&flow),
            gap_reported: false,
        };
        let generation = 7;
        flow.start_generation(generation)
            .unwrap_or_else(|error| panic!("generation must start: {error}"));
        flow.publish(
            &events_tx,
            generation,
            RealtimeEvent::Message(json!({"sequence": 1})),
            EVENT_BASE_WEIGHT,
        )
        .unwrap_or_else(|error| panic!("first event must enter the queue: {error}"));

        let (terminal_staged_tx, terminal_staged_rx) = oneshot::channel();
        let (finish_tx, finish_rx) = oneshot::channel();
        let producer_flow = Arc::clone(&flow);
        let producer = tokio::spawn(async move {
            assert!(matches!(
                producer_flow.publish(
                    &events_tx,
                    generation,
                    RealtimeEvent::Message(json!({"sequence": 2})),
                    EVENT_BYTE_BUDGET + 1,
                ),
                Err(RealtimeError::EventQueueFull)
            ));
            producer_flow
                .publish(
                    &events_tx,
                    generation,
                    RealtimeEvent::Disconnected,
                    EVENT_BASE_WEIGHT,
                )
                .unwrap_or_else(|error| panic!("terminal event must be staged: {error}"));
            terminal_staged_tx
                .send(())
                .unwrap_or_else(|()| panic!("terminal-staged signal must send"));
            finish_rx
                .await
                .unwrap_or_else(|error| panic!("finish signal must arrive: {error}"));
            producer_flow.finish_generation(generation);
        });

        assert_eq!(
            receiver.recv().await,
            Some(RealtimeEvent::Message(json!({"sequence": 1})))
        );
        terminal_staged_rx
            .await
            .unwrap_or_else(|error| panic!("terminal-staged signal must arrive: {error}"));
        assert_eq!(receiver.recv().await, Some(RealtimeEvent::TransportGap));
        receiver.acknowledge_transport_gap();
        finish_tx
            .send(())
            .unwrap_or_else(|()| panic!("finish signal must send"));
        producer
            .await
            .unwrap_or_else(|error| panic!("producer must join: {error}"));

        assert_eq!(receiver.recv().await, Some(RealtimeEvent::Disconnected));
        flow.start_generation(generation + 1)
            .unwrap_or_else(|error| {
                panic!("acknowledgement must allow the next generation: {error}")
            });
        assert_eq!(receiver.recv().await, None);
        assert_eq!(flow.queued_weight.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn stale_generation_cannot_publish_or_latch_a_gap_after_replacement() {
        let flow = EventFlow::new();
        let (events_tx, events_rx) = mpsc::channel(8);
        let mut receiver = RealtimeEventReceiver {
            events: events_rx,
            flow: Arc::clone(&flow),
            gap_reported: false,
        };

        flow.start_generation(1)
            .unwrap_or_else(|error| panic!("first generation must start: {error}"));
        assert!(matches!(
            flow.start_generation(2),
            Err(RealtimeError::ConnectionCancelled)
        ));
        assert_eq!(
            flow.publish(
                &events_tx,
                1,
                RealtimeEvent::Disconnected,
                EVENT_BASE_WEIGHT,
            )
            .unwrap_or_else(|error| panic!("disconnect must publish: {error}")),
            PublishOutcome::Published
        );
        flow.finish_generation(1);
        flow.start_generation(2)
            .unwrap_or_else(|error| panic!("replacement generation must start: {error}"));
        assert_eq!(
            flow.publish(&events_tx, 2, RealtimeEvent::Reconnected, EVENT_BASE_WEIGHT,)
                .unwrap_or_else(|error| panic!("reconnect must publish: {error}")),
            PublishOutcome::Published
        );

        for weight in [EVENT_BASE_WEIGHT, EVENT_BYTE_BUDGET + 1] {
            assert_eq!(
                flow.publish(
                    &events_tx,
                    1,
                    RealtimeEvent::Message(json!({"generation": 1})),
                    weight,
                )
                .unwrap_or_else(|error| panic!("stale publication must be ignored: {error}")),
                PublishOutcome::StaleGeneration
            );
        }
        assert!(!flow.has_unacknowledged_gap());
        assert_eq!(
            flow.publish(
                &events_tx,
                2,
                RealtimeEvent::Message(json!({"generation": 2})),
                EVENT_BASE_WEIGHT,
            )
            .unwrap_or_else(|error| panic!("replacement message must publish: {error}")),
            PublishOutcome::Published
        );
        flow.finish_generation(2);
        drop(events_tx);

        assert_eq!(receiver.recv().await, Some(RealtimeEvent::Disconnected));
        assert_eq!(receiver.recv().await, Some(RealtimeEvent::Reconnected));
        assert_eq!(
            receiver.recv().await,
            Some(RealtimeEvent::Message(json!({"generation": 2})))
        );
        assert_eq!(receiver.recv().await, None);
    }

    #[tokio::test]
    async fn stale_close_frame_cannot_disable_the_replacement_watchdog() {
        let realtime = fixture_realtime();
        install_connected_generation(&realtime.inner, 2);
        realtime
            .inner
            .reconnect_enabled
            .store(true, Ordering::Release);
        realtime.inner.start_watchdog();
        assert!(
            realtime
                .inner
                .watchdog_task
                .lock()
                .as_ref()
                .is_some_and(|task| !task.is_finished())
        );

        assert_eq!(
            realtime
                .inner
                .process_text(1, "{\"type\":7,\"allowReconnect\":false}\u{001e}")
                .unwrap_or_else(|error| panic!("close frame must decode: {error}")),
            ProcessOutcome::Close
        );
        assert!(realtime.inner.reconnect_enabled.load(Ordering::Acquire));
        assert!(
            realtime
                .inner
                .watchdog_task
                .lock()
                .as_ref()
                .is_some_and(|task| !task.is_finished())
        );

        assert_eq!(
            realtime
                .inner
                .process_text(2, "{\"type\":7,\"allowReconnect\":false}\u{001e}")
                .unwrap_or_else(|error| panic!("close frame must decode: {error}")),
            ProcessOutcome::Close
        );
        assert!(realtime.inner.reconnect_enabled.load(Ordering::Acquire));
        assert!(realtime.inner.watchdog_task.lock().is_some());
    }

    #[tokio::test]
    async fn stale_generation_cannot_refresh_replacement_liveness() {
        let realtime = fixture_realtime();
        install_connected_generation(&realtime.inner, 2);
        let original = Instant::now() - Duration::from_secs(10);
        *realtime.inner.last_activity.lock() = original;

        realtime.inner.record_activity(1);
        assert_eq!(*realtime.inner.last_activity.lock(), original);

        realtime.inner.record_activity(2);
        assert!(*realtime.inner.last_activity.lock() > original);
    }

    #[tokio::test]
    async fn typed_subscriptions_reject_the_wrong_hub_before_network_io() {
        let credentials = crate::Credentials::new("user", "key")
            .unwrap_or_else(|error| panic!("fixture credentials must be valid: {error}"));
        let client = crate::Client::builder(credentials)
            .build()
            .unwrap_or_else(|error| panic!("fixture client must build: {error}"));
        let market = client.realtime(Hub::Market);
        let user = client.realtime(Hub::User);
        let contract = ContractId::new("CON.F.US.MNQ.M26")
            .unwrap_or_else(|error| panic!("fixture contract must be valid: {error}"));

        assert!(matches!(
            market.subscribe_accounts().await,
            Err(RealtimeError::WrongHub)
        ));
        assert!(matches!(
            user.subscribe_contract_trades(&contract).await,
            Err(RealtimeError::WrongHub)
        ));
    }

    #[tokio::test]
    async fn transport_gap_fences_connect_until_acknowledged() {
        let credentials = crate::Credentials::new("user", "key")
            .unwrap_or_else(|error| panic!("fixture credentials must be valid: {error}"));
        let client = crate::Client::builder(credentials)
            .build()
            .unwrap_or_else(|error| panic!("fixture client must build: {error}"));
        let realtime = client.realtime(Hub::Market);
        let mut events = realtime
            .take_event_receiver()
            .unwrap_or_else(|| panic!("event receiver must be available"));
        let generation = 1;
        realtime
            .inner
            .event_flow
            .start_generation(generation)
            .unwrap_or_else(|error| panic!("generation must start: {error}"));
        assert!(matches!(
            realtime.inner.event_flow.publish(
                &realtime.inner.event_tx,
                generation,
                RealtimeEvent::Message(json!({"overflow": true})),
                EVENT_BYTE_BUDGET + 1,
            ),
            Err(RealtimeError::EventQueueFull)
        ));
        realtime
            .inner
            .event_flow
            .publish(
                &realtime.inner.event_tx,
                generation,
                RealtimeEvent::Disconnected,
                EVENT_BASE_WEIGHT,
            )
            .unwrap_or_else(|error| panic!("terminal event must be staged: {error}"));
        realtime.inner.event_flow.finish_generation(generation);

        assert!(matches!(
            realtime.connect().await,
            Err(RealtimeError::TransportGapPending)
        ));
        assert!(matches!(
            events.recv().await,
            Some(RealtimeEvent::TransportGap)
        ));
        assert!(matches!(
            events.recv().await,
            Some(RealtimeEvent::Disconnected)
        ));
        assert!(matches!(
            realtime.connect().await,
            Err(RealtimeError::TransportGapPending)
        ));

        events.acknowledge_transport_gap();
        assert!(matches!(
            realtime.connect().await,
            Err(RealtimeError::MissingAuthToken)
        ));
    }

    #[tokio::test]
    async fn disabled_reconnect_does_not_spawn_a_watchdog() {
        let credentials = crate::Credentials::new("user", "key")
            .unwrap_or_else(|error| panic!("fixture credentials must be valid: {error}"));
        let client = crate::Client::builder(credentials)
            .build()
            .unwrap_or_else(|error| panic!("fixture client must build: {error}"));
        let realtime = client.realtime(Hub::Market);

        realtime.inner.start_watchdog();

        assert!(realtime.inner.watchdog_task.lock().is_none());
    }

    #[test]
    fn websocket_configuration_bounds_every_internal_buffer() {
        let config = websocket_config();
        assert_eq!(config.read_buffer_size, WEBSOCKET_READ_BUFFER_SIZE);
        assert_eq!(config.write_buffer_size, WEBSOCKET_WRITE_BUFFER_SIZE);
        assert_eq!(
            config.max_write_buffer_size,
            WEBSOCKET_MAX_WRITE_BUFFER_SIZE
        );
        assert_eq!(config.max_message_size, Some(WEBSOCKET_MAX_MESSAGE_SIZE));
        assert_eq!(config.max_frame_size, Some(WEBSOCKET_MAX_FRAME_SIZE));
        assert!(config.max_write_buffer_size > config.write_buffer_size);
        assert!(config.max_message_size >= config.max_frame_size);
    }

    #[test]
    fn websocket_accept_matches_the_rfc_6455_vector() {
        assert_eq!(
            websocket_accept("dGhlIHNhbXBsZSBub25jZQ=="),
            "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
        );
        let key = websocket_key()
            .unwrap_or_else(|error| panic!("fixture key generation must succeed: {error}"));
        let decoded = BASE64
            .decode(key.as_bytes())
            .unwrap_or_else(|error| panic!("generated key must be base64: {error}"));
        assert_eq!(decoded.len(), 16);
    }

    #[test]
    fn websocket_upgrade_validation_rejects_untrusted_responses() {
        let expected_accept = websocket_accept("dGhlIHNhbXBsZSBub25jZQ==");
        let mut valid = header::HeaderMap::new();
        valid.insert(
            header::CONNECTION,
            header::HeaderValue::from_static("keep-alive, Upgrade"),
        );
        valid.insert(
            header::UPGRADE,
            header::HeaderValue::from_static("WebSocket"),
        );
        valid.insert(
            header::SEC_WEBSOCKET_ACCEPT,
            header::HeaderValue::from_str(&expected_accept)
                .unwrap_or_else(|error| panic!("fixture accept header must be valid: {error}")),
        );

        assert!(
            validate_websocket_upgrade(
                StatusCode::SWITCHING_PROTOCOLS,
                Version::HTTP_11,
                &valid,
                &expected_accept,
            )
            .is_ok()
        );

        let mut cases = Vec::new();
        cases.push((StatusCode::OK, Version::HTTP_11, valid.clone()));
        cases.push((
            StatusCode::SWITCHING_PROTOCOLS,
            Version::HTTP_2,
            valid.clone(),
        ));
        for missing in [
            header::CONNECTION,
            header::UPGRADE,
            header::SEC_WEBSOCKET_ACCEPT,
        ] {
            let mut headers = valid.clone();
            headers.remove(missing);
            cases.push((StatusCode::SWITCHING_PROTOCOLS, Version::HTTP_11, headers));
        }
        for unsolicited in [
            header::SEC_WEBSOCKET_EXTENSIONS,
            header::SEC_WEBSOCKET_PROTOCOL,
        ] {
            let mut headers = valid.clone();
            headers.insert(unsolicited, header::HeaderValue::from_static("unsupported"));
            cases.push((StatusCode::SWITCHING_PROTOCOLS, Version::HTTP_11, headers));
        }
        let mut duplicate_accept = valid.clone();
        duplicate_accept.append(
            header::SEC_WEBSOCKET_ACCEPT,
            header::HeaderValue::from_str(&expected_accept)
                .unwrap_or_else(|error| panic!("fixture accept header must be valid: {error}")),
        );
        cases.push((
            StatusCode::SWITCHING_PROTOCOLS,
            Version::HTTP_11,
            duplicate_accept,
        ));

        for (status, version, headers) in cases {
            assert!(matches!(
                validate_websocket_upgrade(status, version, &headers, &expected_accept),
                Err(RealtimeError::Transport)
            ));
        }
    }

    #[test]
    fn invocation_encoder_accepts_small_terminated_messages() {
        let message = encode_invocation("1", "SubscribeAccounts", &[])
            .unwrap_or_else(|error| panic!("small invocation must encode: {error}"));
        let Message::Text(text) = message else {
            panic!("invocation must encode as text");
        };
        assert!(text.ends_with(SIGNALR_TERMINATOR));
        assert!(text.len() <= MAX_OUTBOUND_INVOCATION_SIZE);
    }

    #[tokio::test]
    async fn oversized_invocation_is_rejected_before_enqueue() {
        let credentials = crate::Credentials::new("user", "key")
            .unwrap_or_else(|error| panic!("fixture credentials must be valid: {error}"));
        let client = crate::Client::builder(credentials)
            .build()
            .unwrap_or_else(|error| panic!("fixture client must build: {error}"));
        let realtime = client.realtime(Hub::Market);
        let oversized = "x".repeat(MAX_OUTBOUND_INVOCATION_SIZE);

        assert!(matches!(
            realtime
                .invoke("Oversized", vec![Value::String(oversized)])
                .await,
            Err(RealtimeError::OutboundMessageTooLarge {
                max_bytes: MAX_OUTBOUND_INVOCATION_SIZE
            })
        ));
    }
    #[test]
    fn dropping_ready_client_outside_a_runtime_joins_on_its_original_runtime() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap_or_else(|error| panic!("fixture runtime: {error}"));
        let (realtime, mut events) = runtime.block_on(async {
            let realtime = fixture_realtime();
            install_connected_generation(&realtime.inner, 1);
            let events = realtime
                .take_event_receiver()
                .unwrap_or_else(|| panic!("receiver"));
            (realtime, events)
        });
        // Ordinary caller thread: no ambient Tokio context exists here.
        drop(realtime);
        runtime.block_on(async {
            tokio::time::pause();
            assert_eq!(events.recv().await, Some(RealtimeEvent::Disconnected));
            assert_eq!(events.recv().await, None);
        });
    }
}
