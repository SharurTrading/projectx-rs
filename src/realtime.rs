// SPDX-FileCopyrightText: 2026 Kevin Monaghan
// SPDX-License-Identifier: MIT

//! `ProjectX` `SignalR`-over-WebSocket transport.

use std::{
    collections::BTreeMap,
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use futures_util::{SinkExt as _, StreamExt as _};
use parking_lot::Mutex as ParkingMutex;
use serde::de::DeserializeOwned;
use serde_json::Value;
use thiserror::Error;
use tokio::{
    sync::{Mutex, mpsc, oneshot},
    task::JoinHandle,
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Error as TungsteniteError, Message},
};

use crate::{AccountId, ContractId, Endpoints, Error as ClientError, token::TokenStore};

const SIGNALR_TERMINATOR: char = '\u{001e}';
const WRITER_CAPACITY: usize = 1_024;
const EVENT_CAPACITY: usize = 10_000;
const PENDING_INVOCATION_CAPACITY: usize = WRITER_CAPACITY;
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const COMPLETION_TIMEOUT: Duration = Duration::from_secs(15);
const CLOSE_TIMEOUT: Duration = Duration::from_secs(5);
const WATCHDOG_INTERVAL: Duration = Duration::from_secs(5);
const STALE_AFTER: Duration = Duration::from_secs(30);

type SharedWriter = Arc<Mutex<Option<mpsc::Sender<Message>>>>;
type SharedTask = Arc<Mutex<Option<JoinHandle<()>>>>;
type SharedWriterTask = Arc<Mutex<Option<JoinHandle<Result<(), RealtimeError>>>>>;
type SharedReceiver = Arc<Mutex<Option<RealtimeEventReceiver>>>;
type PendingInvocation = oneshot::Sender<Result<(), ()>>;
type PendingInvocations = Arc<ParkingMutex<BTreeMap<String, PendingInvocation>>>;

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
#[derive(Clone, Debug, PartialEq)]
pub struct SignalRInvocation {
    target: String,
    contract_id: Option<ContractId>,
    payload: Value,
}

impl SignalRInvocation {
    /// Decodes a type-1 `SignalR` invocation.
    ///
    /// Non-invocation frames return `Ok(None)`.
    ///
    /// # Errors
    ///
    /// Returns an error when a type-1 frame omits its target or payload.
    pub fn from_value(value: &Value) -> Result<Option<Self>, RealtimeError> {
        if value.get("type").and_then(Value::as_i64) != Some(1) {
            return Ok(None);
        }
        let target = value
            .get("target")
            .and_then(Value::as_str)
            .ok_or(RealtimeError::Protocol("invocation target is missing"))?;
        let arguments = value
            .get("arguments")
            .and_then(Value::as_array)
            .ok_or(RealtimeError::Protocol("invocation arguments are missing"))?;
        let (contract_id, payload) = match arguments.as_slice() {
            [first, second, ..] => {
                let contract_id = first
                    .as_str()
                    .map(ContractId::new)
                    .transpose()
                    .map_err(|_| RealtimeError::Protocol("contract identifier is invalid"))?;
                (contract_id, second.clone())
            }
            [only] => (None, only.clone()),
            [] => return Err(RealtimeError::Protocol("invocation payload is missing")),
        };
        Ok(Some(Self {
            target: target.to_owned(),
            contract_id,
            payload,
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
        serde_json::from_value(self.entity().clone()).map_err(RealtimeError::Decode)
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
    /// A decoded `SignalR` JSON message.
    Message(Value),
}

/// Single-consumer bounded receiver for real-time events.
pub struct RealtimeEventReceiver {
    events: mpsc::Receiver<RealtimeEvent>,
    overflowed: Arc<AtomicBool>,
    gap_reported: bool,
}

impl RealtimeEventReceiver {
    /// Returns whether every producer for this event stream is gone.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.events.is_closed()
    }

    /// Receives the next accepted event or the ordered transport-gap marker.
    pub async fn recv(&mut self) -> Option<RealtimeEvent> {
        match self.events.try_recv() {
            Ok(event) => return Some(event),
            Err(mpsc::error::TryRecvError::Disconnected) => return None,
            Err(mpsc::error::TryRecvError::Empty) => {}
        }
        if self.overflowed.load(Ordering::Acquire) && !self.gap_reported {
            self.gap_reported = true;
            return Some(RealtimeEvent::TransportGap);
        }
        self.events.recv().await
    }

    /// Allows reconnect after the caller has installed its recovery fence.
    pub fn acknowledge_transport_gap(&mut self) {
        if self.gap_reported {
            self.overflowed.store(false, Ordering::Release);
            self.gap_reported = false;
        }
    }
}

impl fmt::Debug for RealtimeEventReceiver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RealtimeEventReceiver")
            .field("overflowed", &self.overflowed.load(Ordering::Acquire))
            .field("gap_reported", &self.gap_reported)
            .finish_non_exhaustive()
    }
}

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
    /// The `SignalR` handshake failed.
    #[error("SignalR handshake failed: {0}")]
    Handshake(&'static str),
    /// A `SignalR` or event payload was malformed.
    #[error("SignalR protocol error: {0}")]
    Protocol(&'static str),
    /// JSON decoding failed.
    #[error("SignalR JSON decoding failed")]
    Decode(#[source] serde_json::Error),
    /// The client is already connected.
    #[error("real-time client is already connected")]
    AlreadyConnected,
    /// The client is not connected.
    #[error("real-time client is not connected")]
    NotConnected,
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
    /// The single event receiver was dropped.
    #[error("real-time event receiver is closed")]
    EventReceiverClosed,
    /// The pending invocation bound was reached.
    #[error("pending SignalR invocation capacity is exhausted")]
    PendingInvocationCapacity,
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
    hub: Hub,
    endpoints: Endpoints,
    token: Arc<TokenStore>,
    writer: SharedWriter,
    reader_task: SharedTask,
    writer_task: SharedWriterTask,
    watchdog_task: SharedTask,
    reconnect_lock: Arc<Mutex<()>>,
    connected: Arc<AtomicBool>,
    shutdown: Arc<AtomicBool>,
    last_activity: Arc<ParkingMutex<Instant>>,
    request_counter: Arc<AtomicU64>,
    pending: PendingInvocations,
    event_tx: mpsc::Sender<RealtimeEvent>,
    event_rx: SharedReceiver,
    overflowed: Arc<AtomicBool>,
}

impl RealtimeClient {
    pub(crate) fn new(hub: Hub, endpoints: Endpoints, token: Arc<TokenStore>) -> Self {
        let (event_tx, event_rx) = mpsc::channel(EVENT_CAPACITY);
        let overflowed = Arc::new(AtomicBool::new(false));
        Self {
            hub,
            endpoints,
            token,
            writer: Arc::new(Mutex::new(None)),
            reader_task: Arc::new(Mutex::new(None)),
            writer_task: Arc::new(Mutex::new(None)),
            watchdog_task: Arc::new(Mutex::new(None)),
            reconnect_lock: Arc::new(Mutex::new(())),
            connected: Arc::new(AtomicBool::new(false)),
            shutdown: Arc::new(AtomicBool::new(false)),
            last_activity: Arc::new(ParkingMutex::new(Instant::now())),
            request_counter: Arc::new(AtomicU64::new(1)),
            pending: Arc::default(),
            event_tx,
            event_rx: Arc::new(Mutex::new(Some(RealtimeEventReceiver {
                events: event_rx,
                overflowed: Arc::clone(&overflowed),
                gap_reported: false,
            }))),
            overflowed,
        }
    }

    /// Claims this client's single event receiver.
    pub async fn take_event_receiver(&self) -> Option<RealtimeEventReceiver> {
        self.event_rx.lock().await.take()
    }

    /// Connects and validates the `SignalR` handshake.
    ///
    /// # Errors
    ///
    /// Returns an error when authentication, URL construction, WebSocket
    /// upgrade, or the `SignalR` handshake fails.
    pub async fn connect(&self) -> Result<(), RealtimeError> {
        self.shutdown.store(false, Ordering::Release);
        self.connect_once(false).await?;
        self.start_watchdog().await;
        Ok(())
    }

    /// Gracefully disconnects and stops background tasks.
    ///
    /// # Errors
    ///
    /// Returns an error if the close handshake does not complete within the
    /// bounded timeout.
    pub async fn disconnect(&self) -> Result<(), RealtimeError> {
        self.shutdown.store(true, Ordering::Release);
        if let Some(task) = self.watchdog_task.lock().await.take() {
            task.abort();
        }

        let writer = self.writer.lock().await.take();
        let mut writer_task = self.writer_task.lock().await.take();
        let mut reader_task = self.reader_task.lock().await.take();
        let close_result = tokio::time::timeout(CLOSE_TIMEOUT, async {
            if let Some(writer) = writer {
                writer
                    .send(Message::Close(None))
                    .await
                    .map_err(|_| RealtimeError::SendClosed)?;
            }
            if let Some(task) = writer_task.as_mut() {
                task.await.map_err(|_| RealtimeError::Close)??;
            }
            if let Some(task) = reader_task.as_mut() {
                task.await.map_err(|_| RealtimeError::Close)?;
            }
            Ok(())
        })
        .await
        .map_err(|_| RealtimeError::Close)?;

        if close_result.is_err() {
            if let Some(task) = writer_task {
                task.abort();
            }
            if let Some(task) = reader_task {
                task.abort();
            }
        }
        self.connected.store(false, Ordering::Release);
        self.fail_pending();
        close_result
    }

    /// Returns whether the latest connection completed its `SignalR` handshake.
    #[must_use]
    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Acquire)
    }

    /// Invokes an arbitrary provider target and waits for its completion frame.
    ///
    /// # Errors
    ///
    /// Returns an error when disconnected, a bounded capacity is exhausted,
    /// the provider rejects the invocation, or completion times out.
    pub async fn invoke(
        &self,
        target: impl Into<String>,
        arguments: Vec<Value>,
    ) -> Result<(), RealtimeError> {
        self.send_invocation(target.into(), arguments).await
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
        self.ensure_hub(Hub::Market)?;
        self.contract_invocation("SubscribeContractTrades", contract)
            .await
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
        self.ensure_hub(Hub::Market)?;
        self.contract_invocation("UnsubscribeContractTrades", contract)
            .await
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
        self.ensure_hub(Hub::Market)?;
        self.contract_invocation("SubscribeContractQuotes", contract)
            .await
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
        self.ensure_hub(Hub::Market)?;
        self.contract_invocation("UnsubscribeContractQuotes", contract)
            .await
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
        self.ensure_hub(Hub::Market)?;
        self.contract_invocation("SubscribeContractMarketDepth", contract)
            .await
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
        self.ensure_hub(Hub::Market)?;
        self.contract_invocation("UnsubscribeContractMarketDepth", contract)
            .await
    }

    /// Subscribes to account updates.
    ///
    /// # Errors
    ///
    /// Returns a real-time invocation error.
    pub async fn subscribe_accounts(&self) -> Result<(), RealtimeError> {
        self.ensure_hub(Hub::User)?;
        self.send_invocation("SubscribeAccounts".to_owned(), Vec::new())
            .await
    }

    /// Unsubscribes from account updates.
    ///
    /// # Errors
    ///
    /// Returns a real-time invocation error.
    pub async fn unsubscribe_accounts(&self) -> Result<(), RealtimeError> {
        self.ensure_hub(Hub::User)?;
        self.send_invocation("UnsubscribeAccounts".to_owned(), Vec::new())
            .await
    }

    /// Subscribes to order updates for an account.
    ///
    /// # Errors
    ///
    /// Returns a real-time invocation error.
    pub async fn subscribe_orders(&self, account: AccountId) -> Result<(), RealtimeError> {
        self.ensure_hub(Hub::User)?;
        self.account_invocation("SubscribeOrders", account).await
    }

    /// Unsubscribes from order updates for an account.
    ///
    /// # Errors
    ///
    /// Returns a real-time invocation error.
    pub async fn unsubscribe_orders(&self, account: AccountId) -> Result<(), RealtimeError> {
        self.ensure_hub(Hub::User)?;
        self.account_invocation("UnsubscribeOrders", account).await
    }

    /// Subscribes to position updates for an account.
    ///
    /// # Errors
    ///
    /// Returns a real-time invocation error.
    pub async fn subscribe_positions(&self, account: AccountId) -> Result<(), RealtimeError> {
        self.ensure_hub(Hub::User)?;
        self.account_invocation("SubscribePositions", account).await
    }

    /// Unsubscribes from position updates for an account.
    ///
    /// # Errors
    ///
    /// Returns a real-time invocation error.
    pub async fn unsubscribe_positions(&self, account: AccountId) -> Result<(), RealtimeError> {
        self.ensure_hub(Hub::User)?;
        self.account_invocation("UnsubscribePositions", account)
            .await
    }

    /// Subscribes to trade updates for an account.
    ///
    /// # Errors
    ///
    /// Returns a real-time invocation error.
    pub async fn subscribe_trades(&self, account: AccountId) -> Result<(), RealtimeError> {
        self.ensure_hub(Hub::User)?;
        self.account_invocation("SubscribeTrades", account).await
    }

    /// Unsubscribes from trade updates for an account.
    ///
    /// # Errors
    ///
    /// Returns a real-time invocation error.
    pub async fn unsubscribe_trades(&self, account: AccountId) -> Result<(), RealtimeError> {
        self.ensure_hub(Hub::User)?;
        self.account_invocation("UnsubscribeTrades", account).await
    }

    fn ensure_hub(&self, expected: Hub) -> Result<(), RealtimeError> {
        if self.hub == expected {
            Ok(())
        } else {
            Err(RealtimeError::WrongHub)
        }
    }

    async fn contract_invocation(
        &self,
        target: &str,
        contract: &ContractId,
    ) -> Result<(), RealtimeError> {
        self.send_invocation(target.to_owned(), vec![Value::String(contract.to_string())])
            .await
    }

    async fn account_invocation(
        &self,
        target: &str,
        account: AccountId,
    ) -> Result<(), RealtimeError> {
        self.send_invocation(target.to_owned(), vec![Value::from(account.get())])
            .await
    }

    async fn start_watchdog(&self) {
        if self.watchdog_task.lock().await.is_some() {
            return;
        }
        let client = self.clone();
        let task = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(WATCHDOG_INTERVAL);
            loop {
                ticker.tick().await;
                if client.shutdown.load(Ordering::Acquire) {
                    break;
                }
                let stale = client.last_activity.lock().elapsed() > STALE_AFTER;
                let should_reconnect =
                    !client.overflowed.load(Ordering::Acquire) && (!client.is_connected() || stale);
                if should_reconnect && let Err(error) = client.reconnect().await {
                    tracing::warn!(%error, hub = ?client.hub, "ProjectX real-time reconnect failed");
                }
            }
        });
        *self.watchdog_task.lock().await = Some(task);
    }

    async fn reconnect(&self) -> Result<(), RealtimeError> {
        let _reconnect = self.reconnect_lock.lock().await;
        if self.shutdown.load(Ordering::Acquire) || self.overflowed.load(Ordering::Acquire) {
            return Ok(());
        }
        if let Some(task) = self.reader_task.lock().await.take() {
            task.abort();
        }
        if let Some(task) = self.writer_task.lock().await.take() {
            task.abort();
        }
        self.fail_pending();
        self.writer.lock().await.take();
        self.connected.store(false, Ordering::Release);
        self.connect_once(true).await
    }

    async fn connect_once(&self, reconnecting: bool) -> Result<(), RealtimeError> {
        if self.writer.lock().await.is_some() {
            return Err(RealtimeError::AlreadyConnected);
        }
        let url = self.connection_url().await?;
        let (stream, _) = connect_async(url.as_str())
            .await
            .map_err(|_| RealtimeError::Transport)?;
        let (mut write, mut read) = stream.split();
        let (writer_tx, mut writer_rx) = mpsc::channel::<Message>(WRITER_CAPACITY);
        let writer_task = tokio::spawn(async move {
            while let Some(message) = writer_rx.recv().await {
                write
                    .send(message)
                    .await
                    .map_err(|_| RealtimeError::Transport)?;
            }
            Ok(())
        });
        *self.writer.lock().await = Some(writer_tx);

        if let Err(error) = self.send_handshake().await {
            self.writer.lock().await.take();
            writer_task.abort();
            return Err(error);
        }
        let handshake_tail =
            match tokio::time::timeout(HANDSHAKE_TIMEOUT, self.await_handshake(&mut read)).await {
                Ok(result) => result,
                Err(_) => Err(RealtimeError::Handshake("response timed out")),
            };
        let handshake_tail = match handshake_tail {
            Ok(tail) => tail,
            Err(error) => {
                self.writer.lock().await.take();
                writer_task.abort();
                return Err(error);
            }
        };
        *self.writer_task.lock().await = Some(writer_task);
        self.record_activity();
        self.connected.store(true, Ordering::Release);

        let lifecycle = if reconnecting {
            RealtimeEvent::Reconnected
        } else {
            RealtimeEvent::Connected
        };
        if let Err(error) = self.publish(lifecycle) {
            if let Some(task) = self.writer_task.lock().await.take() {
                task.abort();
            }
            self.writer.lock().await.take();
            self.connected.store(false, Ordering::Release);
            return Err(error);
        }

        let client = self.clone();
        let reader_task = tokio::spawn(async move {
            run_reader(client, read, handshake_tail).await;
        });
        *self.reader_task.lock().await = Some(reader_task);
        Ok(())
    }

    async fn connection_url(&self) -> Result<url::Url, RealtimeError> {
        let token = self
            .token
            .snapshot()
            .await
            .filter(|token| !token.trim().is_empty())
            .ok_or(RealtimeError::MissingAuthToken)?;
        let mut url = self
            .endpoints
            .hub_url(self.hub.path())
            .map_err(RealtimeError::Endpoint)?;
        url.query_pairs_mut().append_pair("access_token", &token);
        Ok(url)
    }

    async fn send_handshake(&self) -> Result<(), RealtimeError> {
        let payload = format!(
            "{}{}",
            serde_json::json!({"protocol":"json","version":1}),
            SIGNALR_TERMINATOR
        );
        self.send_message(Message::Text(payload.into())).await
    }

    async fn await_handshake<S>(&self, read: &mut S) -> Result<Option<String>, RealtimeError>
    where
        S: futures_util::Stream<Item = Result<Message, TungsteniteError>> + Unpin,
    {
        loop {
            match read.next().await {
                Some(Ok(Message::Text(text))) => return validate_handshake(text.as_ref()),
                Some(Ok(Message::Binary(bytes))) => {
                    let text = std::str::from_utf8(bytes.as_ref())
                        .map_err(|_| RealtimeError::Handshake("response was not UTF-8"))?;
                    return validate_handshake(text);
                }
                Some(Ok(Message::Ping(payload))) => {
                    self.send_message(Message::Pong(payload)).await?;
                }
                Some(Ok(Message::Pong(_) | Message::Frame(_))) => {}
                Some(Ok(Message::Close(_))) | None => {
                    return Err(RealtimeError::Handshake(
                        "connection closed before the response",
                    ));
                }
                Some(Err(_)) => return Err(RealtimeError::Transport),
            }
        }
    }

    async fn send_invocation(
        &self,
        target: String,
        arguments: Vec<Value>,
    ) -> Result<(), RealtimeError> {
        let invocation_id = self
            .request_counter
            .fetch_add(1, Ordering::AcqRel)
            .to_string();
        let (reply_tx, reply_rx) = oneshot::channel();
        {
            let mut pending = self.pending.lock();
            if pending.len() >= PENDING_INVOCATION_CAPACITY {
                return Err(RealtimeError::PendingInvocationCapacity);
            }
            pending.insert(invocation_id.clone(), reply_tx);
        }
        let payload = serde_json::json!({
            "type": 1,
            "invocationId": invocation_id,
            "target": target,
            "arguments": arguments,
        });
        if let Err(error) = self
            .send_message(Message::Text(
                format!("{payload}{SIGNALR_TERMINATOR}").into(),
            ))
            .await
        {
            self.pending.lock().remove(&invocation_id);
            return Err(error);
        }

        match tokio::time::timeout(COMPLETION_TIMEOUT, reply_rx).await {
            Ok(Ok(Ok(()))) => Ok(()),
            Ok(Ok(Err(()))) => Err(RealtimeError::InvocationRejected { target }),
            Ok(Err(_)) => Err(RealtimeError::InvocationSessionEnded { target }),
            Err(_) => {
                self.pending.lock().remove(&invocation_id);
                Err(RealtimeError::InvocationTimedOut { target })
            }
        }
    }

    async fn send_message(&self, message: Message) -> Result<(), RealtimeError> {
        let writer = self.writer.lock().await;
        let writer = writer.as_ref().ok_or(RealtimeError::NotConnected)?;
        writer.try_send(message).map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => RealtimeError::SendQueueFull,
            mpsc::error::TrySendError::Closed(_) => RealtimeError::SendClosed,
        })
    }

    fn process_text(&self, text: &str) -> Result<(), RealtimeError> {
        for frame in text
            .split(SIGNALR_TERMINATOR)
            .filter(|frame| !frame.is_empty())
        {
            let value: Value = serde_json::from_str(frame).map_err(RealtimeError::Decode)?;
            if let Some((invocation_id, result)) = completion(&value) {
                if let Some(reply) = self.pending.lock().remove(invocation_id) {
                    let _ = reply.send(result);
                }
                continue;
            }
            self.publish(RealtimeEvent::Message(value))?;
        }
        Ok(())
    }

    fn publish(&self, event: RealtimeEvent) -> Result<(), RealtimeError> {
        match self.event_tx.try_send(event) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.overflowed.store(true, Ordering::Release);
                self.connected.store(false, Ordering::Release);
                Err(RealtimeError::EventQueueFull)
            }
            Err(mpsc::error::TrySendError::Closed(_)) => Err(RealtimeError::EventReceiverClosed),
        }
    }

    fn fail_pending(&self) {
        self.pending.lock().clear();
    }

    fn record_activity(&self) {
        *self.last_activity.lock() = Instant::now();
    }
}

impl Clone for RealtimeClient {
    fn clone(&self) -> Self {
        Self {
            hub: self.hub,
            endpoints: self.endpoints.clone(),
            token: Arc::clone(&self.token),
            writer: Arc::clone(&self.writer),
            reader_task: Arc::clone(&self.reader_task),
            writer_task: Arc::clone(&self.writer_task),
            watchdog_task: Arc::clone(&self.watchdog_task),
            reconnect_lock: Arc::clone(&self.reconnect_lock),
            connected: Arc::clone(&self.connected),
            shutdown: Arc::clone(&self.shutdown),
            last_activity: Arc::clone(&self.last_activity),
            request_counter: Arc::clone(&self.request_counter),
            pending: Arc::clone(&self.pending),
            event_tx: self.event_tx.clone(),
            event_rx: Arc::clone(&self.event_rx),
            overflowed: Arc::clone(&self.overflowed),
        }
    }
}

impl fmt::Debug for RealtimeClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RealtimeClient")
            .field("hub", &self.hub)
            .field("endpoints", &self.endpoints)
            .field("connected", &self.is_connected())
            .finish_non_exhaustive()
    }
}

async fn run_reader<S>(client: RealtimeClient, mut read: S, handshake_tail: Option<String>)
where
    S: futures_util::Stream<Item = Result<Message, TungsteniteError>> + Unpin,
{
    let overflowed = handshake_tail.as_deref().is_some_and(|tail| {
        matches!(
            client.process_text(tail),
            Err(RealtimeError::EventQueueFull)
        )
    });
    while !overflowed && let Some(message) = read.next().await {
        match message {
            Ok(Message::Text(text)) => {
                client.record_activity();
                if client.process_text(text.as_ref()).is_err() {
                    break;
                }
            }
            Ok(Message::Binary(bytes)) => {
                client.record_activity();
                let Ok(text) = std::str::from_utf8(bytes.as_ref()) else {
                    break;
                };
                if client.process_text(text).is_err() {
                    break;
                }
            }
            Ok(Message::Ping(payload)) => {
                let _ = client.send_message(Message::Pong(payload)).await;
            }
            Ok(Message::Close(_)) | Err(_) => break,
            Ok(Message::Pong(_) | Message::Frame(_)) => {}
        }
    }
    client.connected.store(false, Ordering::Release);
    client.fail_pending();
    let _ = client.publish(RealtimeEvent::Disconnected);
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

fn completion(value: &Value) -> Option<(&str, Result<(), ()>)> {
    (value.get("type")?.as_u64()? == 3).then_some(())?;
    let invocation_id = value.get("invocationId")?.as_str()?;
    let result = if value.get("error").and_then(Value::as_str).is_some() {
        Err(())
    } else {
        Ok(())
    };
    Some((invocation_id, result))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn invocation_extracts_market_contract_and_payload() {
        let value = json!({
            "type": 1,
            "target": "GatewayTrade",
            "arguments": ["CON.F.US.MNQ.M26", {"price": 1.25}],
        });
        let invocation = SignalRInvocation::from_value(&value)
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
}
