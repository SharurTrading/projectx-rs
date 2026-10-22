// SPDX-FileCopyrightText: 2026 Kevin Monaghan
// SPDX-License-Identifier: MIT-0

//! Connection lifecycle state machine, invocation admission, and record processing.

use std::{
    collections::BTreeMap,
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use futures_util::StreamExt as _;
use parking_lot::Mutex as ParkingMutex;
use serde_json::Value;
use tokio::{
    sync::{Notify, mpsc, oneshot},
    task::JoinHandle,
};
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

use crate::{Endpoints, token::TokenStore};

use super::config::RealtimeConfig;
use super::error::RealtimeError;
use super::event::{Hub, RealtimeEvent};
use super::event_flow::{
    EventEnvelope, EventFlow, PublishOutcome, RealtimeEventReceiver, RealtimeGeneration,
};
use super::invocation::{
    SIGNALR_TERMINATOR, SignalRFrame, SignalRInvocation, completion, encode_invocation,
};
use super::socket::{
    join_socket_tasks, negotiate_handshake, run_reader, run_writer, send_queued, upgrade_websocket,
};

const COALESCED_RECORD_QUANTUM: usize = 32;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
pub(super) const CLOSE_TIMEOUT: Duration = Duration::from_secs(5);
const WATCHDOG_INTERVAL: Duration = Duration::from_secs(5);

pub(super) type PendingInvocation = oneshot::Sender<Result<(), ()>>;

pub(super) struct PendingEntry {
    pub(super) generation: u64,
    pub(super) reply: PendingInvocation,
}

pub(super) type PendingInvocations = Arc<ParkingMutex<BTreeMap<String, PendingEntry>>>;

pub(super) struct RealtimeInner {
    pub(super) hub: Hub,
    pub(super) endpoints: Endpoints,
    pub(super) http: reqwest::Client,
    pub(super) token: Arc<TokenStore>,
    pub(super) config: RealtimeConfig,
    pub(super) lifecycle: ParkingMutex<Lifecycle>,
    pub(super) lifecycle_changed: Notify,
    pub(super) generation: AtomicU64,
    pub(super) reconnect_enabled: AtomicBool,
    pub(super) owner_cancel: CancellationToken,
    pub(super) client_handles: AtomicUsize,
    pub(super) watchdog_task: ParkingMutex<Option<JoinHandle<()>>>,
    pub(super) socket_probe: ParkingMutex<Option<(u64, u64)>>,
    pub(super) request_counter: AtomicU64,
    pub(super) pending: PendingInvocations,
    pub(super) event_tx: mpsc::UnboundedSender<EventEnvelope>,
    pub(super) event_rx: ParkingMutex<Option<RealtimeEventReceiver>>,
    pub(super) event_flow: Arc<EventFlow>,
}

pub(super) enum Lifecycle {
    Disconnected,
    Connecting {
        generation: u64,
        cancellation: CancellationToken,
    },
    Connected(Session),
    Closing {
        generation: u64,
        was_connected: bool,
        close_failed: Arc<AtomicBool>,
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

pub(super) struct Session {
    pub(super) runtime: tokio::runtime::Handle,
    pub(super) generation: u64,
    pub(super) writer: mpsc::Sender<Message>,
    pub(super) cancellation: CancellationToken,
    // Waiters retain this generation's outcome across replacement sessions.
    pub(super) close_failed: Arc<AtomicBool>,
    pub(super) reader_task: JoinHandle<Result<(), RealtimeError>>,
    pub(super) writer_task: JoinHandle<Result<(), RealtimeError>>,
}

pub(super) struct ConnectClaim {
    pub(super) inner: Weak<RealtimeInner>,
    pub(super) generation: u64,
    pub(super) cancellation: CancellationToken,
    pub(super) complete: bool,
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

pub(super) enum DisconnectAction {
    None,
    Wait {
        generation: u64,
        close_failed: Arc<AtomicBool>,
    },
    Close(Session),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ProcessOutcome {
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

impl RealtimeInner {
    pub(super) fn is_connected(&self) -> bool {
        matches!(*self.lifecycle.lock(), Lifecycle::Connected(_))
    }

    pub(super) fn begin_connect(
        self: &Arc<Self>,
        reconnecting: bool,
    ) -> Result<ConnectClaim, RealtimeError> {
        self.begin_connect_with_pre_lock(reconnecting, || {})
    }

    pub(super) fn begin_connect_with_pre_lock<F>(
        self: &Arc<Self>,
        reconnecting: bool,
        before_lifecycle_lock: F,
    ) -> Result<ConnectClaim, RealtimeError>
    where
        F: FnOnce(),
    {
        before_lifecycle_lock();
        let mut lifecycle = self.lifecycle.lock();
        self.ensure_event_receiver()?;
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

    pub(super) async fn connect_once(
        self: &Arc<Self>,
        reconnecting: bool,
    ) -> Result<(), RealtimeError> {
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

        let start = CancellationToken::new();
        let event = if reconnecting {
            RealtimeEvent::Reconnected
        } else {
            RealtimeEvent::Connected
        };
        self.install_ready(
            claim.generation,
            || {
                let (write, read) = stream.split();
                let (writer, writer_rx) = mpsc::channel(self.config.writer_capacity);
                let cancellation = claim.cancellation.child_token();
                let writer_task = tokio::spawn(run_writer(
                    Arc::downgrade(self),
                    claim.generation,
                    write,
                    writer_rx,
                    cancellation.clone(),
                    start.clone(),
                ));
                let reader_task = tokio::spawn(run_reader(
                    Arc::downgrade(self),
                    claim.generation,
                    read,
                    handshake_tail,
                    cancellation.clone(),
                    start.clone(),
                ));
                Session {
                    runtime: tokio::runtime::Handle::current(),
                    generation: claim.generation,
                    writer,
                    cancellation,
                    close_failed: Arc::new(AtomicBool::new(false)),
                    reader_task,
                    writer_task,
                }
            },
            event,
        )?;
        start.cancel();
        self.start_watchdog();
        claim.complete();
        Ok(())
    }

    pub(super) fn install_ready<F>(
        self: &Arc<Self>,
        generation: u64,
        create_session: F,
        event: RealtimeEvent,
    ) -> Result<(), RealtimeError>
    where
        F: FnOnce() -> Session,
    {
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
        // Validate installation before spawning either socket task. The lifecycle
        // guard fences cancellation until both handles have an installed owner.
        *lifecycle = Lifecycle::Connected(create_session());
        let published = self.publish(generation, event);
        drop(lifecycle);
        self.lifecycle_changed.notify_waiters();
        match published {
            Ok(PublishOutcome::Published) => Ok(()),
            Ok(PublishOutcome::StaleGeneration | PublishOutcome::GapFenced) => {
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

    pub(super) fn begin_disconnect(&self) -> DisconnectAction {
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
                let close_failed = Arc::new(AtomicBool::new(false));
                *lifecycle = Lifecycle::Closing {
                    generation,
                    was_connected: false,
                    close_failed: Arc::clone(&close_failed),
                };
                DisconnectAction::Wait {
                    generation,
                    close_failed,
                }
            }
            Lifecycle::Connected(session) => {
                let generation = session.generation;
                *lifecycle = Lifecycle::Closing {
                    generation,
                    was_connected: true,
                    close_failed: Arc::clone(&session.close_failed),
                };
                DisconnectAction::Close(session)
            }
            Lifecycle::Closing {
                generation,
                was_connected,
                close_failed,
            } => {
                *lifecycle = Lifecycle::Closing {
                    generation,
                    was_connected,
                    close_failed: Arc::clone(&close_failed),
                };
                DisconnectAction::Wait {
                    generation,
                    close_failed,
                }
            }
        };
        drop(lifecycle);
        self.lifecycle_changed.notify_waiters();
        action
    }

    pub(super) async fn disconnect(self: &Arc<Self>) -> Result<(), RealtimeError> {
        match self.begin_disconnect() {
            DisconnectAction::None => Ok(()),
            DisconnectAction::Wait {
                generation,
                close_failed,
            } => {
                let waited = tokio::time::timeout(
                    CLOSE_TIMEOUT,
                    self.wait_until_generation_ends(generation),
                )
                .await;
                if waited.is_err() || close_failed.load(Ordering::Acquire) {
                    Err(RealtimeError::Close)
                } else {
                    Ok(())
                }
            }
            DisconnectAction::Close(session) => {
                let generation = session.generation;
                let close_failed = Arc::clone(&session.close_failed);
                self.retire_session(session, true);
                self.wait_until_generation_ends(generation).await;
                if close_failed.load(Ordering::Acquire) {
                    Err(RealtimeError::Close)
                } else {
                    Ok(())
                }
            }
        }
    }

    pub(super) async fn wait_until_generation_ends(&self, generation: u64) {
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

    pub(super) fn finish_closing(&self, generation: u64) {
        let mut lifecycle = self.lifecycle.lock();
        let should_publish = match &*lifecycle {
            Lifecycle::Closing {
                generation: active,
                was_connected,
                ..
            } if *active == generation => *was_connected,
            _ => return,
        };
        self.finish_event_generation_locked(generation, should_publish);
        *lifecycle = Lifecycle::Disconnected;
        drop(lifecycle);
        self.fail_pending_generation(generation);
        self.lifecycle_changed.notify_waiters();
    }

    pub(super) fn end_generation(self: &Arc<Self>, generation: u64) {
        let mut lifecycle = self.lifecycle.lock();
        let previous = std::mem::replace(&mut *lifecycle, Lifecycle::Disconnected);
        match previous {
            Lifecycle::Connected(session) if session.generation == generation => {
                *lifecycle = Lifecycle::Closing {
                    generation,
                    was_connected: true,
                    close_failed: Arc::clone(&session.close_failed),
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
                ..
            } if active == generation => {
                drop(lifecycle);
                self.lifecycle_changed.notify_waiters();
            }
            other => {
                *lifecycle = other;
            }
        }
    }

    // Lifecycle::Closing tracks this supervisor until both socket tasks join.
    // A reader can request retirement without attempting to join itself. The
    // supervisor deliberately outlives callers that cancel a disconnect wait.
    fn retire_session(self: &Arc<Self>, mut session: Session, graceful: bool) {
        let generation = session.generation;
        if !graceful {
            session.cancellation.cancel();
        }
        self.fail_pending_generation(generation);
        let inner = Arc::clone(self);
        // Drop can run on an ordinary thread. Retire on the socket's original
        // runtime, preserving joined evidence without requiring ambient context.
        let runtime = session.runtime.clone();
        runtime.spawn(async move {
            // A stuck writer cannot delay a real transport-loss boundary forever.
            // Abort only after cancellation, then join before publishing evidence.
            if graceful && send_queued(&session.writer, Message::Close(None)).is_err() {
                session.cancellation.cancel();
                session.close_failed.store(true, Ordering::Release);
            }
            if !join_socket_tasks(&mut session, graceful).await {
                session.close_failed.store(true, Ordering::Release);
            }
            inner.finish_closing(generation);
        });
    }

    pub(super) fn shutdown_now(self: &Arc<Self>) {
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
            let _ = self.publish(generation, RealtimeEvent::Disconnected);
        }
        self.event_flow.finish_generation(generation);
    }

    pub(super) fn start_watchdog(self: &Arc<Self>) {
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
            .versioned_snapshot()
            .ok_or(RealtimeError::MissingAuthToken)?;
        let mut url = self
            .endpoints
            .hub_url(self.hub.path())
            .map_err(RealtimeError::Endpoint)?;
        url.query_pairs_mut()
            .append_pair("access_token", token.expose());
        Ok(url)
    }

    pub(super) async fn send_invocation(
        self: &Arc<Self>,
        expected: Option<RealtimeGeneration>,
        target: String,
        arguments: &[Value],
    ) -> Result<(), RealtimeError> {
        let invocation_id = self
            .request_counter
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                value.checked_add(1)
            })
            .map_err(|_| RealtimeError::IdentifierCapacity)?
            .to_string();
        let message = encode_invocation(&invocation_id, &target, arguments)?;
        let (reply_tx, reply_rx) = oneshot::channel();
        let (generation, writer) = self.register_invocation(expected, &invocation_id, reply_tx)?;
        let _pending = PendingGuard {
            pending: Arc::clone(&self.pending),
            invocation_id: invocation_id.clone(),
            generation,
        };
        send_queued(&writer, message)?;

        match tokio::time::timeout(self.config.invocation_timeout, reply_rx).await {
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
        if pending.len() >= self.config.pending_capacity {
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

    pub(super) async fn process_text(
        &self,
        generation: u64,
        text: &str,
    ) -> Result<ProcessOutcome, RealtimeError> {
        self.ensure_event_receiver()?;
        let mut records = text.split(SIGNALR_TERMINATOR).enumerate().peekable();
        // A coalesced frame must not monopolize a current-thread executor.
        // This is a scheduling quantum, not a record or queue limit.
        while let Some((index, frame)) = records.next() {
            if index != 0 && index % COALESCED_RECORD_QUANTUM == 0 {
                tokio::task::yield_now().await;
            }
            if records.peek().is_none() {
                if !frame.is_empty() {
                    self.event_flow.mark_gap(generation);
                }
                break;
            }
            match self.process_record(generation, frame) {
                Ok(ProcessOutcome::Continue) => {}
                Ok(ProcessOutcome::Close) => return Ok(ProcessOutcome::Close),
                Err(RealtimeError::Decode(_) | RealtimeError::Protocol(_)) => {
                    self.event_flow.mark_gap(generation);
                }
                Err(error) => return Err(error),
            }
        }
        Ok(ProcessOutcome::Continue)
    }

    pub(super) fn process_record(
        &self,
        generation: u64,
        frame: &str,
    ) -> Result<ProcessOutcome, RealtimeError> {
        let header: SignalRFrame<'_> =
            serde_json::from_str(frame).map_err(RealtimeError::Decode)?;
        match header.message_type()? {
            1 => {
                let invocation = SignalRInvocation::from_frame(&header)?.ok_or(
                    RealtimeError::Protocol("type-1 frame was not an invocation"),
                )?;
                if self.publish(generation, RealtimeEvent::Invocation(invocation))?
                    == PublishOutcome::StaleGeneration
                {
                    return Ok(ProcessOutcome::Close);
                }
            }
            3 => {
                let value: Value = serde_json::from_str(frame).map_err(RealtimeError::Decode)?;
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
                let value: Value = serde_json::from_str(frame).map_err(RealtimeError::Decode)?;
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
                let value: Value = serde_json::from_str(frame).map_err(RealtimeError::Decode)?;
                if self.publish(generation, RealtimeEvent::Message(value))?
                    == PublishOutcome::StaleGeneration
                {
                    return Ok(ProcessOutcome::Close);
                }
            }
        }
        Ok(ProcessOutcome::Continue)
    }

    pub(super) fn ensure_event_receiver(&self) -> Result<(), RealtimeError> {
        if self.event_tx.is_closed() {
            self.reconnect_enabled.store(false, Ordering::Release);
            self.stop_watchdog();
            return Err(RealtimeError::EventReceiverClosed);
        }
        Ok(())
    }

    pub(super) fn publish(
        &self,
        generation: u64,
        event: RealtimeEvent,
    ) -> Result<PublishOutcome, RealtimeError> {
        match self.event_flow.publish(&self.event_tx, generation, event) {
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

    pub(super) fn acknowledge_probe(&self, payload: &[u8]) {
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
