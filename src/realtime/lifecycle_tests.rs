// SPDX-FileCopyrightText: 2026 Kevin Monaghan
// SPDX-License-Identifier: MIT-0

//! Unit tests for connect, disconnect, watchdog, and generation lifecycle behavior.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use serde_json::json;
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

use crate::ContractId;

use super::RealtimeClient;
use super::error::RealtimeError;
use super::event::{Hub, RealtimeEvent};
use super::event_flow::{PublishOutcome, RealtimeGeneration};
use super::invocation::SIGNALR_TERMINATOR;
use super::lifecycle::{
    CLOSE_TIMEOUT, ConnectClaim, DisconnectAction, Lifecycle, ProcessOutcome, RealtimeInner,
    Session,
};

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
        close_failed: Arc::new(AtomicBool::new(false)),
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
        close_failed: Arc::new(AtomicBool::new(false)),
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
            realtime.inner.process_text(1, batch).await,
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
    realtime.inner.event_flow.mark_gap(1);
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
            ..
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
        DisconnectAction::Wait { generation: 1, .. }
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
            ..
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
        close_failed: Arc::new(AtomicBool::new(false)),
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
    for sequence in 0..3 {
        assert!(matches!(
            realtime
                .inner
                .publish(1, RealtimeEvent::Message(json!({"sequence": sequence})),),
            Ok(PublishOutcome::Published)
        ));
    }
    realtime.inner.end_generation(1);
    for sequence in 0..3 {
        assert_eq!(
            events.recv().await,
            Some(RealtimeEvent::Message(json!({"sequence": sequence})))
        );
    }
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
            .await
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
            .await
            .unwrap_or_else(|error| panic!("close frame must decode: {error}")),
        ProcessOutcome::Close
    );
    assert!(realtime.inner.reconnect_enabled.load(Ordering::Acquire));
    assert!(realtime.inner.watchdog_task.lock().is_some());
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
    realtime.inner.event_flow.mark_gap(generation);
    realtime
        .inner
        .event_flow
        .publish(
            &realtime.inner.event_tx,
            generation,
            RealtimeEvent::Disconnected,
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
