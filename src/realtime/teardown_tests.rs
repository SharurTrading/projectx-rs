// SPDX-FileCopyrightText: 2026 Kevin Monaghan
// SPDX-License-Identifier: MIT-0

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

use super::error::RealtimeError;
use super::event::{Hub, RealtimeEvent};
use super::lifecycle::{Lifecycle, Session};
use super::socket::join_socket_tasks;

fn session_with_tasks(
    reader_task: JoinHandle<Result<(), RealtimeError>>,
    writer_task: JoinHandle<Result<(), RealtimeError>>,
) -> Session {
    let (writer, _writer_rx) = mpsc::channel(1);
    Session {
        runtime: tokio::runtime::Handle::current(),
        generation: 1,
        writer,
        cancellation: CancellationToken::new(),
        close_failed: Arc::new(AtomicBool::new(false)),
        reader_task,
        writer_task,
    }
}

#[tokio::test]
async fn graceful_join_preserves_writer_failure_and_still_joins_reader() {
    let release_reader = CancellationToken::new();
    let reader_release = release_reader.clone();
    let reader_finished = Arc::new(AtomicBool::new(false));
    let reader_witness = Arc::clone(&reader_finished);
    let mut session = session_with_tasks(
        tokio::spawn(async move {
            reader_release.cancelled().await;
            reader_witness.store(true, Ordering::Release);
            Ok(())
        }),
        tokio::spawn(async { Err(RealtimeError::Transport) }),
    );
    let joined = join_socket_tasks(&mut session, true);
    tokio::pin!(joined);
    assert!(futures_util::poll!(&mut joined).is_pending());
    assert!(!reader_finished.load(Ordering::Acquire));
    release_reader.cancel();
    assert!(!joined.await);
    assert!(reader_finished.load(Ordering::Acquire));
}

#[tokio::test]
async fn graceful_join_reports_cancelled_socket_task_as_failure() {
    let writer = tokio::spawn(std::future::pending::<Result<(), RealtimeError>>());
    writer.abort();
    let mut session = session_with_tasks(tokio::spawn(async { Ok(()) }), writer);

    assert!(!join_socket_tasks(&mut session, true).await);
    assert!(session.reader_task.is_finished());
    assert!(session.writer_task.is_finished());
}

#[tokio::test]
async fn forced_join_accepts_cancelled_socket_tasks_after_teardown() {
    let writer = tokio::spawn(std::future::pending::<Result<(), RealtimeError>>());
    writer.abort();
    let mut session = session_with_tasks(
        tokio::spawn(async { Err(RealtimeError::Transport) }),
        writer,
    );
    session.cancellation.cancel();

    assert!(join_socket_tasks(&mut session, false).await);
    assert!(session.reader_task.is_finished());
    assert!(session.writer_task.is_finished());
}

#[tokio::test]
async fn concurrent_disconnect_waiters_both_receive_graceful_writer_failure() {
    let credentials = crate::Credentials::new("user", "key")
        .unwrap_or_else(|error| panic!("fixture credentials: {error}"));
    let client = crate::Client::builder(credentials)
        .build()
        .unwrap_or_else(|error| panic!("fixture client: {error}"));
    let realtime = client.realtime(Hub::Market);
    let mut events = realtime
        .take_event_receiver()
        .unwrap_or_else(|| panic!("fixture event receiver"));
    let release_tasks = CancellationToken::new();
    let reader_release = release_tasks.clone();
    let writer_release = release_tasks.clone();
    let reader_finished = Arc::new(AtomicBool::new(false));
    let writer_finished = Arc::new(AtomicBool::new(false));
    let reader_witness = Arc::clone(&reader_finished);
    let writer_witness = Arc::clone(&writer_finished);
    let (writer, mut writer_rx) = mpsc::channel(1);
    let session = Session {
        runtime: tokio::runtime::Handle::current(),
        generation: 1,
        writer,
        cancellation: CancellationToken::new(),
        close_failed: Arc::new(AtomicBool::new(false)),
        reader_task: tokio::spawn(async move {
            reader_release.cancelled().await;
            reader_witness.store(true, Ordering::Release);
            Ok(())
        }),
        writer_task: tokio::spawn(async move {
            assert!(matches!(writer_rx.recv().await, Some(Message::Close(_))));
            writer_release.cancelled().await;
            writer_witness.store(true, Ordering::Release);
            Err(RealtimeError::Transport)
        }),
    };
    realtime
        .inner
        .event_flow
        .start_generation(1)
        .unwrap_or_else(|error| panic!("fixture event generation: {error}"));
    *realtime.inner.lifecycle.lock() = Lifecycle::Connected(session);

    let first = realtime.disconnect();
    tokio::pin!(first);
    assert!(futures_util::poll!(&mut first).is_pending());
    let second = realtime.disconnect();
    tokio::pin!(second);
    assert!(futures_util::poll!(&mut second).is_pending());
    assert_eq!(events.queued_event_count(), 0);
    release_tasks.cancel();

    let (first_result, second_result) = tokio::join!(first, second);
    assert!(matches!(first_result, Err(RealtimeError::Close)));
    assert!(matches!(second_result, Err(RealtimeError::Close)));
    assert!(reader_finished.load(Ordering::Acquire));
    assert!(writer_finished.load(Ordering::Acquire));
    assert!(!realtime.is_connected());
    assert_eq!(events.recv().await, Some(RealtimeEvent::Disconnected));
}

#[tokio::test]
async fn delayed_disconnect_waiters_retain_failure_after_another_generation_closes() {
    let credentials = crate::Credentials::new("user", "key")
        .unwrap_or_else(|error| panic!("fixture credentials: {error}"));
    let client = crate::Client::builder(credentials)
        .build()
        .unwrap_or_else(|error| panic!("fixture client: {error}"));
    let realtime = client.realtime(Hub::Market);
    let mut events = realtime
        .take_event_receiver()
        .unwrap_or_else(|| panic!("fixture event receiver"));

    let install_failing_generation = |generation| {
        let (writer, mut writer_rx) = mpsc::channel(1);
        let session = Session {
            runtime: tokio::runtime::Handle::current(),
            generation,
            writer,
            cancellation: CancellationToken::new(),
            close_failed: Arc::new(AtomicBool::new(false)),
            reader_task: tokio::spawn(async { Ok(()) }),
            writer_task: tokio::spawn(async move {
                assert!(matches!(writer_rx.recv().await, Some(Message::Close(_))));
                Err(RealtimeError::Transport)
            }),
        };
        realtime
            .inner
            .event_flow
            .start_generation(generation)
            .unwrap_or_else(|error| panic!("fixture event generation: {error}"));
        *realtime.inner.lifecycle.lock() = Lifecycle::Connected(session);
    };

    install_failing_generation(1);
    let owner = realtime.disconnect();
    tokio::pin!(owner);
    assert!(futures_util::poll!(&mut owner).is_pending());
    let waiter = realtime.disconnect();
    tokio::pin!(waiter);
    assert!(futures_util::poll!(&mut waiter).is_pending());

    // Drive retirement without polling either original disconnect future.
    realtime.inner.wait_until_generation_ends(1).await;
    assert_eq!(events.recv().await, Some(RealtimeEvent::Disconnected));
    install_failing_generation(2);
    assert!(matches!(
        realtime.disconnect().await,
        Err(RealtimeError::Close)
    ));
    assert_eq!(events.recv().await, Some(RealtimeEvent::Disconnected));

    // Both kinds of waiter must retain generation 1's failed close result,
    // even though generation 2 has since published its own close result.
    assert!(matches!(owner.await, Err(RealtimeError::Close)));
    assert!(matches!(waiter.await, Err(RealtimeError::Close)));
    assert!(matches!(
        *realtime.inner.lifecycle.lock(),
        Lifecycle::Disconnected
    ));
    assert_eq!(events.queued_event_count(), 0);
}
