// SPDX-FileCopyrightText: 2026 Kevin Monaghan
// SPDX-License-Identifier: MIT-0

use super::*;

fn realtime() -> RealtimeClient {
    crate::Client::builder(
        crate::Credentials::new("synthetic-user", "synthetic-key")
            .unwrap_or_else(|error| panic!("fixture credentials: {error}")),
    )
    .build()
    .unwrap_or_else(|error| panic!("fixture client: {error}"))
    .realtime(Hub::Market)
}

#[tokio::test]
async fn receiver_drop_during_gap_reports_closure_and_cannot_relatch_or_reconnect() {
    let realtime = realtime();
    let events = realtime
        .take_event_receiver()
        .unwrap_or_else(|| panic!("receiver"));
    let flow = &realtime.inner.event_flow;
    flow.start_generation(1)
        .unwrap_or_else(|error| panic!("start: {error}"));
    realtime
        .inner
        .publish(
            1,
            RealtimeEvent::Message(serde_json::json!({"sequence": 1})),
        )
        .unwrap_or_else(|error| panic!("accepted prefix: {error}"));
    assert_eq!(events.queued_event_count(), 1);
    flow.mark_gap(1);
    assert!(flow.has_unacknowledged_gap());
    drop(events);
    assert!(!flow.has_unacknowledged_gap());
    assert!(matches!(
        realtime.inner.publish(1, RealtimeEvent::Disconnected),
        Err(RealtimeError::EventReceiverClosed)
    ));
    flow.mark_gap(1);
    assert!(!flow.has_unacknowledged_gap());
    for record in ["{\"type\":6}\u{001e}", "malformed\u{001e}"] {
        realtime
            .inner
            .reconnect_enabled
            .store(true, Ordering::Release);
        assert!(matches!(
            realtime.inner.process_text(1, record).await,
            Err(RealtimeError::EventReceiverClosed)
        ));
        assert!(!realtime.inner.reconnect_enabled.load(Ordering::Acquire));
    }
    flow.finish_generation(1);
    assert!(matches!(
        flow.start_generation(2),
        Err(RealtimeError::EventReceiverClosed)
    ));
    realtime
        .inner
        .reconnect_enabled
        .store(true, Ordering::Release);
    assert!(matches!(
        realtime.connect().await,
        Err(RealtimeError::EventReceiverClosed)
    ));
    assert!(!realtime.inner.reconnect_enabled.load(Ordering::Acquire));
    assert!(realtime.take_event_receiver().is_none());
}

#[test]
fn rejected_install_never_creates_socket_tasks_or_changes_accepted_evidence() {
    let realtime = realtime();
    let events = realtime
        .take_event_receiver()
        .unwrap_or_else(|| panic!("receiver"));
    let claim = realtime
        .inner
        .begin_connect(false)
        .unwrap_or_else(|error| panic!("claim: {error}"));
    claim.cancellation.cancel();
    let created = std::cell::Cell::new(false);
    let result = realtime.inner.install_ready(
        claim.generation,
        || {
            created.set(true);
            panic!("rejected installation must never construct a session");
        },
        RealtimeEvent::Connected,
    );
    assert!(matches!(result, Err(RealtimeError::ConnectionCancelled)));
    assert!(!created.get());
    assert_eq!(events.queued_event_count(), 0);
    assert_eq!(
        realtime.inner.event_flow.state.lock().active_generation,
        None
    );
    assert!(matches!(
        *realtime.inner.lifecycle.lock(),
        Lifecycle::Connecting { .. }
    ));
    drop(claim);
    assert!(matches!(
        *realtime.inner.lifecycle.lock(),
        Lifecycle::Disconnected
    ));
}

#[tokio::test]
async fn borrowed_invocation_arguments_survive_presend_refusal() {
    let realtime = realtime();
    let arguments = [serde_json::json!({"request": 42})];
    assert!(matches!(
        realtime.invoke("Synthetic", &arguments).await,
        Err(RealtimeError::NotConnected)
    ));
    assert_eq!(arguments[0]["request"], 42);
}
