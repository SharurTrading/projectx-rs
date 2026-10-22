// SPDX-FileCopyrightText: 2026 Kevin Monaghan
// SPDX-License-Identifier: MIT-0

//! Unit tests for event-flow publication, gap fencing, and receiver retention.

use std::sync::Arc;

use serde_json::json;
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::tungstenite::Message;

use super::error::RealtimeError;
use super::event::RealtimeEvent;
use super::event_flow::{
    EventFlow, PublishOutcome, RealtimeEventReceiver, RealtimeGeneration, RealtimeMessage,
};
use super::socket::send_queued;

#[tokio::test]
async fn gap_follows_accepted_events_without_waiting_for_generation_end() {
    let flow = EventFlow::new();
    let (events_tx, events_rx) = mpsc::unbounded_channel();
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
    )
    .unwrap_or_else(|error| panic!("first event must enter the queue: {error}"));

    let (terminal_staged_tx, terminal_staged_rx) = oneshot::channel();
    let (finish_tx, finish_rx) = oneshot::channel();
    let producer_flow = Arc::clone(&flow);
    let producer = tokio::spawn(async move {
        producer_flow.mark_gap(generation);
        producer_flow
            .publish(&events_tx, generation, RealtimeEvent::Disconnected)
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
        .unwrap_or_else(|error| panic!("acknowledgement must allow the next generation: {error}"));
    assert_eq!(receiver.recv().await, None);
}

#[tokio::test]
async fn stale_generation_cannot_publish_or_latch_a_gap_after_replacement() {
    let flow = EventFlow::new();
    let (events_tx, events_rx) = mpsc::unbounded_channel();
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
        flow.publish(&events_tx, 1, RealtimeEvent::Disconnected,)
            .unwrap_or_else(|error| panic!("disconnect must publish: {error}")),
        PublishOutcome::Published
    );
    flow.finish_generation(1);
    flow.start_generation(2)
        .unwrap_or_else(|error| panic!("replacement generation must start: {error}"));
    assert_eq!(
        flow.publish(&events_tx, 2, RealtimeEvent::Reconnected)
            .unwrap_or_else(|error| panic!("reconnect must publish: {error}")),
        PublishOutcome::Published
    );

    assert_eq!(
        flow.publish(
            &events_tx,
            1,
            RealtimeEvent::Message(json!({"generation": 1}))
        )
        .unwrap_or_else(|error| panic!("stale publication must be ignored: {error}")),
        PublishOutcome::StaleGeneration
    );
    assert!(!flow.has_unacknowledged_gap());
    assert_eq!(
        flow.publish(
            &events_tx,
            2,
            RealtimeEvent::Message(json!({"generation": 2})),
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
async fn stalled_receiver_retains_more_than_the_former_event_limit_across_generations() {
    const BURST: usize = 65_537;
    let flow = EventFlow::new();
    let (events_tx, events_rx) = mpsc::unbounded_channel();
    let mut receiver = RealtimeEventReceiver {
        events: events_rx,
        flow: Arc::clone(&flow),
        gap_reported: false,
    };
    flow.start_generation(1)
        .unwrap_or_else(|error| panic!("first generation must start: {error}"));
    for sequence in 0..BURST {
        let kind = if sequence % 2 == 0 { "market" } else { "order" };
        assert!(matches!(
            flow.publish(
                &events_tx,
                1,
                RealtimeEvent::Message(json!({"kind": kind, "sequence": sequence})),
            ),
            Ok(PublishOutcome::Published)
        ));
    }
    assert_eq!(receiver.queued_event_count(), BURST);
    assert!(receiver.oldest_event_age().is_some());
    assert!(!flow.has_unacknowledged_gap());

    assert!(matches!(
        flow.publish(&events_tx, 1, RealtimeEvent::Disconnected),
        Ok(PublishOutcome::Published)
    ));
    flow.finish_generation(1);
    flow.start_generation(2)
        .unwrap_or_else(|error| panic!("replacement generation must start: {error}"));
    assert!(matches!(
        flow.publish(&events_tx, 2, RealtimeEvent::Reconnected),
        Ok(PublishOutcome::Published)
    ));
    flow.finish_generation(2);
    drop(events_tx);

    for sequence in 0..BURST {
        let message = receiver
            .recv_message()
            .await
            .unwrap_or_else(|| panic!("event {sequence} must survive"));
        assert_eq!(message.generation, RealtimeGeneration(1));
        let RealtimeEvent::Message(value) = message.event else {
            panic!("data event {sequence} must survive");
        };
        assert_eq!(value["sequence"], sequence);
        assert_eq!(
            value["kind"],
            if sequence % 2 == 0 { "market" } else { "order" }
        );
    }
    assert_eq!(
        receiver.recv_message().await,
        Some(RealtimeMessage {
            generation: RealtimeGeneration(1),
            event: RealtimeEvent::Disconnected,
        })
    );
    assert_eq!(
        receiver.recv_message().await,
        Some(RealtimeMessage {
            generation: RealtimeGeneration(2),
            event: RealtimeEvent::Reconnected,
        })
    );
    assert_eq!(receiver.queued_event_count(), 0);
    assert_eq!(receiver.oldest_event_age(), None);
    assert_eq!(receiver.recv_message().await, None);
}

#[tokio::test]
async fn outbound_saturation_does_not_discard_inbound_events() {
    let (writer, _writer_rx) = mpsc::channel(1);
    assert!(send_queued(&writer, Message::Ping(Vec::new().into())).is_ok());
    assert!(matches!(
        send_queued(&writer, Message::Ping(Vec::new().into())),
        Err(RealtimeError::SendQueueFull)
    ));

    let flow = EventFlow::new();
    let (events_tx, events_rx) = mpsc::unbounded_channel();
    let mut receiver = RealtimeEventReceiver {
        events: events_rx,
        flow: Arc::clone(&flow),
        gap_reported: false,
    };
    flow.start_generation(1)
        .unwrap_or_else(|error| panic!("generation must start: {error}"));
    assert!(matches!(
        flow.publish(
            &events_tx,
            1,
            RealtimeEvent::Message(json!({"order": "terminal"})),
        ),
        Ok(PublishOutcome::Published)
    ));
    assert_eq!(
        receiver.recv().await,
        Some(RealtimeEvent::Message(json!({"order": "terminal"})))
    );
    assert!(!flow.has_unacknowledged_gap());
}

#[test]
fn dropping_event_receiver_refuses_new_data() {
    let flow = EventFlow::new();
    let (events_tx, events_rx) = mpsc::unbounded_channel();
    let receiver = RealtimeEventReceiver {
        events: events_rx,
        flow: Arc::clone(&flow),
        gap_reported: false,
    };
    flow.start_generation(1)
        .unwrap_or_else(|error| panic!("generation must start: {error}"));
    assert!(matches!(
        flow.publish(&events_tx, 1, RealtimeEvent::Connected),
        Ok(PublishOutcome::Published)
    ));
    assert_eq!(receiver.queued_event_count(), 1);
    drop(receiver);
    assert!(matches!(
        flow.publish(&events_tx, 1, RealtimeEvent::Disconnected),
        Err(RealtimeError::EventReceiverClosed)
    ));
}
