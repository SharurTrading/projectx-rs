// SPDX-FileCopyrightText: 2026 Kevin Monaghan
// SPDX-License-Identifier: MIT-0

//! Unit tests for event-flow publication, loss notifications, and receiver retention.

use std::sync::Arc;

use serde_json::json;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

use super::error::RealtimeError;
use super::event::RealtimeEvent;
use super::event_flow::{
    EventFlow, PublishOutcome, RealtimeEventReceiver, RealtimeGeneration, RealtimeMessage,
};
use super::socket::send_queued;

#[tokio::test]
async fn held_receiver_retains_each_loss_data_and_lifecycle_in_source_order() {
    let flow = EventFlow::new();
    let (events_tx, events_rx) = mpsc::unbounded_channel();
    let mut receiver = RealtimeEventReceiver {
        events: events_rx,
        flow: Arc::clone(&flow),
    };
    flow.start_generation(7)
        .unwrap_or_else(|error| panic!("generation must start: {error}"));
    let expected = [
        RealtimeEvent::Connected,
        RealtimeEvent::Message(json!({"sequence": "A"})),
        RealtimeEvent::TransportGap,
        RealtimeEvent::Message(json!({"sequence": "B"})),
        RealtimeEvent::TransportGap,
        RealtimeEvent::Message(json!({"sequence": "C"})),
        RealtimeEvent::Disconnected,
    ];
    for event in &expected {
        assert_eq!(
            flow.publish(&events_tx, 7, event.clone())
                .unwrap_or_else(|error| panic!("event must publish: {error}")),
            PublishOutcome::Published
        );
    }
    flow.finish_generation(7);
    flow.start_generation(8)
        .unwrap_or_else(|error| panic!("next generation must start: {error}"));
    flow.publish(&events_tx, 8, RealtimeEvent::Reconnected)
        .unwrap_or_else(|error| panic!("replacement must publish: {error}"));
    assert_eq!(receiver.queued_event_count(), 8);
    for (index, event) in expected.into_iter().enumerate() {
        assert_eq!(receiver.queued_event_count(), 8 - index);
        assert_eq!(
            receiver.recv_message().await,
            Some(RealtimeMessage {
                generation: RealtimeGeneration(7),
                event,
            })
        );
    }
    assert_eq!(receiver.recv().await, Some(RealtimeEvent::Reconnected));
    assert_eq!(receiver.queued_event_count(), 0);
    assert_eq!(receiver.oldest_event_age(), None);
    drop(events_tx);
    assert_eq!(receiver.recv().await, None);
}

#[tokio::test]
async fn replacement_stream_preserves_its_identity_when_old_producers_publish() {
    let flow = EventFlow::new();
    let (events_tx, events_rx) = mpsc::unbounded_channel();
    let mut receiver = RealtimeEventReceiver {
        events: events_rx,
        flow: Arc::clone(&flow),
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
    assert_eq!(
        flow.publish(&events_tx, 1, RealtimeEvent::TransportGap)
            .unwrap_or_else(|error| panic!("stale loss must be ignored: {error}")),
        PublishOutcome::StaleGeneration
    );
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
}

#[test]
fn dropping_event_receiver_refuses_new_data() {
    let flow = EventFlow::new();
    let (events_tx, events_rx) = mpsc::unbounded_channel();
    let receiver = RealtimeEventReceiver {
        events: events_rx,
        flow: Arc::clone(&flow),
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
