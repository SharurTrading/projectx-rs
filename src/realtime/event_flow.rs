// SPDX-FileCopyrightText: 2026 Kevin Monaghan
// SPDX-License-Identifier: MIT-0

//! Lossless data delivery with retained continuity and lifecycle boundaries.

use std::{collections::VecDeque, time::Duration};

use super::{
    Arc, AtomicBool, AtomicUsize, Instant, Notify, Ordering, ParkingMutex, RealtimeError,
    RealtimeEvent, fmt, mpsc,
};

/// Identity of one ready socket, scoped to its owning real-time client.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RealtimeGeneration(pub(super) u64);

/// An observation attributed to the exact socket that produced it.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct RealtimeMessage {
    /// The originating socket's identity.
    pub generation: RealtimeGeneration,
    /// The observation or lifecycle boundary.
    pub event: RealtimeEvent,
}

pub(super) struct EventFlow {
    pub(super) state: ParkingMutex<EventFlowState>,
    pub(super) queued_weight: AtomicUsize,
    gap_pending: AtomicBool,
    changed: Notify,
}

#[derive(Default)]
pub(super) struct EventFlowState {
    pub(super) active_generation: Option<u64>,
    queued_at: VecDeque<Instant>,
    gap: Option<GapFence>,
}

// Two retained lifecycle slots suffice: a generation with a real transport gap
// may start and end, but another generation cannot start until it is consumed.
struct GapFence {
    generation: u64,
    start_event: Option<RealtimeEvent>,
    terminal_event: Option<RealtimeEvent>,
    generation_ended: bool,
    acknowledged: bool,
}

struct EventReservation {
    flow: Arc<EventFlow>,
    weight: usize,
}

impl Drop for EventReservation {
    fn drop(&mut self) {
        let previous = self
            .flow
            .queued_weight
            .fetch_sub(self.weight, Ordering::AcqRel);
        debug_assert!(previous >= self.weight);
    }
}

pub(super) struct EventEnvelope {
    message: RealtimeMessage,
    reservation: EventReservation,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum PublishOutcome {
    Published,
    GapFenced,
    StaleGeneration,
}

impl EventEnvelope {
    fn into_message(self) -> RealtimeMessage {
        let Self {
            message,
            reservation,
        } = self;
        drop(reservation);
        message
    }
}

impl EventFlow {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            state: ParkingMutex::new(EventFlowState::default()),
            queued_weight: AtomicUsize::new(0),
            gap_pending: AtomicBool::new(false),
            changed: Notify::new(),
        })
    }

    pub(super) fn has_unacknowledged_gap(&self) -> bool {
        self.gap_pending.load(Ordering::Acquire)
    }

    pub(super) fn start_generation(&self, generation: u64) -> Result<(), RealtimeError> {
        let mut state = self.state.lock();
        if state.gap.is_some() {
            return Err(RealtimeError::TransportGapPending);
        }
        if state.active_generation.is_some() {
            return Err(RealtimeError::ConnectionCancelled);
        }
        state.active_generation = Some(generation);
        Ok(())
    }

    pub(super) fn publish(
        self: &Arc<Self>,
        events: &mpsc::UnboundedSender<EventEnvelope>,
        generation: u64,
        event: RealtimeEvent,
        weight: usize,
    ) -> Result<PublishOutcome, RealtimeError> {
        let mut state = self.state.lock();
        if state.active_generation != Some(generation) {
            return Ok(PublishOutcome::StaleGeneration);
        }
        if state.gap.is_some() {
            return Ok(self.retain_gap_boundary(&mut state, generation, event));
        }
        self.queued_weight.fetch_add(weight, Ordering::AcqRel);
        let envelope = EventEnvelope {
            message: RealtimeMessage {
                generation: RealtimeGeneration(generation),
                event,
            },
            reservation: EventReservation {
                flow: Arc::clone(self),
                weight,
            },
        };
        match events.send(envelope) {
            Ok(()) => {
                state.queued_at.push_back(Instant::now());
                Ok(PublishOutcome::Published)
            }
            Err(_) => Err(RealtimeError::EventReceiverClosed),
        }
    }

    fn retain_gap_boundary(
        &self,
        state: &mut EventFlowState,
        generation: u64,
        event: RealtimeEvent,
    ) -> PublishOutcome {
        let fence = state.gap.get_or_insert(GapFence {
            generation,
            start_event: None,
            terminal_event: None,
            generation_ended: false,
            acknowledged: false,
        });
        let retained = match event {
            RealtimeEvent::Connected | RealtimeEvent::Reconnected => {
                fence.start_event = Some(event);
                true
            }
            RealtimeEvent::Disconnected => {
                fence.terminal_event = Some(event);
                true
            }
            _ => false,
        };
        self.gap_pending.store(true, Ordering::Release);
        self.changed.notify_waiters();
        if retained {
            PublishOutcome::Published
        } else {
            PublishOutcome::GapFenced
        }
    }

    pub(super) fn mark_gap(&self, generation: u64) {
        let mut state = self.state.lock();
        if state.active_generation == Some(generation) {
            let _ = self.retain_gap_boundary(&mut state, generation, RealtimeEvent::TransportGap);
        }
    }

    pub(super) fn finish_generation(&self, generation: u64) {
        let mut state = self.state.lock();
        if state.active_generation != Some(generation) {
            return;
        }
        state.active_generation = None;
        if let Some(fence) = state.gap.as_mut() {
            fence.generation_ended = true;
        }
        drop(state);
        self.changed.notify_waiters();
    }

    fn release_acknowledged(&self, state: &mut EventFlowState) {
        if state.gap.as_ref().is_some_and(|fence| {
            fence.acknowledged && fence.start_event.is_none() && fence.terminal_event.is_none()
        }) {
            state.gap = None;
            self.gap_pending.store(false, Ordering::Release);
        }
    }

    fn acknowledge_gap(&self) {
        let mut state = self.state.lock();
        if let Some(fence) = state.gap.as_mut() {
            fence.acknowledged = true;
        }
        self.release_acknowledged(&mut state);
        drop(state);
        self.changed.notify_waiters();
    }
}

/// Single-consumer receiver for real-time events.
pub struct RealtimeEventReceiver {
    pub(super) events: mpsc::UnboundedReceiver<EventEnvelope>,
    pub(super) flow: Arc<EventFlow>,
    pub(super) gap_reported: bool,
}

impl RealtimeEventReceiver {
    /// Number of accepted events waiting for this receiver.
    #[must_use]
    pub fn queued_event_count(&self) -> usize {
        self.flow.state.lock().queued_at.len()
    }

    /// Age of the oldest accepted event waiting for this receiver.
    #[must_use]
    pub fn oldest_event_age(&self) -> Option<Duration> {
        self.flow
            .state
            .lock()
            .queued_at
            .front()
            .map(Instant::elapsed)
    }

    /// Returns whether every producer for this event stream is gone.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.events.is_closed()
    }

    /// Receives an event without its socket identity.
    /// Use [`Self::recv_message`] when work may overlap transport replacement.
    pub async fn recv(&mut self) -> Option<RealtimeEvent> {
        self.recv_message().await.map(|message| message.event)
    }

    /// Receives the accepted prefix, then retained gap and lifecycle boundaries.
    /// A disconnected boundary proves both socket tasks have stopped.
    pub async fn recv_message(&mut self) -> Option<RealtimeMessage> {
        loop {
            let changed = self.flow.changed.notified();
            tokio::pin!(changed);
            let _ = changed.as_mut().enable();
            {
                let mut state = self.flow.state.lock();
                if let Ok(envelope) = self.events.try_recv() {
                    state.queued_at.pop_front();
                    return Some(envelope.into_message());
                }
                if let Some(fence) = state.gap.as_mut() {
                    let event = if let Some(event) = fence.start_event.take() {
                        Some(event)
                    } else if !fence.acknowledged && !self.gap_reported {
                        self.gap_reported = true;
                        Some(RealtimeEvent::TransportGap)
                    } else if fence.generation_ended {
                        fence.terminal_event.take()
                    } else {
                        None
                    };
                    if let Some(event) = event {
                        let message = RealtimeMessage {
                            generation: RealtimeGeneration(fence.generation),
                            event,
                        };
                        self.flow.release_acknowledged(&mut state);
                        return Some(message);
                    }
                }
                if self.events.is_closed() {
                    return None;
                }
            }
            tokio::select! {
                biased;
                envelope = self.events.recv() => {
                    if let Some(envelope) = envelope {
                        self.flow.state.lock().queued_at.pop_front();
                        return Some(envelope.into_message());
                    }
                }
                () = &mut changed => {}
            }
        }
    }

    /// Resumes data admission after the caller installs its recovery boundary.
    /// This never closes a socket. Completions and keepalives continue while fenced.
    pub fn acknowledge_transport_gap(&mut self) {
        if self.gap_reported {
            self.flow.acknowledge_gap();
            self.gap_reported = false;
        }
    }
}

impl Drop for RealtimeEventReceiver {
    fn drop(&mut self) {
        let mut state = self.flow.state.lock();
        self.events.close();
        state.queued_at.clear();
    }
}

impl fmt::Debug for RealtimeEventReceiver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RealtimeEventReceiver")
            .field("gap_pending", &self.flow.has_unacknowledged_gap())
            .field("queued_events", &self.queued_event_count())
            .field("oldest_event_age", &self.oldest_event_age())
            .field(
                "queued_weight",
                &self.flow.queued_weight.load(Ordering::Acquire),
            )
            .field("gap_reported", &self.gap_reported)
            .finish_non_exhaustive()
    }
}
