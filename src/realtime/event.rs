// SPDX-FileCopyrightText: 2026 Kevin Monaghan
// SPDX-License-Identifier: MIT-0

//! Real-time hub selector and the events a client emits.

use serde_json::Value;

use super::invocation::SignalRInvocation;

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
    pub(super) const fn path(self) -> &'static str {
        match self {
            Self::Market => "market",
            Self::User => "user",
        }
    }
}

/// Events emitted by a [`crate::RealtimeClient`].
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
    /// A malformed provider record or lost transport continuity created a gap.
    ///
    /// Callers must fence recovery and then call
    /// [`crate::RealtimeEventReceiver::acknowledge_transport_gap`].
    TransportGap,
    /// A provider type-1 invocation with an exact raw entity retained for typed decoding.
    Invocation(SignalRInvocation),
    /// A decoded `SignalR` JSON message.
    ///
    /// Type-1 invocations are emitted through [`Self::Invocation`]; this
    /// variant carries other application-visible message families.
    Message(Value),
}
