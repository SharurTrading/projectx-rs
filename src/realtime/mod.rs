// SPDX-FileCopyrightText: 2026 Kevin Monaghan
// SPDX-License-Identifier: MIT-0

//! `ProjectX` `SignalR`-over-WebSocket transport.

mod client;
mod config;
mod error;
mod event;
mod event_flow;
mod invocation;
mod lifecycle;
mod session_handle;
mod socket;

#[cfg(test)]
mod admission_tests;
#[cfg(test)]
mod event_flow_tests;
#[cfg(test)]
mod lifecycle_tests;
#[cfg(test)]
mod protocol_tests;
#[cfg(test)]
mod teardown_tests;

pub use client::RealtimeClient;
pub(crate) use config::RealtimeConfig;
pub use error::RealtimeError;
pub use event::{Hub, RealtimeEvent};
pub use event_flow::{RealtimeEventReceiver, RealtimeGeneration, RealtimeMessage};
pub use invocation::SignalRInvocation;
pub use session_handle::RealtimeSession;
