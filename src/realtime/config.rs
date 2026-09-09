// SPDX-FileCopyrightText: 2026 Kevin Monaghan
// SPDX-License-Identifier: MIT-0

//! Caller-configurable control and signalling queues, never subscription quotas.

use std::time::Duration;

#[derive(Clone, Copy, Debug)]
pub(crate) struct RealtimeConfig {
    pub(crate) event_capacity: usize,
    pub(crate) writer_capacity: usize,
    pub(crate) pending_capacity: usize,
    pub(crate) invocation_timeout: Duration,
}

impl Default for RealtimeConfig {
    fn default() -> Self {
        Self {
            // A paused consumer can absorb several 20,000-event fixture bursts.
            // Overflow still signals a stalled consumer; callers can tune this.
            event_capacity: 65_536,
            // Headroom beyond the 1,024 simultaneous-subscription fixture. These
            // bound outstanding control work, not the number of live subscriptions.
            writer_capacity: 4_096,
            pending_capacity: 4_096,
            invocation_timeout: Duration::from_secs(15),
        }
    }
}

impl RealtimeConfig {
    pub(crate) fn validate(self) -> Result<(), crate::Error> {
        if [
            self.event_capacity,
            self.writer_capacity,
            self.pending_capacity,
        ]
        .into_iter()
        .any(|capacity| capacity == 0 || capacity > tokio::sync::Semaphore::MAX_PERMITS)
            || self.invocation_timeout.is_zero()
            || tokio::time::Instant::now()
                .checked_add(self.invocation_timeout)
                .is_none()
        {
            return Err(crate::Error::Configuration(
                "real-time capacities must fit Tokio's non-zero permit range and the invocation deadline must fit its clock".to_owned(),
            ));
        }
        Ok(())
    }
}
