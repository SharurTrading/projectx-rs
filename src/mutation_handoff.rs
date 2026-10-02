// SPDX-FileCopyrightText: 2026 Kevin Monaghan
// SPDX-License-Identifier: MIT-0

//! One-use caller custody at HTTP request ownership transfer.

use std::fmt;

use crate::Error;

/// An opaque, consumed callback for one mutation's HTTP handoff.
///
/// The callback must synchronously perform the caller's atomic one-use claim,
/// returning `true` only if that claim wins. A read-only `is_current` check is
/// insufficient: the caller owns the race between claiming and revoking permission.
/// The SDK invokes the original callback once after all SDK preflight succeeds,
/// immediately before transferring the exact built request to reqwest.
///
/// Success transfers ownership to the HTTP transport. Connection/pool readiness,
/// socket writes and provider acceptance are later outcomes; the claim is not proof
/// of any of them. A later revoke cannot cancel or replay this transferred request.
/// `false` produces [`Error::MutationHandoffRefused`] without transferring a request.
///
/// The context cannot be cloned or reused. Passing it to a guarded mutation consumes
/// the context even on a preflight error, but that error does not invoke the callback
/// or claim the caller's permission. The caller retains its own permission state.
/// Captured context is excluded from debug output.
pub struct MutationHandoff<F> {
    claim: F,
}

impl<F: FnOnce() -> bool + Send> MutationHandoff<F> {
    /// Captures the original synchronous atomic claim for one mutation.
    ///
    /// The callback must not block. It owns no SDK transport or provider result,
    /// and a refused claim means only that this request was not handed to reqwest.
    #[must_use]
    pub fn new(claim: F) -> Self {
        Self { claim }
    }

    pub(crate) fn claim(self) -> Result<(), Error> {
        if (self.claim)() {
            Ok(())
        } else {
            Err(Error::MutationHandoffRefused)
        }
    }
}

impl<F> fmt::Debug for MutationHandoff<F> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MutationHandoff")
            .field("claim", &"[REDACTED]")
            .finish()
    }
}
