// SPDX-FileCopyrightText: 2026 Kevin Monaghan
// SPDX-License-Identifier: MIT

//! Internal bearer-token storage.

use std::{fmt, sync::Arc};

use parking_lot::Mutex;
use secrecy::{ExposeSecret, SecretString};

/// An owned token plus the revision it was read from.
///
/// Callers may hold this snapshot across network I/O. A rotated token is only
/// committed when the revision still matches, so an older response cannot
/// replace a newer session.
pub(crate) struct TokenSnapshot {
    token: SecretString,
    revision: u128,
}

/// Opaque identity for one installed bearer-token revision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TokenRevision(u128);

impl TokenSnapshot {
    pub(crate) fn expose(&self) -> &str {
        self.token.expose_secret()
    }

    pub(crate) const fn revision(&self) -> TokenRevision {
        TokenRevision(self.revision)
    }
}

impl fmt::Debug for TokenSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TokenSnapshot")
            .field("token", &"[REDACTED]")
            .field("revision", &self.revision)
            .finish()
    }
}

#[derive(Debug, Default)]
struct TokenState {
    token: Option<SecretString>,
    revision: u128,
    authentications_in_flight: u128,
    pending_update: Option<PendingUpdate>,
}

#[derive(Debug)]
struct PendingRotation {
    basis_revision: u128,
    token: SecretString,
}

#[derive(Debug)]
enum PendingUpdate {
    Invalidate { basis_revision: u128 },
    Rotate(PendingRotation),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum UpdateOutcome {
    Applied,
    Deferred,
    Stale,
}

/// Cancellation-safe ownership of one authentication attempt.
///
/// Full authentication has precedence over token validation. While any
/// attempt is in flight, validation responses cannot rotate the current token.
pub(crate) struct AuthenticationAttempt {
    store: Arc<TokenStore>,
    basis_revision: u128,
    active: bool,
}

impl AuthenticationAttempt {
    /// Commits the token if no other authentication completed first.
    ///
    /// Concurrent successful authentications use first-completion-wins
    /// semantics. Every caller still observes success, but a delayed response
    /// can never replace a token already installed by another attempt.
    pub(crate) fn commit(mut self, token: String) -> bool {
        let mut state = self.store.state.lock();
        let committed = state.revision == self.basis_revision;
        if committed {
            state.token = Some(SecretString::from(token));
            state.revision = state.revision.wrapping_add(1);
            state.pending_update = None;
        }
        state.authentications_in_flight = state.authentications_in_flight.saturating_sub(1);
        apply_pending_update(&mut state);
        self.active = false;
        committed
    }
}

impl fmt::Debug for AuthenticationAttempt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthenticationAttempt")
            .field("basis_revision", &self.basis_revision)
            .field("active", &self.active)
            .finish_non_exhaustive()
    }
}

impl Drop for AuthenticationAttempt {
    fn drop(&mut self) {
        if self.active {
            let mut state = self.store.state.lock();
            state.authentications_in_flight = state.authentications_in_flight.saturating_sub(1);
            apply_pending_update(&mut state);
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct TokenStore {
    state: Mutex<TokenState>,
}

impl TokenStore {
    /// Registers a full authentication attempt before its network request.
    pub(crate) fn begin_authentication(self: &Arc<Self>) -> AuthenticationAttempt {
        let basis_revision = {
            let mut state = self.state.lock();
            state.authentications_in_flight = state.authentications_in_flight.saturating_add(1);
            state.revision
        };
        AuthenticationAttempt {
            store: Arc::clone(self),
            basis_revision,
            active: true,
        }
    }

    /// Returns an owned secret snapshot for a network request.
    pub(crate) fn versioned_snapshot(&self) -> Option<TokenSnapshot> {
        let state = self.state.lock();
        if has_pending_update(&state) {
            return None;
        }
        state.token.as_ref().map(|token| TokenSnapshot {
            token: SecretString::from(token.expose_secret().to_owned()),
            revision: state.revision,
        })
    }

    /// Returns an owned token for real-time connection setup.
    pub(crate) fn snapshot(&self) -> Option<String> {
        let state = self.state.lock();
        if has_pending_update(&state) {
            return None;
        }
        state
            .token
            .as_ref()
            .map(|token| token.expose_secret().to_owned())
    }

    /// Applies validation rotation only to the session that was validated.
    pub(crate) fn rotate_if_current(&self, basis: &TokenSnapshot, token: String) -> UpdateOutcome {
        let mut state = self.state.lock();
        if state.revision != basis.revision {
            return UpdateOutcome::Stale;
        }
        if state.authentications_in_flight != 0 {
            if state.pending_update.is_none() {
                state.pending_update = Some(PendingUpdate::Rotate(PendingRotation {
                    basis_revision: basis.revision,
                    token: SecretString::from(token),
                }));
                return UpdateOutcome::Deferred;
            }
            return UpdateOutcome::Stale;
        }
        state.token = Some(SecretString::from(token));
        state.revision = state.revision.wrapping_add(1);
        UpdateOutcome::Applied
    }

    /// Invalidates only the session represented by `basis`.
    ///
    /// A concurrent full authentication takes precedence. If all such attempts
    /// fail or are cancelled, the invalidation is applied when the last attempt
    /// releases its guard.
    pub(crate) fn invalidate_if_current(&self, basis: &TokenSnapshot) -> UpdateOutcome {
        self.invalidate_revision_if_current(basis.revision())
    }

    /// Invalidates only the session identified by an opaque revision.
    pub(crate) fn invalidate_revision_if_current(&self, basis: TokenRevision) -> UpdateOutcome {
        let mut state = self.state.lock();
        if state.revision != basis.0 {
            return UpdateOutcome::Stale;
        }
        if state.authentications_in_flight == 0 {
            state.token = None;
            state.revision = state.revision.wrapping_add(1);
            state.pending_update = None;
            UpdateOutcome::Applied
        } else if state.pending_update.is_none() {
            state.pending_update = Some(PendingUpdate::Invalidate {
                basis_revision: basis.0,
            });
            UpdateOutcome::Deferred
        } else {
            UpdateOutcome::Stale
        }
    }

    pub(crate) fn is_authenticated(&self) -> bool {
        let state = self.state.lock();
        state.token.is_some() && !has_pending_update(&state)
    }

    pub(crate) fn has_viable_session_or_authentication(&self) -> bool {
        let state = self.state.lock();
        state.authentications_in_flight != 0
            || (state.token.is_some() && !has_pending_update(&state))
    }
}

fn has_pending_update(state: &TokenState) -> bool {
    state.pending_update.as_ref().is_some_and(|update| {
        let basis_revision = match update {
            PendingUpdate::Invalidate { basis_revision } => *basis_revision,
            PendingUpdate::Rotate(rotation) => rotation.basis_revision,
        };
        basis_revision == state.revision
    })
}

fn apply_pending_update(state: &mut TokenState) {
    if state.authentications_in_flight != 0 {
        return;
    }
    let Some(update) = state.pending_update.take() else {
        return;
    };
    match update {
        PendingUpdate::Invalidate { basis_revision } if basis_revision == state.revision => {
            state.token = None;
            state.revision = state.revision.wrapping_add(1);
        }
        PendingUpdate::Rotate(rotation) if rotation.basis_revision == state.revision => {
            state.token = Some(rotation.token);
            state.revision = state.revision.wrapping_add(1);
        }
        PendingUpdate::Invalidate { .. } | PendingUpdate::Rotate(_) => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delayed_authentication_cannot_replace_first_completed_authentication() {
        let store = Arc::new(TokenStore::default());
        let delayed = store.begin_authentication();
        let completed_first = store.begin_authentication();

        assert!(completed_first.commit("newer-token".to_owned()));
        assert!(!delayed.commit("stale-token".to_owned()));
        assert_eq!(store.snapshot().as_deref(), Some("newer-token"));
    }

    #[test]
    fn validation_cannot_replace_a_new_authentication() {
        let store = Arc::new(TokenStore::default());
        assert!(
            store
                .begin_authentication()
                .commit("initial-token".to_owned())
        );
        let validation = store
            .versioned_snapshot()
            .unwrap_or_else(|| panic!("fixture token must exist"));
        assert!(
            store
                .begin_authentication()
                .commit("reauthenticated-token".to_owned())
        );

        assert_eq!(
            store.rotate_if_current(&validation, "stale-rotation".to_owned()),
            UpdateOutcome::Stale
        );
        assert_eq!(store.snapshot().as_deref(), Some("reauthenticated-token"));
    }

    #[test]
    fn first_validation_completion_wins_for_one_revision() {
        let store = Arc::new(TokenStore::default());
        assert!(
            store
                .begin_authentication()
                .commit("initial-token".to_owned())
        );
        let delayed = store
            .versioned_snapshot()
            .unwrap_or_else(|| panic!("fixture token must exist"));
        let completed_first = store
            .versioned_snapshot()
            .unwrap_or_else(|| panic!("fixture token must exist"));

        assert_eq!(
            store.rotate_if_current(&completed_first, "newer-token".to_owned()),
            UpdateOutcome::Applied
        );
        assert_eq!(
            store.rotate_if_current(&delayed, "stale-token".to_owned()),
            UpdateOutcome::Stale
        );
        assert_eq!(store.snapshot().as_deref(), Some("newer-token"));
    }

    #[test]
    fn cancelled_authentication_releases_validation_fence() {
        let store = Arc::new(TokenStore::default());
        assert!(
            store
                .begin_authentication()
                .commit("initial-token".to_owned())
        );
        let validation = store
            .versioned_snapshot()
            .unwrap_or_else(|| panic!("fixture token must exist"));
        let cancelled = store.begin_authentication();
        drop(cancelled);

        assert_eq!(
            store.rotate_if_current(&validation, "rotated-token".to_owned()),
            UpdateOutcome::Applied
        );
        assert_eq!(store.snapshot().as_deref(), Some("rotated-token"));
    }

    #[test]
    fn current_session_is_invalidated_immediately_without_authentication_race() {
        let store = Arc::new(TokenStore::default());
        assert!(
            store
                .begin_authentication()
                .commit("initial-token".to_owned())
        );
        let rejected = store
            .versioned_snapshot()
            .unwrap_or_else(|| panic!("fixture token must exist"));

        assert_eq!(
            store.invalidate_if_current(&rejected),
            UpdateOutcome::Applied
        );
        assert!(store.snapshot().is_none());
        assert_eq!(store.invalidate_if_current(&rejected), UpdateOutcome::Stale);
    }

    #[test]
    fn stale_invalidation_cannot_clear_a_new_authentication() {
        let store = Arc::new(TokenStore::default());
        assert!(
            store
                .begin_authentication()
                .commit("initial-token".to_owned())
        );
        let rejected = store
            .versioned_snapshot()
            .unwrap_or_else(|| panic!("fixture token must exist"));
        assert!(
            store
                .begin_authentication()
                .commit("reauthenticated-token".to_owned())
        );

        assert_eq!(store.invalidate_if_current(&rejected), UpdateOutcome::Stale);
        assert_eq!(store.snapshot().as_deref(), Some("reauthenticated-token"));
    }

    #[test]
    fn successful_authentication_supersedes_pending_invalidation() {
        let store = Arc::new(TokenStore::default());
        assert!(
            store
                .begin_authentication()
                .commit("initial-token".to_owned())
        );
        let rejected = store
            .versioned_snapshot()
            .unwrap_or_else(|| panic!("fixture token must exist"));
        let reauthentication = store.begin_authentication();

        assert_eq!(
            store.invalidate_if_current(&rejected),
            UpdateOutcome::Deferred
        );
        assert!(store.snapshot().is_none());
        assert!(!store.is_authenticated());
        assert!(reauthentication.commit("reauthenticated-token".to_owned()));
        assert_eq!(store.snapshot().as_deref(), Some("reauthenticated-token"));
    }

    #[test]
    fn pending_invalidation_applies_after_cancelled_authentication() {
        let store = Arc::new(TokenStore::default());
        assert!(
            store
                .begin_authentication()
                .commit("initial-token".to_owned())
        );
        let rejected = store
            .versioned_snapshot()
            .unwrap_or_else(|| panic!("fixture token must exist"));
        let cancelled = store.begin_authentication();

        assert_eq!(
            store.invalidate_if_current(&rejected),
            UpdateOutcome::Deferred
        );
        assert!(store.snapshot().is_none());
        assert!(!store.is_authenticated());
        drop(cancelled);
        assert!(store.snapshot().is_none());
    }

    #[test]
    fn deferred_rotation_applies_after_cancelled_authentication() {
        let store = Arc::new(TokenStore::default());
        assert!(
            store
                .begin_authentication()
                .commit("initial-token".to_owned())
        );
        let validation = store
            .versioned_snapshot()
            .unwrap_or_else(|| panic!("fixture token must exist"));
        let cancelled = store.begin_authentication();

        assert_eq!(
            store.rotate_if_current(&validation, "rotated-token".to_owned()),
            UpdateOutcome::Deferred
        );
        assert!(store.snapshot().is_none());
        assert!(!store.is_authenticated());
        assert!(store.has_viable_session_or_authentication());
        drop(cancelled);

        assert_eq!(store.snapshot().as_deref(), Some("rotated-token"));
        assert!(store.is_authenticated());
    }

    #[test]
    fn first_deferred_rotation_cannot_be_overwritten_by_a_later_invalidation() {
        let store = Arc::new(TokenStore::default());
        assert!(
            store
                .begin_authentication()
                .commit("initial-token".to_owned())
        );
        let rotation = store
            .versioned_snapshot()
            .unwrap_or_else(|| panic!("fixture token must exist"));
        let rejection = store
            .versioned_snapshot()
            .unwrap_or_else(|| panic!("fixture token must exist"));
        let cancelled = store.begin_authentication();

        assert_eq!(
            store.rotate_if_current(&rotation, "rotated-token".to_owned()),
            UpdateOutcome::Deferred
        );
        assert_eq!(
            store.invalidate_if_current(&rejection),
            UpdateOutcome::Stale
        );
        drop(cancelled);

        assert_eq!(store.snapshot().as_deref(), Some("rotated-token"));
    }

    #[test]
    fn first_deferred_invalidation_cannot_be_overwritten_by_a_later_rotation() {
        let store = Arc::new(TokenStore::default());
        assert!(
            store
                .begin_authentication()
                .commit("initial-token".to_owned())
        );
        let rejection = store
            .versioned_snapshot()
            .unwrap_or_else(|| panic!("fixture token must exist"));
        let rotation = store
            .versioned_snapshot()
            .unwrap_or_else(|| panic!("fixture token must exist"));
        let cancelled = store.begin_authentication();

        assert_eq!(
            store.invalidate_if_current(&rejection),
            UpdateOutcome::Deferred
        );
        assert_eq!(
            store.rotate_if_current(&rotation, "rotated-token".to_owned()),
            UpdateOutcome::Stale
        );
        drop(cancelled);

        assert!(store.snapshot().is_none());
    }

    #[test]
    fn successful_authentication_supersedes_deferred_rotation() {
        let store = Arc::new(TokenStore::default());
        assert!(
            store
                .begin_authentication()
                .commit("initial-token".to_owned())
        );
        let validation = store
            .versioned_snapshot()
            .unwrap_or_else(|| panic!("fixture token must exist"));
        let reauthentication = store.begin_authentication();

        assert_eq!(
            store.rotate_if_current(&validation, "rotated-token".to_owned()),
            UpdateOutcome::Deferred
        );
        assert!(reauthentication.commit("reauthenticated-token".to_owned()));

        assert_eq!(store.snapshot().as_deref(), Some("reauthenticated-token"));
    }

    #[test]
    fn debug_output_redacts_stored_and_snapshotted_tokens() {
        let store = Arc::new(TokenStore::default());
        assert!(
            store
                .begin_authentication()
                .commit("synthetic-secret-token".to_owned())
        );
        let snapshot = store
            .versioned_snapshot()
            .unwrap_or_else(|| panic!("fixture token must exist"));

        assert!(!format!("{store:?}").contains("synthetic-secret-token"));
        assert!(!format!("{snapshot:?}").contains("synthetic-secret-token"));

        let authentication = store.begin_authentication();
        assert_eq!(
            store.rotate_if_current(&snapshot, "synthetic-deferred-secret".to_owned()),
            UpdateOutcome::Deferred
        );
        assert!(!format!("{store:?}").contains("synthetic-deferred-secret"));
        drop(authentication);
    }
}
