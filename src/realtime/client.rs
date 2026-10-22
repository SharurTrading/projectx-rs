// SPDX-FileCopyrightText: 2026 Kevin Monaghan
// SPDX-License-Identifier: MIT-0

//! Public real-time client handle over the shared lifecycle core.

use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
};

use parking_lot::Mutex as ParkingMutex;
use serde_json::Value;
use tokio::sync::{Notify, mpsc};
use tokio_util::sync::CancellationToken;

use crate::{AccountId, ContractId, Endpoints, token::TokenStore};

use super::config::RealtimeConfig;
use super::error::RealtimeError;
use super::event::Hub;
use super::event_flow::{EventFlow, RealtimeEventReceiver, RealtimeGeneration};
use super::lifecycle::{Lifecycle, RealtimeInner};
use super::session_handle::RealtimeSession;

/// `SignalR`-over-WebSocket client for one `ProjectX` hub.
///
/// The transport owns no subscription truth. Reconnects emit
/// [`crate::RealtimeEvent::Reconnected`], after which the caller replays its current
/// subscription set.
pub struct RealtimeClient {
    pub(super) inner: Arc<RealtimeInner>,
}

impl RealtimeClient {
    pub(crate) fn new(
        hub: Hub,
        endpoints: Endpoints,
        http: reqwest::Client,
        token: Arc<TokenStore>,
        config: RealtimeConfig,
    ) -> Self {
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let event_flow = EventFlow::new();
        Self {
            inner: Arc::new(RealtimeInner {
                hub,
                endpoints,
                http,
                token,
                config,
                lifecycle: ParkingMutex::new(Lifecycle::Disconnected),
                lifecycle_changed: Notify::new(),
                generation: AtomicU64::new(0),
                reconnect_enabled: AtomicBool::new(false),
                owner_cancel: CancellationToken::new(),
                client_handles: AtomicUsize::new(1),
                watchdog_task: ParkingMutex::new(None),
                socket_probe: ParkingMutex::new(None),
                request_counter: AtomicU64::new(1),
                pending: Arc::default(),
                event_tx,
                event_rx: ParkingMutex::new(Some(RealtimeEventReceiver {
                    events: event_rx,
                    flow: Arc::clone(&event_flow),
                    gap_reported: false,
                })),
                event_flow,
            }),
        }
    }

    /// Claims this client's single event receiver.
    #[must_use]
    pub fn take_event_receiver(&self) -> Option<RealtimeEventReceiver> {
        self.inner.event_rx.lock().take()
    }

    /// Connects and validates the `SignalR` handshake.
    ///
    /// # Errors
    ///
    /// Returns an error when authentication, URL construction, WebSocket
    /// upgrade, or the `SignalR` handshake fails.
    pub async fn connect(&self) -> Result<(), RealtimeError> {
        self.inner.connect_once(false).await
    }

    /// Gracefully disconnects and stops background tasks.
    ///
    /// # Errors
    ///
    /// Returns an error if the close handshake does not complete within the
    /// bounded timeout.
    pub async fn disconnect(&self) -> Result<(), RealtimeError> {
        self.inner.disconnect().await
    }

    /// Returns whether the latest connection completed its `SignalR` handshake.
    #[must_use]
    pub fn is_connected(&self) -> bool {
        self.inner.is_connected()
    }

    /// Captures the current socket for generation-scoped subscription admission.
    ///
    /// # Errors
    /// Returns [`RealtimeError::NotConnected`] unless a socket is ready.
    pub fn session(&self) -> Result<RealtimeSession, RealtimeError> {
        let lifecycle = self.inner.lifecycle.lock();
        let Lifecycle::Connected(session) = &*lifecycle else {
            return Err(RealtimeError::NotConnected);
        };
        Ok(RealtimeSession {
            inner: Arc::downgrade(&self.inner),
            generation: RealtimeGeneration(session.generation),
        })
    }

    /// Invokes an arbitrary provider target and waits for its completion frame.
    ///
    /// # Errors
    ///
    /// Returns an error when disconnected, a bounded capacity is exhausted,
    /// the provider rejects the invocation, or completion times out. A timeout
    /// or cancellation after queue admission has an unknown outcome and reclaims
    /// only that invocation slot. It never closes the connection or retries the call.
    pub async fn invoke(
        &self,
        target: impl Into<String>,
        arguments: &[Value],
    ) -> Result<(), RealtimeError> {
        self.inner
            .send_invocation(None, target.into(), arguments)
            .await
    }

    /// Subscribes to market trades for a contract.
    ///
    /// # Errors
    ///
    /// Returns a real-time invocation error.
    pub async fn subscribe_contract_trades(
        &self,
        contract: &ContractId,
    ) -> Result<(), RealtimeError> {
        if self.inner.hub != Hub::Market {
            return Err(RealtimeError::WrongHub);
        }
        self.session()?.subscribe_contract_trades(contract).await
    }

    /// Unsubscribes from market trades for a contract.
    ///
    /// # Errors
    ///
    /// Returns a real-time invocation error.
    pub async fn unsubscribe_contract_trades(
        &self,
        contract: &ContractId,
    ) -> Result<(), RealtimeError> {
        if self.inner.hub != Hub::Market {
            return Err(RealtimeError::WrongHub);
        }
        self.session()?.unsubscribe_contract_trades(contract).await
    }

    /// Subscribes to market quotes for a contract.
    ///
    /// # Errors
    ///
    /// Returns a real-time invocation error.
    pub async fn subscribe_contract_quotes(
        &self,
        contract: &ContractId,
    ) -> Result<(), RealtimeError> {
        if self.inner.hub != Hub::Market {
            return Err(RealtimeError::WrongHub);
        }
        self.session()?.subscribe_contract_quotes(contract).await
    }

    /// Unsubscribes from market quotes for a contract.
    ///
    /// # Errors
    ///
    /// Returns a real-time invocation error.
    pub async fn unsubscribe_contract_quotes(
        &self,
        contract: &ContractId,
    ) -> Result<(), RealtimeError> {
        if self.inner.hub != Hub::Market {
            return Err(RealtimeError::WrongHub);
        }
        self.session()?.unsubscribe_contract_quotes(contract).await
    }

    /// Subscribes to market depth for a contract.
    ///
    /// # Errors
    ///
    /// Returns a real-time invocation error.
    pub async fn subscribe_contract_depth(
        &self,
        contract: &ContractId,
    ) -> Result<(), RealtimeError> {
        if self.inner.hub != Hub::Market {
            return Err(RealtimeError::WrongHub);
        }
        self.session()?.subscribe_contract_depth(contract).await
    }

    /// Unsubscribes from market depth for a contract.
    ///
    /// # Errors
    ///
    /// Returns a real-time invocation error.
    pub async fn unsubscribe_contract_depth(
        &self,
        contract: &ContractId,
    ) -> Result<(), RealtimeError> {
        if self.inner.hub != Hub::Market {
            return Err(RealtimeError::WrongHub);
        }
        self.session()?.unsubscribe_contract_depth(contract).await
    }

    /// Subscribes to account updates.
    ///
    /// # Errors
    ///
    /// Returns a real-time invocation error.
    pub async fn subscribe_accounts(&self) -> Result<(), RealtimeError> {
        if self.inner.hub != Hub::User {
            return Err(RealtimeError::WrongHub);
        }
        self.session()?.subscribe_accounts().await
    }

    /// Unsubscribes from account updates.
    ///
    /// # Errors
    ///
    /// Returns a real-time invocation error.
    pub async fn unsubscribe_accounts(&self) -> Result<(), RealtimeError> {
        if self.inner.hub != Hub::User {
            return Err(RealtimeError::WrongHub);
        }
        self.session()?.unsubscribe_accounts().await
    }

    /// Subscribes to order updates for an account.
    ///
    /// # Errors
    ///
    /// Returns a real-time invocation error.
    pub async fn subscribe_orders(&self, account: AccountId) -> Result<(), RealtimeError> {
        if self.inner.hub != Hub::User {
            return Err(RealtimeError::WrongHub);
        }
        self.session()?.subscribe_orders(account).await
    }

    /// Unsubscribes from order updates for an account.
    ///
    /// # Errors
    ///
    /// Returns a real-time invocation error.
    pub async fn unsubscribe_orders(&self, account: AccountId) -> Result<(), RealtimeError> {
        if self.inner.hub != Hub::User {
            return Err(RealtimeError::WrongHub);
        }
        self.session()?.unsubscribe_orders(account).await
    }

    /// Subscribes to position updates for an account.
    ///
    /// # Errors
    ///
    /// Returns a real-time invocation error.
    pub async fn subscribe_positions(&self, account: AccountId) -> Result<(), RealtimeError> {
        if self.inner.hub != Hub::User {
            return Err(RealtimeError::WrongHub);
        }
        self.session()?.subscribe_positions(account).await
    }

    /// Unsubscribes from position updates for an account.
    ///
    /// # Errors
    ///
    /// Returns a real-time invocation error.
    pub async fn unsubscribe_positions(&self, account: AccountId) -> Result<(), RealtimeError> {
        if self.inner.hub != Hub::User {
            return Err(RealtimeError::WrongHub);
        }
        self.session()?.unsubscribe_positions(account).await
    }

    /// Subscribes to trade updates for an account.
    ///
    /// # Errors
    ///
    /// Returns a real-time invocation error.
    pub async fn subscribe_trades(&self, account: AccountId) -> Result<(), RealtimeError> {
        if self.inner.hub != Hub::User {
            return Err(RealtimeError::WrongHub);
        }
        self.session()?.subscribe_trades(account).await
    }

    /// Unsubscribes from trade updates for an account.
    ///
    /// # Errors
    ///
    /// Returns a real-time invocation error.
    pub async fn unsubscribe_trades(&self, account: AccountId) -> Result<(), RealtimeError> {
        if self.inner.hub != Hub::User {
            return Err(RealtimeError::WrongHub);
        }
        self.session()?.unsubscribe_trades(account).await
    }
}

impl Clone for RealtimeClient {
    fn clone(&self) -> Self {
        self.inner.client_handles.fetch_add(1, Ordering::Relaxed);
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl Drop for RealtimeClient {
    fn drop(&mut self) {
        if self.inner.client_handles.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.inner.shutdown_now();
        }
    }
}

impl fmt::Debug for RealtimeClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RealtimeClient")
            .field("hub", &self.inner.hub)
            .field("endpoints", &self.inner.endpoints)
            .field("connected", &self.is_connected())
            .finish_non_exhaustive()
    }
}
