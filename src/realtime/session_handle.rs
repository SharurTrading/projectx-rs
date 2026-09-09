// SPDX-FileCopyrightText: 2026 Kevin Monaghan
// SPDX-License-Identifier: MIT-0

use super::{
    AccountId, ContractId, Hub, RealtimeError, RealtimeGeneration, RealtimeInner, Value, Weak,
};

/// Subscription and invocation admission bound to one exact ready socket.
/// This handle cannot stop or reconnect the transport and does not keep its owner alive.
#[derive(Clone)]
pub struct RealtimeSession {
    pub(super) inner: Weak<RealtimeInner>,
    pub(super) generation: RealtimeGeneration,
}

impl RealtimeSession {
    /// The socket to which every invocation through this handle is bound.
    #[must_use]
    pub const fn generation(&self) -> RealtimeGeneration {
        self.generation
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
        arguments: Vec<Value>,
    ) -> Result<(), RealtimeError> {
        self.send_invocation(target.into(), arguments).await
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
        self.ensure_hub(Hub::Market)?;
        self.contract_invocation("SubscribeContractTrades", contract)
            .await
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
        self.ensure_hub(Hub::Market)?;
        self.contract_invocation("UnsubscribeContractTrades", contract)
            .await
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
        self.ensure_hub(Hub::Market)?;
        self.contract_invocation("SubscribeContractQuotes", contract)
            .await
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
        self.ensure_hub(Hub::Market)?;
        self.contract_invocation("UnsubscribeContractQuotes", contract)
            .await
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
        self.ensure_hub(Hub::Market)?;
        self.contract_invocation("SubscribeContractMarketDepth", contract)
            .await
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
        self.ensure_hub(Hub::Market)?;
        self.contract_invocation("UnsubscribeContractMarketDepth", contract)
            .await
    }

    /// Subscribes to account updates.
    ///
    /// # Errors
    ///
    /// Returns a real-time invocation error.
    pub async fn subscribe_accounts(&self) -> Result<(), RealtimeError> {
        self.ensure_hub(Hub::User)?;
        self.send_invocation("SubscribeAccounts".to_owned(), Vec::new())
            .await
    }

    /// Unsubscribes from account updates.
    ///
    /// # Errors
    ///
    /// Returns a real-time invocation error.
    pub async fn unsubscribe_accounts(&self) -> Result<(), RealtimeError> {
        self.ensure_hub(Hub::User)?;
        self.send_invocation("UnsubscribeAccounts".to_owned(), Vec::new())
            .await
    }

    /// Subscribes to order updates for an account.
    ///
    /// # Errors
    ///
    /// Returns a real-time invocation error.
    pub async fn subscribe_orders(&self, account: AccountId) -> Result<(), RealtimeError> {
        self.ensure_hub(Hub::User)?;
        self.account_invocation("SubscribeOrders", account).await
    }

    /// Unsubscribes from order updates for an account.
    ///
    /// # Errors
    ///
    /// Returns a real-time invocation error.
    pub async fn unsubscribe_orders(&self, account: AccountId) -> Result<(), RealtimeError> {
        self.ensure_hub(Hub::User)?;
        self.account_invocation("UnsubscribeOrders", account).await
    }

    /// Subscribes to position updates for an account.
    ///
    /// # Errors
    ///
    /// Returns a real-time invocation error.
    pub async fn subscribe_positions(&self, account: AccountId) -> Result<(), RealtimeError> {
        self.ensure_hub(Hub::User)?;
        self.account_invocation("SubscribePositions", account).await
    }

    /// Unsubscribes from position updates for an account.
    ///
    /// # Errors
    ///
    /// Returns a real-time invocation error.
    pub async fn unsubscribe_positions(&self, account: AccountId) -> Result<(), RealtimeError> {
        self.ensure_hub(Hub::User)?;
        self.account_invocation("UnsubscribePositions", account)
            .await
    }

    /// Subscribes to trade updates for an account.
    ///
    /// # Errors
    ///
    /// Returns a real-time invocation error.
    pub async fn subscribe_trades(&self, account: AccountId) -> Result<(), RealtimeError> {
        self.ensure_hub(Hub::User)?;
        self.account_invocation("SubscribeTrades", account).await
    }

    /// Unsubscribes from trade updates for an account.
    ///
    /// # Errors
    ///
    /// Returns a real-time invocation error.
    pub async fn unsubscribe_trades(&self, account: AccountId) -> Result<(), RealtimeError> {
        self.ensure_hub(Hub::User)?;
        self.account_invocation("UnsubscribeTrades", account).await
    }

    fn ensure_hub(&self, expected: Hub) -> Result<(), RealtimeError> {
        if self.inner.upgrade().ok_or(RealtimeError::NotConnected)?.hub == expected {
            Ok(())
        } else {
            Err(RealtimeError::WrongHub)
        }
    }

    async fn contract_invocation(
        &self,
        target: &str,
        contract: &ContractId,
    ) -> Result<(), RealtimeError> {
        self.inner
            .upgrade()
            .ok_or(RealtimeError::NotConnected)?
            .send_invocation(
                Some(self.generation),
                target.to_owned(),
                vec![Value::String(contract.to_string())],
            )
            .await
    }

    async fn account_invocation(
        &self,
        target: &str,
        account: AccountId,
    ) -> Result<(), RealtimeError> {
        self.inner
            .upgrade()
            .ok_or(RealtimeError::NotConnected)?
            .send_invocation(
                Some(self.generation),
                target.to_owned(),
                vec![Value::from(account.get())],
            )
            .await
    }

    async fn send_invocation(
        &self,
        target: String,
        arguments: Vec<Value>,
    ) -> Result<(), RealtimeError> {
        self.inner
            .upgrade()
            .ok_or(RealtimeError::NotConnected)?
            .send_invocation(Some(self.generation), target, arguments)
            .await
    }
}

impl std::fmt::Debug for RealtimeSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RealtimeSession")
            .field("generation", &self.generation)
            .finish_non_exhaustive()
    }
}
