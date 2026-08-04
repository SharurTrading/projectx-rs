// SPDX-FileCopyrightText: 2026 Kevin Monaghan
// SPDX-License-Identifier: MIT-0

//! Authenticated REST client.

use std::{
    fmt,
    sync::Arc,
    time::{Duration, SystemTime},
};

use futures_util::StreamExt as _;
use parking_lot::Mutex;
use reqwest::{Method, Proxy, header, redirect::Policy};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::{task::JoinHandle, time::MissedTickBehavior};
use tokio_util::sync::CancellationToken;

use crate::{
    Account, AccountId, ApplicationCredentials, Bar, CancelOrder, CloseContract, Contract,
    Credentials, Endpoints, Error, HistoryRequest, Hub, ModifyOrder, OperationResponse, Order,
    OrderId, OrderPage, OrderQuery, OrderResponse, OrderSearch, PartialCloseContract, PlaceOrder,
    Position, ProviderError, RateLimitConfig, RateLimitKind, RealtimeClient, SearchContracts,
    Trade, TradeQuery, TradeSearch,
    credentials::AuthenticationCredentials,
    models::{
        AccountsBody, BarsBody, ContractBody, ContractsBody, EmptyBody, Envelope, OrderBody,
        OrdersBody, PlaceOrderBody, PositionsBody, TradesBody,
    },
    rate_limit::RateLimits,
    token::{TokenRevision, TokenSnapshot, TokenStore, UpdateOutcome},
};

const DEFAULT_TIMEOUT: Duration = Duration::from_mins(1);
const DEFAULT_RESPONSE_LIMIT: usize = 16 * 1024 * 1024;
const DEFAULT_MAX_RETRIES: u32 = 3;
const DEFAULT_RETRY_INITIAL: Duration = Duration::from_secs(1);
const DEFAULT_RETRY_MAX: Duration = Duration::from_secs(10);
const MAX_SERVER_RETRY_AFTER: Duration = Duration::from_hours(24);
const USER_AGENT: &str = concat!("projectx-client/", env!("CARGO_PKG_VERSION"));

/// Authenticated `ProjectX` REST client.
///
/// The client uses the caller's Tokio runtime and keeps its bearer token
/// private. Call [`Client::authenticate`] before calling authenticated methods.
/// Safe authenticated queries wait asynchronously for shared rate-limit
/// capacity before their HTTP timeout begins. Money-moving mutations instead
/// return [`Error::LocallyRateLimited`] without sending when capacity is full.
pub struct Client {
    credentials: Arc<AuthenticationCredentials>,
    endpoints: Endpoints,
    http: reqwest::Client,
    realtime_http: reqwest::Client,
    token: Arc<TokenStore>,
    rate_limits: Arc<RateLimits>,
    response_limit: usize,
    max_retries: u32,
    retry_initial: Duration,
    retry_max: Duration,
}

/// Invalidates the exact session admitted to a logout request unless the
/// provider definitively rejected the request before admission.
struct LogoutAttempt {
    store: Arc<TokenStore>,
    basis: TokenSnapshot,
    armed: bool,
}

impl LogoutAttempt {
    fn new(store: Arc<TokenStore>, basis: TokenSnapshot) -> Self {
        Self {
            store,
            basis,
            armed: true,
        }
    }

    fn basis(&self) -> &TokenSnapshot {
        &self.basis
    }

    fn retain_session(mut self) {
        self.armed = false;
    }
}

impl Drop for LogoutAttempt {
    fn drop(&mut self) {
        if self.armed {
            self.store.invalidate_if_current(&self.basis);
        }
    }
}

/// Invalidates one exact session revision unless validation reaches a
/// trustworthy terminal result.
struct ValidationAttempt {
    store: Arc<TokenStore>,
    basis: TokenSnapshot,
    tracker: Option<Arc<ValidationTracker>>,
    armed: bool,
}

impl ValidationAttempt {
    fn new(
        store: Arc<TokenStore>,
        basis: TokenSnapshot,
        tracker: Option<&Arc<ValidationTracker>>,
    ) -> Option<Self> {
        if tracker.is_some_and(|tracker| !tracker.register(basis.revision())) {
            return None;
        }
        Some(Self {
            store,
            basis,
            tracker: tracker.cloned(),
            armed: true,
        })
    }

    fn basis(&self) -> &TokenSnapshot {
        &self.basis
    }

    fn disarm(mut self) {
        self.armed = false;
        self.release_tracker();
    }

    fn claim_trustworthy_completion(&mut self) -> bool {
        let Some(tracker) = self.tracker.take() else {
            return true;
        };
        tracker.claim_completion(self.basis.revision())
    }

    fn release_tracker(&mut self) {
        if let Some(tracker) = self.tracker.take() {
            let _claimed = tracker.claim_completion(self.basis.revision());
        }
    }
}

impl Drop for ValidationAttempt {
    fn drop(&mut self) {
        if self.armed {
            self.store.invalidate_if_current(&self.basis);
        }
        self.release_tracker();
    }
}

/// Coordinates cancellation with the periodic validator's admitted request.
#[derive(Default)]
struct ValidationTracker {
    state: Mutex<ValidationTrackerState>,
}

#[derive(Default)]
struct ValidationTrackerState {
    closed: bool,
    active: Option<TokenRevision>,
}

impl ValidationTracker {
    fn register(&self, basis: TokenRevision) -> bool {
        let mut state = self.state.lock();
        if state.closed || state.active.is_some() {
            return false;
        }
        state.active = Some(basis);
        true
    }

    fn claim_completion(&self, basis: TokenRevision) -> bool {
        let mut state = self.state.lock();
        if state.active == Some(basis) {
            state.active = None;
            return true;
        }
        false
    }

    fn close(&self) -> Option<TokenRevision> {
        let mut state = self.state.lock();
        state.closed = true;
        state.active.take()
    }
}

impl Client {
    /// Starts configuring a client with explicit API-key credentials.
    pub fn builder(credentials: Credentials) -> ClientBuilder {
        ClientBuilder::new(AuthenticationCredentials::ApiKey(credentials))
    }

    /// Starts configuring a client with authorized-application credentials.
    pub fn application_builder(credentials: ApplicationCredentials) -> ClientBuilder {
        ClientBuilder::new(AuthenticationCredentials::Application(credentials))
    }

    /// Creates a real-time client sharing this client's rotating bearer token.
    ///
    /// The returned hub snapshots the current token immediately before every
    /// initial connection and reconnect. Call [`Self::authenticate`] first.
    #[must_use]
    pub fn realtime(&self, hub: Hub) -> RealtimeClient {
        RealtimeClient::new(
            hub,
            self.endpoints.clone(),
            self.realtime_http.clone(),
            Arc::clone(&self.token),
        )
    }

    /// Authenticates with the endpoint selected by the configured credential type.
    ///
    /// API-key credentials send exactly `userName` and `apiKey` to
    /// `/api/Auth/loginKey`. Authorized-application credentials send exactly
    /// `userName`, `password`, `deviceId`, `appId`, and `verifyKey` to
    /// `/api/Auth/loginApp`.
    ///
    /// # Errors
    ///
    /// Returns an error when transport fails, credentials are rejected, or the
    /// provider omits a usable token.
    pub async fn authenticate(&self) -> Result<(), Error> {
        let attempt = self.token.begin_authentication();
        let response: LoginResponse = match self.credentials.as_ref() {
            AuthenticationCredentials::ApiKey(credentials) => {
                let body = LoginApiKeyRequest {
                    user_name: credentials.expose_user_name(),
                    api_key: credentials.expose_api_key(),
                };
                self.post_unauthenticated("api/Auth/loginKey", &body)
                    .await?
            }
            AuthenticationCredentials::Application(credentials) => {
                let body = LoginAppRequest {
                    user_name: credentials.expose_user_name(),
                    password: credentials.expose_password(),
                    device_id: credentials.expose_device_id(),
                    app_id: credentials.expose_app_id(),
                    verify_key: credentials.expose_verify_key(),
                };
                self.post_unauthenticated("api/Auth/loginApp", &body)
                    .await?
            }
        };
        validate_response_status(response.success, response.error_code)?;
        if !response.success {
            return Err(Error::CredentialsRejected {
                code: response.error_code,
            });
        }
        let token = validate_token(response.token.as_deref())?;
        if !attempt.commit(token) {
            tracing::debug!(
                "discarded a delayed authentication response after another authentication completed"
            );
        }
        Ok(())
    }

    /// Authenticates and starts periodic token validation.
    ///
    /// The validator is cancelled when the returned guard is dropped. Prefer
    /// [`SessionValidator::shutdown`] when graceful task completion matters.
    ///
    /// # Errors
    ///
    /// Returns an error when the validation period is zero or cannot be
    /// represented by Tokio's clock, or when authentication fails.
    pub async fn authenticate_with_validation(
        &self,
        period: Duration,
    ) -> Result<SessionValidator, Error> {
        if period.is_zero() {
            return Err(Error::Configuration(
                "validation period must be non-zero".to_owned(),
            ));
        }
        let first_tick = tokio::time::Instant::now()
            .checked_add(period)
            .ok_or_else(|| {
                Error::Configuration(
                    "validation period cannot be represented by the Tokio clock".to_owned(),
                )
            })?;
        self.authenticate().await?;
        let cancellation = CancellationToken::new();
        let task_cancellation = cancellation.clone();
        let validation_tracker = Arc::new(ValidationTracker::default());
        let task_validation_tracker = Arc::clone(&validation_tracker);
        let client = self.clone();
        let task = tokio::spawn(async move {
            let mut ticker = tokio::time::interval_at(first_tick, period);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    () = task_cancellation.cancelled() => break Ok(()),
                    _ = ticker.tick() => {
                        tokio::select! {
                            biased;
                            result = client.validate_session_tracked(Some(Arc::clone(
                                &task_validation_tracker,
                            ))) => {
                                match result {
                                    Ok(()) => {}
                                    Err(error)
                                        if is_terminal_session_error(&error)
                                            && !client
                                                .token
                                                .has_viable_session_or_authentication() =>
                                    {
                                        break Err(error);
                                    }
                                    Err(error) => {
                                        tracing::warn!(%error, "ProjectX token validation failed");
                                    }
                                }
                            }
                            () = task_cancellation.cancelled() => {
                                if client.token.is_authenticated() {
                                    break Ok(());
                                }
                                break Err(Error::AmbiguousSessionValidation);
                            }
                        }
                    }
                }
            }
        });
        Ok(SessionValidator {
            cancellation,
            task: Some(task),
            token: Arc::clone(&self.token),
            validation_tracker,
        })
    }

    /// Validates the current bearer token and applies provider token rotation.
    ///
    /// # Errors
    ///
    /// Returns an error when unauthenticated, transport fails, or validation is
    /// rejected. Once the validation request may have reached the provider,
    /// cancellation or an untrustworthy response invalidates that exact token
    /// revision and requires authentication before it can be used again.
    pub async fn validate_session(&self) -> Result<(), Error> {
        self.validate_session_tracked(None).await
    }

    /// Logs out the current provider session exactly once.
    ///
    /// A request that may have reached the provider invalidates only the exact
    /// bearer-token revision it used, including when the future is cancelled or
    /// the response is ambiguous. A definitive connection failure or HTTP 429
    /// retains that revision because the provider did not admit the logout.
    ///
    /// # Errors
    ///
    /// Returns an error when unauthenticated, locally or remotely rate limited,
    /// rejected by the provider, or when no trustworthy response is available.
    pub async fn logout(&self) -> Result<OperationResponse, Error> {
        self.require_authentication()?;
        let url = self.endpoints.api_url("api/Auth/logout")?;
        self.rate_limits
            .try_acquire(RateLimitKind::General)
            .map_err(|retry_after| Error::LocallyRateLimited {
                kind: RateLimitKind::General,
                retry_after,
            })?;
        let basis = self
            .token
            .versioned_snapshot()
            .ok_or(Error::NotAuthenticated)?;
        let attempt = LogoutAttempt::new(Arc::clone(&self.token), basis);
        let response = match self
            .http
            .request(Method::POST, url)
            .bearer_auth(attempt.basis().expose())
            .send()
            .await
        {
            Ok(response) => response,
            Err(error) if error.is_connect() || error.is_builder() => {
                attempt.retain_session();
                return Err(Error::Transport(error));
            }
            Err(error) => return Err(Error::Transport(error)),
        };
        if response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
            let retry_after =
                self.apply_provider_cooldown(RateLimitKind::General, response.headers());
            attempt.retain_session();
            return Err(Error::ProviderRateLimited {
                kind: RateLimitKind::General,
                retry_after,
            });
        }
        let response: Envelope<EmptyBody> = self.decode(response).await?;
        accepted(response)?;
        Ok(OperationResponse)
    }

    /// Checks whether the provider REST service is responsive.
    ///
    /// This unauthenticated operation expects the provider's exact `pong`
    /// response and applies the configured response-size bound.
    ///
    /// # Errors
    ///
    /// Returns an error for URL, transport, HTTP status, response-size, or
    /// unexpected-response failures.
    pub async fn ping(&self) -> Result<(), Error> {
        let url = self.endpoints.api_url("api/Status/ping")?;
        let response = self
            .http
            .request(Method::GET, url)
            .send()
            .await
            .map_err(Error::Transport)?;
        let bytes = self.read_bounded_response(response).await?;
        if bytes == b"pong" {
            Ok(())
        } else {
            Err(Error::UnexpectedStatusResponse)
        }
    }

    async fn validate_session_tracked(
        &self,
        tracker: Option<Arc<ValidationTracker>>,
    ) -> Result<(), Error> {
        let (response, attempt) = self.post_validation(tracker).await?;
        let response: ValidateResponse = self.decode(response).await?;
        validate_response_status(response.success, response.error_code)?;
        if !response.success {
            let error = Error::SessionValidationRejected {
                code: response.error_code,
            };
            if matches!(response.error_code, 1..=3) {
                return Err(error);
            }
            return Err(Error::AmbiguousSessionValidation);
        }
        let new_token = response
            .new_token
            .as_deref()
            .map(|token| validate_token(Some(token)))
            .transpose()?;
        if let Some(outcome) = finish_trustworthy_validation(&self.token, attempt, new_token)? {
            match outcome {
                UpdateOutcome::Applied => {}
                UpdateOutcome::Deferred => {
                    tracing::debug!(
                        "deferred token rotation until concurrent authentication completes"
                    );
                }
                UpdateOutcome::Stale => {
                    tracing::debug!(
                        "discarded token rotation from a stale session-validation response"
                    );
                }
            }
        }
        Ok(())
    }

    /// Retrieves active accounts for the authenticated user.
    ///
    /// # Errors
    ///
    /// Returns an error for authentication, transport, provider, or decode failures.
    pub async fn search_active_accounts(&self) -> Result<Vec<Account>, Error> {
        self.search_accounts(true).await
    }

    /// Searches accounts for the authenticated user.
    ///
    /// Set `only_active_accounts` to `false` to include inactive accounts. Use
    /// [`Self::search_active_accounts`] when only active accounts are required.
    ///
    /// # Errors
    ///
    /// Returns an error for authentication, transport, provider, or decode failures.
    pub async fn search_accounts(&self, only_active_accounts: bool) -> Result<Vec<Account>, Error> {
        let response: Envelope<AccountsBody> = self
            .post_authenticated(
                RateLimitKind::General,
                "api/Account/search",
                &AccountSearchRequest {
                    only_active_accounts,
                },
            )
            .await?;
        Ok(accepted(response)?.accounts)
    }

    /// Lists contracts available to the selected live or simulated data feed.
    ///
    /// # Errors
    ///
    /// Returns an error for authentication, transport, provider, or decode failures.
    pub async fn available_contracts(&self, live: bool) -> Result<Vec<Contract>, Error> {
        let response: Envelope<ContractsBody> = self
            .post_authenticated(
                RateLimitKind::General,
                "api/Contract/available",
                &AvailableContractsRequest { live },
            )
            .await?;
        Ok(accepted(response)?.contracts)
    }

    /// Searches contracts using provider-native search text.
    ///
    /// The provider returns at most 20 matching contracts per request.
    ///
    /// # Errors
    ///
    /// Returns an error for authentication, transport, provider, or decode failures.
    pub async fn search_contracts(
        &self,
        request: &SearchContracts,
    ) -> Result<Vec<Contract>, Error> {
        let response: Envelope<ContractsBody> = self
            .post_authenticated(RateLimitKind::General, "api/Contract/search", request)
            .await?;
        Ok(accepted(response)?.contracts)
    }

    /// Retrieves one contract by its explicit provider identifier.
    ///
    /// # Errors
    ///
    /// Returns an error for authentication, transport, provider, or decode failures.
    pub async fn contract_by_id(&self, contract_id: &crate::ContractId) -> Result<Contract, Error> {
        let response: Envelope<ContractBody> = self
            .post_authenticated(
                RateLimitKind::General,
                "api/Contract/searchById",
                &ContractRequest { contract_id },
            )
            .await?;
        Ok(accepted(response)?.contract)
    }

    /// Retrieves historical bars for an explicit provider contract.
    ///
    /// # Errors
    ///
    /// Returns an error for authentication, transport, provider, or decode failures.
    pub async fn retrieve_bars(&self, request: &HistoryRequest) -> Result<Vec<Bar>, Error> {
        let response: Envelope<BarsBody> = self
            .post_authenticated(RateLimitKind::History, "api/History/retrieveBars", request)
            .await?;
        Ok(accepted(response)?.bars)
    }

    /// Searches historical orders for an account and time range.
    ///
    /// # Errors
    ///
    /// Returns an error for authentication, transport, provider, or decode failures.
    pub async fn search_orders(&self, request: &OrderSearch) -> Result<Vec<Order>, Error> {
        let response: Envelope<OrdersBody> = self
            .post_authenticated(RateLimitKind::General, "api/Order/search", request)
            .await?;
        Ok(accepted(response)?.orders)
    }

    /// Retrieves one order by its provider account and order identifiers.
    ///
    /// # Errors
    ///
    /// Returns an error for authentication, transport, provider, or decode failures.
    pub async fn order_by_id(
        &self,
        account_id: AccountId,
        order_id: OrderId,
    ) -> Result<Order, Error> {
        let response: Envelope<OrderBody> = self
            .post_authenticated(
                RateLimitKind::General,
                "api/Order/searchById",
                &AccountOrderRequest {
                    account_id,
                    order_id,
                },
            )
            .await?;
        Ok(accepted(response)?.order)
    }

    /// Searches currently open orders for an account.
    ///
    /// The provider excludes `Suspended` orders from this legacy endpoint,
    /// including inactive bracket children. Use [`Self::query_orders`] and
    /// explicitly select every non-terminal status needed by the application
    /// when building a complete working-order reconciliation view.
    ///
    /// # Errors
    ///
    /// Returns an error for authentication, transport, provider, or decode failures.
    pub async fn search_open_orders(&self, account_id: AccountId) -> Result<Vec<Order>, Error> {
        let response: Envelope<OrdersBody> = self
            .post_authenticated(
                RateLimitKind::General,
                "api/Order/searchOpen",
                &AccountRequest { account_id },
            )
            .await?;
        Ok(accepted(response)?.orders)
    }

    /// Queries filtered orders through `/api/Order/v2/query`.
    ///
    /// For a complete non-terminal working-order reconciliation, filter on
    /// [`OrderStatus::Open`](crate::OrderStatus::Open),
    /// [`OrderStatus::Pending`](crate::OrderStatus::Pending),
    /// [`OrderStatus::PendingCancellation`](crate::OrderStatus::PendingCancellation), and
    /// [`OrderStatus::Suspended`](crate::OrderStatus::Suspended). The last of
    /// these includes inactive bracket children omitted by [`Self::search_open_orders`].
    /// Paginated callers must continue until the returned page is exhausted;
    /// request a total count when an explicit completion check is useful.
    ///
    /// # Errors
    ///
    /// Returns an error for authentication, transport, provider, or decode failures.
    pub async fn query_orders(&self, request: &OrderQuery) -> Result<OrderPage, Error> {
        let response: Envelope<OrderPage> = self
            .post_authenticated(RateLimitKind::General, "api/Order/v2/query", request)
            .await?;
        accepted(response)
    }

    /// Places an order exactly once.
    ///
    /// This method never retries. An untrustworthy response is returned as
    /// [`Error::AmbiguousMutation`]; callers must reconcile open and recent
    /// orders before deciding whether another submission is safe.
    ///
    /// # Errors
    ///
    /// Returns an error for authentication, provider, decode, or ambiguous
    /// transport outcomes.
    pub async fn place_order(&self, request: &PlaceOrder) -> Result<OrderResponse, Error> {
        let kind = MutationKind::OrderPlacement;
        let response: Envelope<PlaceOrderBody> = self
            .post_authenticated_no_retry(RateLimitKind::General, kind.path(), request)
            .await
            .map_err(|error| ambiguous_mutation(kind, error))?;
        let body = accepted(response).map_err(|error| ambiguous_mutation(kind, error))?;
        let order_id = body.order_id.ok_or(Error::AmbiguousMutation {
            operation: kind.operation(),
        })?;
        Ok(OrderResponse { order_id })
    }

    /// Cancels an order exactly once.
    ///
    /// This method never retries. If the provider may have admitted the
    /// request but no trustworthy result is available, it returns
    /// [`Error::AmbiguousMutation`]. Reconcile provider state before retrying.
    ///
    /// # Errors
    ///
    /// Returns an error for authentication, provider rejection, local rate
    /// limiting, or an ambiguous post-admission outcome.
    pub async fn cancel_order(&self, request: &CancelOrder) -> Result<OperationResponse, Error> {
        self.mutation(MutationKind::OrderCancellation, request)
            .await
    }

    /// Modifies an open order exactly once.
    ///
    /// This method never retries. If the provider may have admitted the
    /// request but no trustworthy result is available, it returns
    /// [`Error::AmbiguousMutation`]. Reconcile provider state before retrying.
    ///
    /// # Errors
    ///
    /// Returns an error for authentication, provider rejection, local rate
    /// limiting, or an ambiguous post-admission outcome.
    pub async fn modify_order(&self, request: &ModifyOrder) -> Result<OperationResponse, Error> {
        self.mutation(MutationKind::OrderModification, request)
            .await
    }

    /// Searches currently open positions for an account.
    ///
    /// # Errors
    ///
    /// Returns an error for authentication, transport, provider, or decode failures.
    pub async fn search_open_positions(
        &self,
        account_id: AccountId,
    ) -> Result<Vec<Position>, Error> {
        let response: Envelope<PositionsBody> = self
            .post_authenticated(
                RateLimitKind::General,
                "api/Position/searchOpen",
                &AccountRequest { account_id },
            )
            .await?;
        Ok(accepted(response)?.positions)
    }

    /// Closes the open position for an explicit account and contract exactly once.
    ///
    /// This method never retries. If the provider may have admitted the
    /// request but no trustworthy result is available, it returns
    /// [`Error::AmbiguousMutation`]. Reconcile provider state before retrying.
    ///
    /// # Errors
    ///
    /// Returns an error for authentication, provider rejection, local rate
    /// limiting, or an ambiguous post-admission outcome.
    pub async fn close_contract(
        &self,
        request: &CloseContract,
    ) -> Result<OperationResponse, Error> {
        self.mutation(MutationKind::PositionClose, request).await
    }

    /// Partially closes an open position for an account and contract exactly once.
    ///
    /// This method never retries. If the provider may have admitted the
    /// request but no trustworthy result is available, it returns
    /// [`Error::AmbiguousMutation`]. Reconcile provider state before retrying.
    ///
    /// # Errors
    ///
    /// Returns an error for authentication, provider rejection, local rate
    /// limiting, or an ambiguous post-admission outcome.
    pub async fn partial_close_contract(
        &self,
        request: &PartialCloseContract,
    ) -> Result<OperationResponse, Error> {
        self.mutation(MutationKind::PartialPositionClose, request)
            .await
    }

    /// Searches executions for an account and time range.
    ///
    /// # Errors
    ///
    /// Returns an error for authentication, transport, provider, or decode failures.
    pub async fn search_trades(&self, request: &TradeSearch) -> Result<Vec<Trade>, Error> {
        let response: Envelope<TradesBody> = self
            .post_authenticated(RateLimitKind::General, "api/Trade/search", request)
            .await?;
        Ok(accepted(response)?.trades)
    }

    /// Searches trades with optional start and end timestamp bounds.
    ///
    /// Unlike [`Self::search_trades`], a [`TradeQuery`] can omit either or both
    /// timestamp bounds to express the provider's complete request schema.
    ///
    /// # Errors
    ///
    /// Returns an error for authentication, transport, provider, or decode failures.
    pub async fn query_trades(&self, request: &TradeQuery) -> Result<Vec<Trade>, Error> {
        let response: Envelope<TradesBody> = self
            .post_authenticated(RateLimitKind::General, "api/Trade/search", request)
            .await?;
        Ok(accepted(response)?.trades)
    }

    async fn mutation<T>(&self, kind: MutationKind, request: &T) -> Result<OperationResponse, Error>
    where
        T: Serialize + ?Sized,
    {
        let response: Envelope<EmptyBody> = self
            .post_authenticated_no_retry(RateLimitKind::General, kind.path(), request)
            .await
            .map_err(|error| ambiguous_mutation(kind, error))?;
        accepted(response).map_err(|error| ambiguous_mutation(kind, error))?;
        Ok(OperationResponse)
    }

    async fn post_unauthenticated<T, R>(&self, path: &str, body: &T) -> Result<R, Error>
    where
        T: Serialize + ?Sized,
        R: DeserializeOwned,
    {
        let url = self.endpoints.api_url(path)?;
        let response = self
            .http
            .request(Method::POST, url)
            .json(body)
            .send()
            .await
            .map_err(Error::Transport)?;
        self.decode(response).await
    }

    async fn post_authenticated<T, R>(
        &self,
        kind: RateLimitKind,
        path: &str,
        body: &T,
    ) -> Result<R, Error>
    where
        T: Serialize + ?Sized,
        R: DeserializeOwned,
    {
        self.post_authenticated_tracked(kind, path, body)
            .await
            .map(|(response, _basis)| response)
    }

    async fn post_authenticated_tracked<T, R>(
        &self,
        kind: RateLimitKind,
        path: &str,
        body: &T,
    ) -> Result<(R, TokenSnapshot), Error>
    where
        T: Serialize + ?Sized,
        R: DeserializeOwned,
    {
        let encoded = serde_json::to_vec(body).map_err(Error::Encode)?;
        self.post_authenticated_encoded(kind, path, &encoded).await
    }

    async fn post_authenticated_encoded<R>(
        &self,
        kind: RateLimitKind,
        path: &str,
        body: &[u8],
    ) -> Result<(R, TokenSnapshot), Error>
    where
        R: DeserializeOwned,
    {
        self.require_authentication()?;
        let mut attempt = 0;
        let mut delay = self.retry_initial;
        loop {
            self.rate_limits.wait(kind).await;
            match self.post_authenticated_once(kind, path, body).await {
                Ok(response) => return Ok(response),
                Err(error) if attempt < self.max_retries && should_retry(&error) => {
                    attempt += 1;
                    tokio::time::sleep(retry_delay(&error, delay)).await;
                    delay = delay.saturating_mul(2).min(self.retry_max);
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn post_authenticated_no_retry<T, R>(
        &self,
        kind: RateLimitKind,
        path: &str,
        body: &T,
    ) -> Result<R, Error>
    where
        T: Serialize + ?Sized,
        R: DeserializeOwned,
    {
        let encoded = serde_json::to_vec(body).map_err(Error::Encode)?;
        self.require_authentication()?;
        self.rate_limits
            .try_acquire(kind)
            .map_err(|retry_after| Error::LocallyRateLimited { kind, retry_after })?;
        self.post_authenticated_once(kind, path, &encoded)
            .await
            .map(|(response, _basis)| response)
    }

    async fn post_authenticated_once<R>(
        &self,
        kind: RateLimitKind,
        path: &str,
        body: &[u8],
    ) -> Result<(R, TokenSnapshot), Error>
    where
        R: DeserializeOwned,
    {
        let token = self
            .token
            .versioned_snapshot()
            .ok_or(Error::NotAuthenticated)?;
        let url = self.endpoints.api_url(path)?;
        let request = self
            .http
            .request(Method::POST, url)
            .bearer_auth(token.expose())
            .body(body.to_vec());
        let response = request.send().await.map_err(Error::Transport)?;
        if response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
            let retry_after = self.apply_provider_cooldown(kind, response.headers());
            return Err(Error::ProviderRateLimited { kind, retry_after });
        }
        if response.status() == reqwest::StatusCode::UNAUTHORIZED {
            self.token.invalidate_if_current(&token);
            return Err(Error::UnexpectedStatus { status: 401 });
        }
        self.decode(response)
            .await
            .map(|response| (response, token))
    }

    async fn post_validation(
        &self,
        tracker: Option<Arc<ValidationTracker>>,
    ) -> Result<(reqwest::Response, ValidationAttempt), Error> {
        self.require_authentication()?;
        let mut attempt = 0;
        let mut delay = self.retry_initial;
        loop {
            self.rate_limits.wait(RateLimitKind::General).await;
            match self.post_validation_once(tracker.as_ref()).await {
                Ok(response) => return Ok(response),
                Err(error) if attempt < self.max_retries && should_retry(&error) => {
                    attempt += 1;
                    tokio::time::sleep(retry_delay(&error, delay)).await;
                    delay = delay.saturating_mul(2).min(self.retry_max);
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn post_validation_once(
        &self,
        tracker: Option<&Arc<ValidationTracker>>,
    ) -> Result<(reqwest::Response, ValidationAttempt), Error> {
        let url = self.endpoints.api_url("api/Auth/validate")?;
        let basis = self
            .token
            .versioned_snapshot()
            .ok_or(Error::NotAuthenticated)?;
        let attempt = ValidationAttempt::new(Arc::clone(&self.token), basis, tracker)
            .ok_or(Error::NotAuthenticated)?;
        let response = match self
            .http
            .request(Method::POST, url)
            .bearer_auth(attempt.basis().expose())
            .send()
            .await
        {
            Ok(response) => response,
            Err(error) if error.is_connect() || error.is_builder() => {
                attempt.disarm();
                return Err(Error::Transport(error));
            }
            Err(_error) => return Err(Error::AmbiguousSessionValidation),
        };
        if response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
            let retry_after =
                self.apply_provider_cooldown(RateLimitKind::General, response.headers());
            attempt.disarm();
            return Err(Error::ProviderRateLimited {
                kind: RateLimitKind::General,
                retry_after,
            });
        }
        if response.status() == reqwest::StatusCode::UNAUTHORIZED {
            return Err(Error::UnexpectedStatus { status: 401 });
        }
        if !response.status().is_success() {
            return Err(Error::AmbiguousSessionValidation);
        }
        Ok((response, attempt))
    }

    fn apply_provider_cooldown(
        &self,
        kind: RateLimitKind,
        headers: &header::HeaderMap,
    ) -> Duration {
        let retry_after = parse_retry_after(headers, SystemTime::now())
            .unwrap_or_else(|| self.rate_limits.limit(kind).window())
            .min(MAX_SERVER_RETRY_AFTER);
        self.rate_limits.cool_down(kind, retry_after);
        retry_after
    }

    fn require_authentication(&self) -> Result<(), Error> {
        if self.token.is_authenticated() {
            Ok(())
        } else {
            Err(Error::NotAuthenticated)
        }
    }

    async fn decode<R>(&self, response: reqwest::Response) -> Result<R, Error>
    where
        R: DeserializeOwned,
    {
        let bytes = self.read_bounded_response(response).await?;
        serde_json::from_slice(&bytes).map_err(Error::Decode)
    }

    async fn read_bounded_response(&self, response: reqwest::Response) -> Result<Vec<u8>, Error> {
        let status = response.status();
        if !status.is_success() {
            return Err(Error::UnexpectedStatus {
                status: status.as_u16(),
            });
        }
        let content_length = response.content_length();
        if content_length.is_some_and(|length| length > self.response_limit as u64) {
            return Err(Error::ResponseTooLarge {
                limit_bytes: self.response_limit,
            });
        }

        let capacity = content_length
            .and_then(|length| usize::try_from(length).ok())
            .unwrap_or(0);
        let mut bytes = Vec::with_capacity(capacity);
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(Error::Transport)?;
            if bytes.len().saturating_add(chunk.len()) > self.response_limit {
                return Err(Error::ResponseTooLarge {
                    limit_bytes: self.response_limit,
                });
            }
            bytes.extend_from_slice(&chunk);
        }
        Ok(bytes)
    }
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client")
            .field("credentials", &self.credentials)
            .field("endpoints", &self.endpoints)
            .field("response_limit", &self.response_limit)
            .field("max_retries", &self.max_retries)
            .field("rate_limits", &self.rate_limits.config())
            .finish_non_exhaustive()
    }
}

impl Clone for Client {
    fn clone(&self) -> Self {
        Self {
            credentials: Arc::clone(&self.credentials),
            endpoints: self.endpoints.clone(),
            http: self.http.clone(),
            realtime_http: self.realtime_http.clone(),
            token: Arc::clone(&self.token),
            rate_limits: Arc::clone(&self.rate_limits),
            response_limit: self.response_limit,
            max_retries: self.max_retries,
            retry_initial: self.retry_initial,
            retry_max: self.retry_max,
        }
    }
}

/// Guard for a periodic token-validation task.
#[must_use = "dropping the validator cancels periodic token validation"]
pub struct SessionValidator {
    cancellation: CancellationToken,
    task: Option<JoinHandle<Result<(), Error>>>,
    token: Arc<TokenStore>,
    validation_tracker: Arc<ValidationTracker>,
}

impl SessionValidator {
    fn close_validation(&self) -> bool {
        let Some(revision) = self.validation_tracker.close() else {
            return false;
        };
        self.token.invalidate_revision_if_current(revision);
        true
    }

    /// Cancels validation and waits for its task to finish.
    ///
    /// # Errors
    ///
    /// Returns the terminal validation error if the provider invalidated the
    /// session, [`Error::AmbiguousSessionValidation`] when shutdown cancels an
    /// admitted validation request, or [`Error::BackgroundTaskFailed`] if the
    /// library-owned task panicked or was aborted unexpectedly.
    pub async fn shutdown(mut self) -> Result<(), Error> {
        let validation_was_active = self.close_validation();
        self.cancellation.cancel();
        let task_result = if let Some(task) = self.task.take() {
            task.await
                .map_err(|_join_error| Error::BackgroundTaskFailed {
                    task: "session validator",
                })?
        } else {
            Ok(())
        };
        if validation_was_active {
            return Err(Error::AmbiguousSessionValidation);
        }
        task_result
    }

    /// Returns whether the validation task has stopped.
    #[must_use]
    pub fn is_finished(&self) -> bool {
        self.task.as_ref().is_none_or(JoinHandle::is_finished)
    }
}

impl fmt::Debug for SessionValidator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SessionValidator")
            .field("cancelled", &self.cancellation.is_cancelled())
            .finish_non_exhaustive()
    }
}

impl Drop for SessionValidator {
    fn drop(&mut self) {
        let _validation_was_active = self.close_validation();
        self.cancellation.cancel();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

/// Builder for [`Client`].
#[must_use]
pub struct ClientBuilder {
    credentials: AuthenticationCredentials,
    endpoints: Endpoints,
    timeout: Duration,
    response_limit: usize,
    proxy: Option<String>,
    max_retries: u32,
    retry_initial: Duration,
    retry_max: Duration,
    rate_limits: Option<RateLimitConfig>,
}

impl ClientBuilder {
    fn new(credentials: AuthenticationCredentials) -> Self {
        Self {
            credentials,
            endpoints: Endpoints::default(),
            timeout: DEFAULT_TIMEOUT,
            response_limit: DEFAULT_RESPONSE_LIMIT,
            proxy: None,
            max_retries: DEFAULT_MAX_RETRIES,
            retry_initial: DEFAULT_RETRY_INITIAL,
            retry_max: DEFAULT_RETRY_MAX,
            rate_limits: Some(RateLimitConfig::default()),
        }
    }

    /// Uses custom provider endpoints.
    pub fn endpoints(mut self, endpoints: Endpoints) -> Self {
        self.endpoints = endpoints;
        self
    }

    /// Sets the per-request timeout.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Sets the maximum decoded HTTP response size.
    pub fn response_limit(mut self, bytes: usize) -> Self {
        self.response_limit = bytes;
        self
    }

    /// Routes HTTP requests through the supplied proxy URL.
    ///
    /// Ambient process proxy variables are deliberately ignored. This method
    /// is the only way to enable proxying for REST and real-time traffic.
    pub fn proxy(mut self, proxy: impl Into<String>) -> Self {
        self.proxy = Some(proxy.into());
        self
    }

    /// Sets the number of retries for read/query requests.
    ///
    /// Money-moving mutations are never retried.
    pub fn max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Sets the initial and maximum exponential retry delays.
    pub fn retry_delays(mut self, initial: Duration, maximum: Duration) -> Self {
        self.retry_initial = initial;
        self.retry_max = maximum;
        self
    }

    /// Replaces the default `ProjectX` REST rate limits.
    ///
    /// The configured budgets are shared by the built [`Client`] and all of
    /// its clones.
    pub fn rate_limits(mut self, rate_limits: RateLimitConfig) -> Self {
        self.rate_limits = Some(rate_limits);
        self
    }

    /// Disables local REST rate limiting.
    ///
    /// Use this only when an external coordinator enforces the provider limits
    /// across every client and process using the same credentials. Provider
    /// HTTP 429 responses still participate in query retry delays.
    pub fn disable_rate_limits(mut self) -> Self {
        self.rate_limits = None;
        self
    }

    /// Builds the client without performing network I/O.
    ///
    /// # Errors
    ///
    /// Returns an error for zero timeout/response limits, an invalid or unsafe
    /// proxy combination, or a transport configuration failure.
    pub fn build(self) -> Result<Client, Error> {
        if self.timeout.is_zero()
            || self.response_limit == 0
            || self.retry_initial.is_zero()
            || self.retry_max < self.retry_initial
        {
            return Err(Error::Configuration(
                "timeout, response limit, and retry delays must be valid and non-zero".to_owned(),
            ));
        }
        let now = tokio::time::Instant::now();
        if [self.timeout, self.retry_initial, self.retry_max]
            .into_iter()
            .any(|duration| now.checked_add(duration).is_none())
        {
            return Err(Error::Configuration(
                "timeout and retry delays must be representable by the Tokio clock".to_owned(),
            ));
        }
        if self.proxy.is_some() && self.endpoints.uses_plaintext_transport() {
            return Err(Error::Configuration(
                "a proxy cannot be combined with plain-HTTP loopback endpoints".to_owned(),
            ));
        }
        let mut headers = header::HeaderMap::new();
        headers.insert(
            header::USER_AGENT,
            header::HeaderValue::from_static(USER_AGENT),
        );
        headers.insert(
            header::ACCEPT,
            header::HeaderValue::from_static("text/plain"),
        );
        headers.insert(
            header::CONTENT_TYPE,
            header::HeaderValue::from_static("application/json"),
        );

        let mut builder = reqwest::Client::builder()
            .no_proxy()
            .retry(reqwest::retry::never())
            .default_headers(headers)
            .timeout(self.timeout)
            .redirect(Policy::none());
        let mut realtime_builder = reqwest::Client::builder()
            .no_proxy()
            .retry(reqwest::retry::never())
            .http1_only()
            .timeout(self.timeout)
            .redirect(Policy::none());
        if let Some(proxy) = self.proxy {
            let proxy = Proxy::all(proxy).map_err(Error::Transport)?;
            builder = builder.proxy(proxy.clone());
            realtime_builder = realtime_builder.proxy(proxy);
        }
        let http = builder.build().map_err(Error::Transport)?;
        let realtime_http = realtime_builder.build().map_err(Error::Transport)?;
        Ok(Client {
            credentials: Arc::new(self.credentials),
            endpoints: self.endpoints,
            http,
            realtime_http,
            token: Arc::new(TokenStore::default()),
            rate_limits: Arc::new(RateLimits::new(self.rate_limits)),
            response_limit: self.response_limit,
            max_retries: self.max_retries,
            retry_initial: self.retry_initial,
            retry_max: self.retry_max,
        })
    }
}

fn accepted<T>(response: Envelope<T>) -> Result<T, Error> {
    match response {
        Envelope::Accepted(body) => Ok(body),
        Envelope::Rejected { error_code } => Err(ProviderError { code: error_code }.into()),
        Envelope::InconsistentStatus {
            success,
            error_code,
        } => Err(Error::InconsistentResponseStatus {
            success,
            code: error_code,
        }),
    }
}

fn validate_token(raw: Option<&str>) -> Result<String, Error> {
    let token = raw
        .filter(|value| !value.is_empty())
        .ok_or(Error::MissingAuthenticationToken)?;
    if !token.bytes().all(|byte| byte.is_ascii_graphic()) {
        return Err(Error::InvalidAuthenticationToken);
    }
    header::HeaderValue::from_str(token).map_err(|_| Error::InvalidAuthenticationToken)?;
    Ok(token.to_owned())
}

fn finish_trustworthy_validation(
    store: &TokenStore,
    mut attempt: ValidationAttempt,
    new_token: Option<String>,
) -> Result<Option<UpdateOutcome>, Error> {
    if !attempt.claim_trustworthy_completion() {
        return Err(Error::AmbiguousSessionValidation);
    }
    let outcome = new_token.map(|token| store.rotate_if_current(attempt.basis(), token));
    attempt.disarm();
    Ok(outcome)
}

fn should_retry(error: &Error) -> bool {
    match error {
        Error::Transport(error) => error.is_timeout() || error.is_connect(),
        Error::ProviderRateLimited { .. } => true,
        Error::UnexpectedStatus { status } => *status == 429 || *status >= 500,
        _ => false,
    }
}

fn is_terminal_session_error(error: &Error) -> bool {
    matches!(
        error,
        Error::NotAuthenticated
            | Error::MissingAuthenticationToken
            | Error::InvalidAuthenticationToken
            | Error::AmbiguousSessionValidation
            | Error::ResponseTooLarge { .. }
            | Error::Decode(_)
            | Error::InconsistentResponseStatus { .. }
            | Error::UnexpectedStatus { status: 401 }
            | Error::SessionValidationRejected { code: 1..=3 }
    )
}

fn retry_delay(error: &Error, backoff: Duration) -> Duration {
    match error {
        Error::ProviderRateLimited { retry_after, .. } => backoff.max(*retry_after),
        _ => backoff,
    }
}

fn parse_retry_after(headers: &header::HeaderMap, now: SystemTime) -> Option<Duration> {
    let raw = headers.get(header::RETRY_AFTER)?.to_str().ok()?.trim();
    if let Ok(seconds) = raw.parse::<u64>() {
        return Some(Duration::from_secs(seconds).min(MAX_SERVER_RETRY_AFTER));
    }
    let deadline = httpdate::parse_http_date(raw).ok()?;
    Some(
        deadline
            .duration_since(now)
            .unwrap_or(Duration::ZERO)
            .min(MAX_SERVER_RETRY_AFTER),
    )
}

#[derive(Clone, Copy, Debug)]
enum MutationKind {
    OrderPlacement,
    OrderCancellation,
    OrderModification,
    PositionClose,
    PartialPositionClose,
}

impl MutationKind {
    const fn operation(self) -> &'static str {
        match self {
            Self::OrderPlacement => "order placement",
            Self::OrderCancellation => "order cancellation",
            Self::OrderModification => "order modification",
            Self::PositionClose => "position close",
            Self::PartialPositionClose => "partial position close",
        }
    }

    const fn path(self) -> &'static str {
        match self {
            Self::OrderPlacement => "api/Order/place",
            Self::OrderCancellation => "api/Order/cancel",
            Self::OrderModification => "api/Order/modify",
            Self::PositionClose => "api/Position/closeContract",
            Self::PartialPositionClose => "api/Position/partialCloseContract",
        }
    }

    const fn is_definitive_rejection(self, code: i32) -> bool {
        // These are the endpoint-specific, documented rejection-only codes.
        // Pending, unknown, zero-in-a-rejection, and future codes deliberately
        // remain ambiguous because the provider may already have acted.
        match self {
            Self::OrderPlacement => matches!(code, 1..=5 | 8..=10),
            Self::OrderCancellation => matches!(code, 1..=3 | 6),
            Self::OrderModification => matches!(code, 1..=3 | 6 | 7),
            Self::PositionClose => matches!(code, 1..=5 | 8),
            Self::PartialPositionClose => matches!(code, 1..=6 | 9),
        }
    }
}

fn ambiguous_mutation(kind: MutationKind, error: Error) -> Error {
    match error {
        Error::Provider(ref provider) if kind.is_definitive_rejection(provider.code) => error,
        Error::NotAuthenticated
        | Error::UnexpectedStatus { status: 401 }
        | Error::Configuration(_)
        | Error::Encode(_)
        | Error::LocallyRateLimited { .. } => error,
        _ => Error::AmbiguousMutation {
            operation: kind.operation(),
        },
    }
}

fn validate_response_status(success: bool, code: i32) -> Result<(), Error> {
    if success == (code == 0) {
        Ok(())
    } else {
        Err(Error::InconsistentResponseStatus { success, code })
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct LoginApiKeyRequest<'a> {
    user_name: &'a str,
    api_key: &'a str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct LoginAppRequest<'a> {
    user_name: &'a str,
    password: &'a str,
    device_id: &'a str,
    app_id: &'a str,
    verify_key: &'a str,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct LoginResponse {
    success: bool,
    error_code: i32,
    token: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ValidateResponse {
    success: bool,
    error_code: i32,
    new_token: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AccountSearchRequest {
    only_active_accounts: bool,
}

#[derive(Serialize)]
struct AvailableContractsRequest {
    live: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AccountRequest {
    account_id: AccountId,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AccountOrderRequest {
    account_id: AccountId,
    order_id: OrderId,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ContractRequest<'a> {
    contract_id: &'a crate::ContractId,
}

#[cfg(test)]
mod tests {
    use std::time::UNIX_EPOCH;

    use super::*;

    fn fixture_credentials() -> Credentials {
        Credentials::new("synthetic-user", "synthetic-key")
            .unwrap_or_else(|error| panic!("fixture credentials must be valid: {error}"))
    }

    #[test]
    fn builder_rejects_durations_outside_the_tokio_clock_range() {
        let timeout = Client::builder(fixture_credentials())
            .timeout(Duration::MAX)
            .build();
        assert!(matches!(timeout, Err(Error::Configuration(_))));

        let retry = Client::builder(fixture_credentials())
            .retry_delays(Duration::from_secs(1), Duration::MAX)
            .build();
        assert!(matches!(retry, Err(Error::Configuration(_))));
    }

    #[test]
    fn builder_rejects_a_proxy_for_plaintext_loopback_endpoints() {
        let endpoints = Endpoints::custom("http://127.0.0.1:8080", "http://[::1]:8080")
            .unwrap_or_else(|error| panic!("fixture endpoints must be valid: {error}"));
        let client = Client::builder(fixture_credentials())
            .endpoints(endpoints)
            .proxy("http://127.0.0.1:8888")
            .build();

        assert!(matches!(client, Err(Error::Configuration(_))));
    }

    #[test]
    fn bearer_token_validation_rejects_whitespace_and_control_bytes() {
        assert!(matches!(
            validate_token(Some("token with spaces")),
            Err(Error::InvalidAuthenticationToken)
        ));
        assert!(matches!(
            validate_token(Some("token\n")),
            Err(Error::InvalidAuthenticationToken)
        ));
        assert!(matches!(
            validate_token(Some("")),
            Err(Error::MissingAuthenticationToken)
        ));
        assert_eq!(
            validate_token(Some("synthetic.jwt-token_123"))
                .unwrap_or_else(|error| panic!("fixture token must be valid: {error}")),
            "synthetic.jwt-token_123"
        );
    }

    #[test]
    fn validator_shutdown_and_trustworthy_completion_have_one_atomic_winner() {
        let losing_store = Arc::new(TokenStore::default());
        assert!(
            losing_store
                .begin_authentication()
                .commit("initial-token".to_owned())
        );
        let losing_tracker = Arc::new(ValidationTracker::default());
        let losing_basis = losing_store
            .versioned_snapshot()
            .unwrap_or_else(|| panic!("fixture token must be installed"));
        let losing_attempt = ValidationAttempt::new(
            Arc::clone(&losing_store),
            losing_basis,
            Some(&losing_tracker),
        )
        .unwrap_or_else(|| panic!("fixture validation must be admitted"));

        let closing_revision = losing_tracker
            .close()
            .unwrap_or_else(|| panic!("shutdown must claim the admitted validation"));
        let losing_completion = finish_trustworthy_validation(
            &losing_store,
            losing_attempt,
            Some("rotated-token".to_owned()),
        );
        losing_store.invalidate_revision_if_current(closing_revision);

        assert!(matches!(
            losing_completion,
            Err(Error::AmbiguousSessionValidation)
        ));
        assert!(!losing_store.is_authenticated());

        let winning_store = Arc::new(TokenStore::default());
        assert!(
            winning_store
                .begin_authentication()
                .commit("initial-token".to_owned())
        );
        let winning_tracker = Arc::new(ValidationTracker::default());
        let winning_basis = winning_store
            .versioned_snapshot()
            .unwrap_or_else(|| panic!("fixture token must be installed"));
        let winning_attempt = ValidationAttempt::new(
            Arc::clone(&winning_store),
            winning_basis,
            Some(&winning_tracker),
        )
        .unwrap_or_else(|| panic!("fixture validation must be admitted"));

        let winning_completion = finish_trustworthy_validation(
            &winning_store,
            winning_attempt,
            Some("rotated-token".to_owned()),
        )
        .unwrap_or_else(|error| panic!("trustworthy completion must win: {error}"));

        assert_eq!(winning_completion, Some(UpdateOutcome::Applied));
        assert_eq!(winning_tracker.close(), None);
        assert_eq!(winning_store.snapshot().as_deref(), Some("rotated-token"));
    }

    #[test]
    fn retry_after_parses_delta_seconds_and_bounds_hostile_values() {
        let mut headers = header::HeaderMap::new();
        headers.insert(header::RETRY_AFTER, header::HeaderValue::from_static("45"));
        assert_eq!(
            parse_retry_after(&headers, UNIX_EPOCH),
            Some(Duration::from_secs(45))
        );

        headers.insert(
            header::RETRY_AFTER,
            header::HeaderValue::from_static("18446744073709551615"),
        );
        assert_eq!(
            parse_retry_after(&headers, UNIX_EPOCH),
            Some(MAX_SERVER_RETRY_AFTER)
        );
    }

    #[test]
    fn retry_after_parses_http_dates_without_waiting_for_past_dates() {
        let now = UNIX_EPOCH + Duration::from_secs(1_000_000);
        let future = now + Duration::from_secs(75);
        let mut headers = header::HeaderMap::new();
        let future_header = header::HeaderValue::from_str(&httpdate::fmt_http_date(future))
            .unwrap_or_else(|error| panic!("fixture header must be valid: {error}"));
        headers.insert(header::RETRY_AFTER, future_header);
        assert_eq!(
            parse_retry_after(&headers, now),
            Some(Duration::from_secs(75))
        );

        let past_header = header::HeaderValue::from_str(&httpdate::fmt_http_date(UNIX_EPOCH))
            .unwrap_or_else(|error| panic!("fixture header must be valid: {error}"));
        headers.insert(header::RETRY_AFTER, past_header);
        assert_eq!(parse_retry_after(&headers, now), Some(Duration::ZERO));
    }

    #[test]
    fn retry_after_rejects_malformed_headers() {
        let mut headers = header::HeaderMap::new();
        headers.insert(
            header::RETRY_AFTER,
            header::HeaderValue::from_static("not-a-delay"),
        );
        assert_eq!(parse_retry_after(&headers, UNIX_EPOCH), None);
    }

    #[test]
    fn provider_retry_delay_uses_the_longer_value_without_adding_delays() {
        let provider_delay = Error::ProviderRateLimited {
            kind: RateLimitKind::General,
            retry_after: Duration::from_secs(20),
        };
        assert_eq!(
            retry_delay(&provider_delay, Duration::from_secs(5)),
            Duration::from_secs(20)
        );
        assert_eq!(
            retry_delay(&provider_delay, Duration::from_secs(30)),
            Duration::from_secs(30)
        );
    }

    #[test]
    fn mutation_policies_whitelist_only_documented_definitive_rejections() {
        let cases: &[(MutationKind, &[i32], &[i32])] = &[
            (
                MutationKind::OrderPlacement,
                &[1, 2, 3, 4, 5, 8, 9, 10],
                &[0, 6, 7, 11, 99],
            ),
            (
                MutationKind::OrderCancellation,
                &[1, 2, 3, 6],
                &[0, 4, 5, 7, 99],
            ),
            (
                MutationKind::OrderModification,
                &[1, 2, 3, 6, 7],
                &[0, 4, 5, 8, 99],
            ),
            (
                MutationKind::PositionClose,
                &[1, 2, 3, 4, 5, 8],
                &[0, 6, 7, 9, 99],
            ),
            (
                MutationKind::PartialPositionClose,
                &[1, 2, 3, 4, 5, 6, 9],
                &[0, 7, 8, 10, 99],
            ),
        ];

        for &(kind, definitive, ambiguous) in cases {
            for &code in definitive {
                assert!(
                    kind.is_definitive_rejection(code),
                    "{kind:?} code {code} must be definitive"
                );
            }
            for &code in ambiguous {
                assert!(
                    !kind.is_definitive_rejection(code),
                    "{kind:?} code {code} must be ambiguous"
                );
            }
        }
    }
}
