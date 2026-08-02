// SPDX-FileCopyrightText: 2026 Kevin Monaghan
// SPDX-License-Identifier: MIT

//! Authenticated REST client.

use std::{
    fmt,
    sync::Arc,
    time::{Duration, SystemTime},
};

use futures_util::StreamExt as _;
use reqwest::{Method, Proxy, header, redirect::Policy};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::{
    Account, AccountId, Bar, CancelOrder, CloseContract, Contract, Credentials, Endpoints, Error,
    HistoryRequest, Hub, ModifyOrder, OperationResponse, Order, OrderResponse, OrderSearch,
    PartialCloseContract, PlaceOrder, Position, ProviderError, RateLimitConfig, RateLimitKind,
    RealtimeClient, SearchContracts, Trade, TradeSearch,
    models::{
        AccountsBody, BarsBody, ContractBody, ContractsBody, EmptyBody, Envelope, OrdersBody,
        PlaceOrderBody, PositionsBody, TradesBody,
    },
    rate_limit::RateLimits,
    token::TokenStore,
};

const DEFAULT_TIMEOUT: Duration = Duration::from_mins(1);
const DEFAULT_RESPONSE_LIMIT: usize = 16 * 1024 * 1024;
const DEFAULT_MAX_RETRIES: u32 = 3;
const DEFAULT_RETRY_INITIAL: Duration = Duration::from_secs(1);
const DEFAULT_RETRY_MAX: Duration = Duration::from_secs(10);
const MAX_SERVER_RETRY_AFTER: Duration = Duration::from_hours(24);
const USER_AGENT: &str = "projectx-client/0.1.0";

/// Authenticated `ProjectX` REST client.
///
/// The client uses the caller's Tokio runtime and keeps its bearer token
/// private. Call [`Client::authenticate`] before calling authenticated methods.
/// Safe authenticated queries wait asynchronously for shared rate-limit
/// capacity before their HTTP timeout begins. Money-moving mutations instead
/// return [`Error::LocallyRateLimited`] without sending when capacity is full.
pub struct Client {
    credentials: Arc<Credentials>,
    endpoints: Endpoints,
    http: reqwest::Client,
    token: Arc<TokenStore>,
    rate_limits: Arc<RateLimits>,
    response_limit: usize,
    max_retries: u32,
    retry_initial: Duration,
    retry_max: Duration,
}

impl Client {
    /// Starts configuring a client with explicit credentials.
    pub fn builder(credentials: Credentials) -> ClientBuilder {
        ClientBuilder::new(credentials)
    }

    /// Creates a real-time client sharing this client's rotating bearer token.
    ///
    /// The returned hub snapshots the current token immediately before every
    /// initial connection and reconnect. Call [`Self::authenticate`] first.
    pub fn realtime(&self, hub: Hub) -> RealtimeClient {
        RealtimeClient::new(hub, self.endpoints.clone(), Arc::clone(&self.token))
    }

    /// Authenticates with `/api/Auth/loginKey` and stores the returned token.
    ///
    /// # Errors
    ///
    /// Returns an error when transport fails, credentials are rejected, or the
    /// provider omits a usable token.
    pub async fn authenticate(&self) -> Result<(), Error> {
        let body = LoginRequest {
            user_name: self.credentials.expose_user_name(),
            api_key: self.credentials.expose_api_key(),
        };
        let response: LoginResponse = self
            .post_unauthenticated("api/Auth/loginKey", &body)
            .await?;
        if !response.success {
            return Err(Error::Authentication(format!(
                "provider rejected the credentials (code: {:?})",
                response.error_code
            )));
        }
        let token = validate_token(response.token.as_deref())?;
        self.token.set(token).await;
        Ok(())
    }

    /// Authenticates and starts periodic token validation.
    ///
    /// The validator is cancelled when the returned guard is dropped. Prefer
    /// [`SessionValidator::shutdown`] when graceful task completion matters.
    ///
    /// # Errors
    ///
    /// Returns an error for a zero validation period or when authentication
    /// fails.
    pub async fn authenticate_with_validation(
        &self,
        period: Duration,
    ) -> Result<SessionValidator, Error> {
        if period.is_zero() {
            return Err(Error::Configuration(
                "validation period must be non-zero".to_owned(),
            ));
        }
        self.authenticate().await?;
        let cancellation = CancellationToken::new();
        let task_cancellation = cancellation.clone();
        let client = self.clone();
        let task = tokio::spawn(async move {
            let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
            loop {
                tokio::select! {
                    () = task_cancellation.cancelled() => break,
                    _ = ticker.tick() => {
                        tokio::select! {
                            () = task_cancellation.cancelled() => break,
                            result = client.validate_session() => {
                                if let Err(error) = result {
                                    tracing::warn!(%error, "ProjectX token validation failed");
                                }
                            }
                        }
                    }
                }
            }
        });
        Ok(SessionValidator {
            cancellation,
            task: Some(task),
        })
    }

    /// Validates the current bearer token and applies provider token rotation.
    ///
    /// # Errors
    ///
    /// Returns an error when unauthenticated, transport fails, or validation is
    /// rejected.
    pub async fn validate_session(&self) -> Result<(), Error> {
        let response: ValidateResponse = self
            .post_authenticated(
                RateLimitKind::General,
                "api/Auth/validate",
                &EmptyRequest {},
            )
            .await?;
        if !response.success {
            return Err(Error::Authentication(format!(
                "provider rejected token validation (code: {:?})",
                response.error_code
            )));
        }
        if let Some(token) = response.new_token {
            self.token.set(validate_token(Some(&token))?).await;
        }
        Ok(())
    }

    /// Retrieves active accounts for the authenticated user.
    ///
    /// # Errors
    ///
    /// Returns an error for authentication, transport, provider, or decode failures.
    pub async fn search_active_accounts(&self) -> Result<Vec<Account>, Error> {
        let response: Envelope<AccountsBody> = self
            .post_authenticated(
                RateLimitKind::General,
                "api/Account/search",
                &AccountSearchRequest {
                    only_active_accounts: true,
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
    /// Returns an error when the limit is outside `1..=20_000`, or for
    /// authentication, transport, provider, and decode failures.
    pub async fn retrieve_bars(&self, request: &HistoryRequest) -> Result<Vec<Bar>, Error> {
        if !(1..=20_000).contains(&request.limit) || request.unit_number <= 0 {
            return Err(Error::Configuration(
                "history limit must be 1..=20,000 and unit_number must be positive".to_owned(),
            ));
        }
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

    /// Searches currently open orders for an account.
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

    /// Places an order exactly once.
    ///
    /// This method never retries. An untrustworthy response is returned as
    /// [`Error::AmbiguousMutation`]; callers must reconcile open and recent
    /// orders before deciding whether another submission is safe.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid quantity, authentication, provider, decode,
    /// or ambiguous transport outcomes.
    pub async fn place_order(&self, request: &PlaceOrder) -> Result<OrderResponse, Error> {
        if request.size <= 0 {
            return Err(Error::Configuration(
                "order size must be positive".to_owned(),
            ));
        }
        let response: Envelope<PlaceOrderBody> = self
            .post_authenticated_no_retry(RateLimitKind::General, "api/Order/place", request)
            .await
            .map_err(|error| ambiguous_mutation("order placement", error))?;
        let body = accepted(response)?;
        let order_id = body.order_id.ok_or_else(|| {
            Error::Authentication("provider accepted an order without returning its ID".to_owned())
        })?;
        Ok(OrderResponse { order_id })
    }

    /// Cancels an order.
    ///
    /// # Errors
    ///
    /// Returns an error for authentication, transport, provider, or decode failures.
    pub async fn cancel_order(&self, request: &CancelOrder) -> Result<OperationResponse, Error> {
        self.mutation("order cancellation", "api/Order/cancel", request)
            .await
    }

    /// Modifies an open order.
    ///
    /// # Errors
    ///
    /// Returns an error when no replacement value is supplied, or for
    /// authentication, transport, provider, and decode failures.
    pub async fn modify_order(&self, request: &ModifyOrder) -> Result<OperationResponse, Error> {
        if request.size.is_none()
            && request.limit_price.is_none()
            && request.stop_price.is_none()
            && request.trail_price.is_none()
        {
            return Err(Error::Configuration(
                "modify order requires at least one replacement value".to_owned(),
            ));
        }
        self.mutation("order modification", "api/Order/modify", request)
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

    /// Closes the open position for an explicit account and contract.
    ///
    /// # Errors
    ///
    /// Returns an error for authentication, transport, provider, or decode failures.
    pub async fn close_contract(
        &self,
        request: &CloseContract,
    ) -> Result<OperationResponse, Error> {
        self.mutation("position close", "api/Position/closeContract", request)
            .await
    }

    /// Partially closes an open position for an account and contract.
    ///
    /// # Errors
    ///
    /// Returns an error when size is not positive, or for authentication,
    /// transport, provider, and decode failures.
    pub async fn partial_close_contract(
        &self,
        request: &PartialCloseContract,
    ) -> Result<OperationResponse, Error> {
        if request.size <= 0 {
            return Err(Error::Configuration(
                "partial close size must be positive".to_owned(),
            ));
        }
        self.mutation(
            "partial position close",
            "api/Position/partialCloseContract",
            request,
        )
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

    async fn mutation<T>(
        &self,
        operation: &'static str,
        path: &str,
        request: &T,
    ) -> Result<OperationResponse, Error>
    where
        T: Serialize + ?Sized,
    {
        let response: Envelope<EmptyBody> = self
            .post_authenticated_no_retry(RateLimitKind::General, path, request)
            .await
            .map_err(|error| ambiguous_mutation(operation, error))?;
        accepted(response)?;
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
        let encoded = serde_json::to_vec(body).map_err(Error::Encode)?;
        self.require_authentication().await?;
        let mut attempt = 0;
        let mut delay = self.retry_initial;
        loop {
            self.rate_limits.wait(kind).await;
            match self.post_authenticated_once(kind, path, &encoded).await {
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
        self.require_authentication().await?;
        self.rate_limits
            .try_acquire(kind)
            .map_err(|retry_after| Error::LocallyRateLimited { kind, retry_after })?;
        self.post_authenticated_once(kind, path, &encoded).await
    }

    async fn post_authenticated_once<R>(
        &self,
        kind: RateLimitKind,
        path: &str,
        body: &[u8],
    ) -> Result<R, Error>
    where
        R: DeserializeOwned,
    {
        let token = self.token.snapshot().await.ok_or(Error::NotAuthenticated)?;
        let url = self.endpoints.api_url(path)?;
        let response = self
            .http
            .request(Method::POST, url)
            .bearer_auth(token)
            .body(body.to_vec())
            .send()
            .await
            .map_err(Error::Transport)?;
        if response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
            let retry_after = parse_retry_after(response.headers(), SystemTime::now())
                .unwrap_or_else(|| self.rate_limits.limit(kind).window())
                .min(MAX_SERVER_RETRY_AFTER);
            self.rate_limits.cool_down(kind, retry_after);
            return Err(Error::ProviderRateLimited { kind, retry_after });
        }
        self.decode(response).await
    }

    async fn require_authentication(&self) -> Result<(), Error> {
        if self.token.is_authenticated().await {
            Ok(())
        } else {
            Err(Error::NotAuthenticated)
        }
    }

    async fn decode<R>(&self, response: reqwest::Response) -> Result<R, Error>
    where
        R: DeserializeOwned,
    {
        let status = response.status();
        if !status.is_success() {
            return Err(Error::UnexpectedStatus {
                status: status.as_u16(),
            });
        }
        if response
            .content_length()
            .is_some_and(|length| length > self.response_limit as u64)
        {
            return Err(Error::ResponseTooLarge {
                limit_bytes: self.response_limit,
            });
        }

        let mut bytes = Vec::new();
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
        serde_json::from_slice(&bytes).map_err(Error::Decode)
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
    task: Option<JoinHandle<()>>,
}

impl SessionValidator {
    /// Cancels validation and waits for its task to finish.
    pub async fn shutdown(mut self) {
        self.cancellation.cancel();
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
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
        self.cancellation.cancel();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

/// Builder for [`Client`].
#[must_use]
pub struct ClientBuilder {
    credentials: Credentials,
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
    fn new(credentials: Credentials) -> Self {
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
    /// Returns an error for zero timeout/response limits, an invalid proxy, or
    /// a transport configuration failure.
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
            .default_headers(headers)
            .timeout(self.timeout)
            .redirect(Policy::none());
        if let Some(proxy) = self.proxy {
            builder = builder.proxy(Proxy::all(proxy).map_err(Error::Transport)?);
        }
        let http = builder.build().map_err(Error::Transport)?;
        Ok(Client {
            credentials: Arc::new(self.credentials),
            endpoints: self.endpoints,
            http,
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
    if response.success {
        Ok(response.body)
    } else {
        Err(ProviderError {
            code: response.error_code,
        }
        .into())
    }
}

fn validate_token(raw: Option<&str>) -> Result<String, Error> {
    let token = raw
        .filter(|value| !value.is_empty() && value.trim() == *value)
        .ok_or_else(|| {
            Error::Authentication("provider returned success without a usable token".to_owned())
        })?;
    header::HeaderValue::from_str(&format!("Bearer {token}")).map_err(|_| {
        Error::Authentication("provider returned an invalid bearer token".to_owned())
    })?;
    Ok(token.to_owned())
}

fn should_retry(error: &Error) -> bool {
    match error {
        Error::Transport(error) => error.is_timeout() || error.is_connect(),
        Error::ProviderRateLimited { .. } => true,
        Error::UnexpectedStatus { status } => *status == 429 || *status >= 500,
        _ => false,
    }
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

fn ambiguous_mutation(operation: &'static str, error: Error) -> Error {
    match error {
        Error::Provider(_)
        | Error::NotAuthenticated
        | Error::Configuration(_)
        | Error::Encode(_)
        | Error::LocallyRateLimited { .. } => error,
        _ => Error::AmbiguousMutation { operation },
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct LoginRequest<'a> {
    user_name: &'a str,
    api_key: &'a str,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct LoginResponse {
    success: bool,
    error_code: Option<i32>,
    token: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ValidateResponse {
    success: bool,
    error_code: Option<i32>,
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
struct ContractRequest<'a> {
    contract_id: &'a crate::ContractId,
}

#[derive(Serialize)]
struct EmptyRequest {}

#[cfg(test)]
mod tests {
    use std::time::UNIX_EPOCH;

    use super::*;

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
}
