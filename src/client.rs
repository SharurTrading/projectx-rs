//! Authenticated REST client.

use std::{fmt, sync::Arc, time::Duration};

use futures_util::StreamExt as _;
use reqwest::{Method, Proxy, header};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::{
    Account, AccountId, Bar, CancelOrder, CloseContract, Contract, Credentials, Endpoints, Error,
    HistoryRequest, ModifyOrder, OperationResponse, Order, OrderResponse, OrderSearch, PlaceOrder,
    Position, ProviderError, SearchContracts, Trade, TradeSearch,
    models::{
        AccountsBody, BarsBody, ContractsBody, EmptyBody, Envelope, OrdersBody, PlaceOrderBody,
        PositionsBody, TradesBody,
    },
    token::TokenStore,
};

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(60);
const DEFAULT_RESPONSE_LIMIT: usize = 16 * 1024 * 1024;
const USER_AGENT: &str = "projectx-client/0.1.0";

/// Authenticated `ProjectX` REST client.
///
/// The client uses the caller's Tokio runtime and keeps its bearer token
/// private. Call [`Client::authenticate`] before calling authenticated methods.
pub struct Client {
    credentials: Credentials,
    endpoints: Endpoints,
    http: reqwest::Client,
    token: Arc<TokenStore>,
    response_limit: usize,
}

impl Client {
    /// Starts configuring a client with explicit credentials.
    pub fn builder(credentials: Credentials) -> ClientBuilder {
        ClientBuilder::new(credentials)
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

    /// Validates the current bearer token and applies provider token rotation.
    ///
    /// # Errors
    ///
    /// Returns an error when unauthenticated, transport fails, or validation is
    /// rejected.
    pub async fn validate_session(&self) -> Result<(), Error> {
        let response: ValidateResponse = self
            .post_authenticated("api/Auth/validate", &EmptyRequest {})
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
            .post_authenticated("api/Contract/search", request)
            .await?;
        Ok(accepted(response)?.contracts)
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
            .post_authenticated("api/History/retrieveBars", request)
            .await?;
        Ok(accepted(response)?.bars)
    }

    /// Searches historical orders for an account and time range.
    ///
    /// # Errors
    ///
    /// Returns an error for authentication, transport, provider, or decode failures.
    pub async fn search_orders(&self, request: &OrderSearch) -> Result<Vec<Order>, Error> {
        let response: Envelope<OrdersBody> =
            self.post_authenticated("api/Order/search", request).await?;
        Ok(accepted(response)?.orders)
    }

    /// Searches currently open orders for an account.
    ///
    /// # Errors
    ///
    /// Returns an error for authentication, transport, provider, or decode failures.
    pub async fn search_open_orders(&self, account_id: AccountId) -> Result<Vec<Order>, Error> {
        let response: Envelope<OrdersBody> = self
            .post_authenticated("api/Order/searchOpen", &AccountRequest { account_id })
            .await?;
        Ok(accepted(response)?.orders)
    }

    /// Places an order exactly once.
    ///
    /// This method never retries. A transport failure is returned as
    /// [`Error::AmbiguousOrderOutcome`]; callers must reconcile open and recent
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
        let response: Envelope<PlaceOrderBody> =
            match self.post_authenticated("api/Order/place", request).await {
                Err(Error::Transport(source)) => return Err(Error::AmbiguousOrderOutcome(source)),
                result => result?,
            };
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
        self.operation("api/Order/cancel", request).await
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
        self.operation("api/Order/modify", request).await
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
            .post_authenticated("api/Position/searchOpen", &AccountRequest { account_id })
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
        self.operation("api/Position/closeContract", request).await
    }

    /// Searches executions for an account and time range.
    ///
    /// # Errors
    ///
    /// Returns an error for authentication, transport, provider, or decode failures.
    pub async fn search_trades(&self, request: &TradeSearch) -> Result<Vec<Trade>, Error> {
        let response: Envelope<TradesBody> =
            self.post_authenticated("api/Trade/search", request).await?;
        Ok(accepted(response)?.trades)
    }

    async fn operation<T>(&self, path: &str, request: &T) -> Result<OperationResponse, Error>
    where
        T: Serialize + ?Sized,
    {
        let response: Envelope<EmptyBody> = self.post_authenticated(path, request).await?;
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

    async fn post_authenticated<T, R>(&self, path: &str, body: &T) -> Result<R, Error>
    where
        T: Serialize + ?Sized,
        R: DeserializeOwned,
    {
        let token = self.token.snapshot().await.ok_or(Error::NotAuthenticated)?;
        let url = self.endpoints.api_url(path)?;
        let response = self
            .http
            .request(Method::POST, url)
            .bearer_auth(token)
            .json(body)
            .send()
            .await
            .map_err(Error::Transport)?;
        self.decode(response).await
    }

    async fn decode<R>(&self, response: reqwest::Response) -> Result<R, Error>
    where
        R: DeserializeOwned,
    {
        let response = response.error_for_status().map_err(Error::Transport)?;
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
            .finish_non_exhaustive()
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
}

impl ClientBuilder {
    fn new(credentials: Credentials) -> Self {
        Self {
            credentials,
            endpoints: Endpoints::default(),
            timeout: DEFAULT_TIMEOUT,
            response_limit: DEFAULT_RESPONSE_LIMIT,
            proxy: None,
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

    /// Builds the client without performing network I/O.
    ///
    /// # Errors
    ///
    /// Returns an error for zero timeout/response limits, an invalid proxy, or
    /// a transport configuration failure.
    pub fn build(self) -> Result<Client, Error> {
        if self.timeout.is_zero() || self.response_limit == 0 {
            return Err(Error::Configuration(
                "timeout and response limit must be non-zero".to_owned(),
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
            .timeout(self.timeout);
        if let Some(proxy) = self.proxy {
            builder = builder.proxy(Proxy::all(proxy).map_err(Error::Transport)?);
        }
        let http = builder.build().map_err(Error::Transport)?;
        Ok(Client {
            credentials: self.credentials,
            endpoints: self.endpoints,
            http,
            token: Arc::new(TokenStore::default()),
            response_limit: self.response_limit,
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
struct EmptyRequest {}
