// SPDX-FileCopyrightText: 2026 Kevin Monaghan
// SPDX-License-Identifier: MIT

//! Error types.

use std::time::Duration;

use thiserror::Error;

use crate::RateLimitKind;

/// A provider-declared failed operation.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
#[error("ProjectX rejected the operation (code: {code})")]
pub struct ProviderError {
    /// Provider error code.
    pub code: i32,
}

/// Errors returned by the `ProjectX` client.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// Client configuration was invalid.
    #[error("invalid configuration: {0}")]
    Configuration(String),
    /// A provider identifier was invalid.
    #[error("invalid {kind}: {reason}")]
    InvalidIdentifier {
        /// Identifier category.
        kind: &'static str,
        /// Public-safe reason for rejection.
        reason: &'static str,
    },
    /// The provider rejected the supplied API-key credentials.
    #[error("provider rejected the credentials (code: {code})")]
    CredentialsRejected {
        /// Public provider rejection code.
        code: i32,
    },
    /// The provider rejected validation of the current session.
    #[error("provider rejected session validation (code: {code})")]
    SessionValidationRejected {
        /// Public provider rejection code.
        code: i32,
    },
    /// The provider's success flag and required error code disagreed.
    #[error("provider returned inconsistent status (success: {success}, code: {code})")]
    InconsistentResponseStatus {
        /// Provider success flag.
        success: bool,
        /// Provider error code.
        code: i32,
    },
    /// A successful authentication response omitted a usable bearer token.
    #[error("provider returned success without a usable authentication token")]
    MissingAuthenticationToken,
    /// A provider authentication response contained an invalid bearer token.
    #[error("provider returned an invalid authentication token")]
    InvalidAuthenticationToken,
    /// No authenticated bearer token is available.
    #[error("the client is not authenticated")]
    NotAuthenticated,
    /// Session validation may have rotated the provider token, but no
    /// trustworthy response was received.
    #[error("session-validation outcome is ambiguous; authenticate again before using the client")]
    AmbiguousSessionValidation,
    /// The provider rejected an otherwise valid request.
    #[error(transparent)]
    Provider(#[from] ProviderError),
    /// The HTTP transport failed.
    #[error("HTTP transport failed")]
    Transport(#[source] reqwest::Error),
    /// The provider returned a non-successful HTTP status.
    #[error("provider returned HTTP status {status}")]
    UnexpectedStatus {
        /// Numeric HTTP status code.
        status: u16,
    },
    /// A request was not sent because the shared local budget was exhausted.
    #[error("local {kind} rate limit is exhausted; retry after {retry_after:?}")]
    LocallyRateLimited {
        /// Provider budget that rejected local admission.
        kind: RateLimitKind,
        /// Minimum time until this client can admit another request.
        retry_after: Duration,
    },
    /// The provider rejected an authenticated request with HTTP 429.
    #[error("provider rate limit is exhausted; retry after {retry_after:?}")]
    ProviderRateLimited {
        /// Provider budget associated with the rejected endpoint.
        kind: RateLimitKind,
        /// Delay from `Retry-After`, or the configured window when absent.
        retry_after: Duration,
    },
    /// A URL could not be constructed.
    #[error("invalid endpoint URL")]
    Url(#[source] url::ParseError),
    /// A provider response exceeded the configured safety limit.
    #[error("provider response exceeded the {limit_bytes}-byte limit")]
    ResponseTooLarge {
        /// Configured response limit.
        limit_bytes: usize,
    },
    /// A provider response could not be decoded.
    #[error("provider response was not valid JSON")]
    Decode(#[source] serde_json::Error),
    /// A request could not be encoded as JSON.
    #[error("request could not be encoded as JSON")]
    Encode(#[source] serde_json::Error),
    /// A money-moving mutation may have reached the provider but did not
    /// produce a trustworthy response.
    #[error("{operation} outcome is ambiguous; reconcile provider state before retrying")]
    AmbiguousMutation {
        /// Public-safe operation name.
        operation: &'static str,
    },
    /// A library-owned background task terminated unexpectedly.
    #[error("{task} background task terminated unexpectedly")]
    BackgroundTaskFailed {
        /// Public-safe task name.
        task: &'static str,
    },
}
