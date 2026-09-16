// SPDX-FileCopyrightText: 2026 Kevin Monaghan
// SPDX-License-Identifier: MIT-0

//! Error types.

use std::{fmt, time::Duration};

use thiserror::Error;

use crate::RateLimitKind;

/// A provider-declared failed operation.
///
/// The provider names every code it publishes, and the same number means
/// different things to different endpoints: code `2` is `OrderRejected` for
/// `/api/Order/place` and `OrderNotFound` for `/api/Order/cancel`. [`Self::name`]
/// carries the published text for the endpoint that produced the rejection.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
#[error("ProjectX rejected the operation ({})", CodeText { code: *code, name: *name })]
pub struct ProviderError {
    /// Provider error code.
    pub code: i32,
    /// Provider-published name for [`Self::code`], when the endpoint's
    /// published error-code table defines it.
    ///
    /// Undocumented codes, including codes the provider adds after this crate
    /// was published, are `None`. The provider's free-form `errorMessage` is
    /// untrusted remote text and is never exposed here.
    pub name: Option<&'static str>,
}

/// Renders a provider error code together with its published name.
#[derive(Clone, Copy, Debug)]
struct CodeText {
    code: i32,
    name: Option<&'static str>,
}

impl fmt::Display for CodeText {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.name {
            Some(name) => write!(formatter, "code: {} {name}", self.code),
            None => write!(formatter, "code: {}", self.code),
        }
    }
}

/// Renders the provider code of an ambiguous outcome, or nothing when the
/// outcome never produced a decodable provider response.
#[derive(Clone, Copy, Debug)]
struct AmbiguousCodeText(Option<CodeText>);

impl fmt::Display for AmbiguousCodeText {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            Some(code) => write!(formatter, " ({code})"),
            None => Ok(()),
        }
    }
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
    /// The provider rejected the supplied authentication credentials.
    #[error("provider rejected the credentials ({})", CodeText { code: *code, name: *name })]
    CredentialsRejected {
        /// Public provider rejection code.
        code: i32,
        /// Provider-published name for `code`, when `/api/Auth/loginKey` and
        /// `/api/Auth/loginApp` publish one for it.
        name: Option<&'static str>,
    },
    /// The provider rejected validation of the current session.
    #[error(
        "provider rejected session validation ({})",
        CodeText { code: *code, name: *name }
    )]
    SessionValidationRejected {
        /// Public provider rejection code.
        code: i32,
        /// Provider-published name for `code`, when `/api/Auth/validate`
        /// publishes one for it.
        name: Option<&'static str>,
    },
    /// The provider's success flag and required error code disagreed.
    ///
    /// The provider did not declare a rejection, so this reports the
    /// contradictory pair as received instead of naming a rejection code.
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
    /// The provider status endpoint returned a body other than `pong`.
    #[error("provider status endpoint returned an unexpected response")]
    UnexpectedStatusResponse,
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
    ///
    /// A decoded provider rejection that is documented as pending, unknown, or
    /// otherwise not a definitive rejection stays ambiguous, and carries the
    /// provider's code and published name so the caller can tell
    /// `OrderPending` apart from an unrecognized future code.
    #[error(
        "{} outcome is ambiguous{}; reconcile provider state before retrying",
        operation,
        AmbiguousCodeText(code.map(|code| CodeText { code, name: *name }))
    )]
    AmbiguousMutation {
        /// Public-safe operation name.
        operation: &'static str,
        /// Provider error code, when a provider rejection was decoded.
        code: Option<i32>,
        /// Provider-published name for `code`, when the endpoint's published
        /// error-code table defines it.
        name: Option<&'static str>,
    },
    /// A library-owned background task terminated unexpectedly.
    #[error("{task} background task terminated unexpectedly")]
    BackgroundTaskFailed {
        /// Public-safe task name.
        task: &'static str,
    },
}
