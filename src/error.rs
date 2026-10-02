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

/// The transport-level origin of an ambiguous mutation outcome, when a
/// provider rejection was not decoded.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum AmbiguityOrigin {
    /// The HTTP transport failed after the request may have been submitted,
    /// in a way that is neither a timeout nor a connection failure.
    Transport,
    /// The request timed out after submission; the provider may have
    /// processed it.
    TransportTimeout,
    /// The connection could not be established.
    TransportConnect,
    /// The provider answered with an HTTP status the client could not
    /// classify as a definitive outcome.
    HttpStatus(u16),
    /// A response body arrived but could not be decoded.
    Decode,
    /// A success body decoded but omitted a field required to identify the
    /// result.
    MissingResult,
    /// The response exceeded the configured size bound.
    ResponseTooLarge,
    /// The provider's success flag and required error code contradicted
    /// each other.
    InconsistentStatus,
    /// The provider refused the request with HTTP 429.
    RateLimited,
    /// The outcome was untrustworthy in a way this crate does not classify.
    /// Every ambiguous mutation this crate constructs carries a provider
    /// code or one of the classified origins; this variant reports an
    /// outcome that reached the ambiguity mapping outside those classes.
    Unclassified,
}

impl fmt::Display for AmbiguityOrigin {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transport => formatter.write_str("HTTP transport failed after submission"),
            Self::TransportTimeout => formatter.write_str("request timed out after submission"),
            Self::TransportConnect => formatter.write_str("connection could not be established"),
            Self::HttpStatus(status) => write!(formatter, "provider returned HTTP status {status}"),
            Self::Decode => formatter.write_str("response could not be decoded"),
            Self::MissingResult => {
                formatter.write_str("success response omitted a required result field")
            }
            Self::ResponseTooLarge => {
                formatter.write_str("response exceeded the configured size limit")
            }
            Self::InconsistentStatus => {
                formatter.write_str("provider returned inconsistent status fields")
            }
            Self::RateLimited => formatter.write_str("provider rate limit refused the request"),
            Self::Unclassified => formatter.write_str("unclassified transport-level failure"),
        }
    }
}

/// Renders a decoded provider rejection, or the transport-level origin when
/// none was decoded, or nothing when the outcome produced no classifiable
/// evidence.
#[derive(Clone, Copy, Debug)]
struct AmbiguityText {
    code: Option<i32>,
    name: Option<&'static str>,
    origin: Option<AmbiguityOrigin>,
}

impl fmt::Display for AmbiguityText {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(code) = self.code {
            return write!(
                formatter,
                " ({})",
                CodeText {
                    code,
                    name: self.name
                }
            );
        }
        if let Some(origin) = self.origin {
            return write!(formatter, " ({origin})");
        }
        Ok(())
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
    /// The caller refused the mutation before HTTP transport ownership transfer.
    #[error("mutation handoff refused before transport")]
    MutationHandoffRefused,
    /// An HTTP request could not be built before transport ownership transfer.
    #[error("HTTP request could not be built")]
    RequestBuild(#[source] reqwest::Error),
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
    /// `OrderPending` apart from an unrecognized future code. When no provider
    /// rejection was decoded, `origin` instead carries the transport-level
    /// evidence for the ambiguity. Exactly one of `code` and `origin` is set
    /// in every error this crate constructs: a decoded rejection is the
    /// evidence, and otherwise a classified — or explicitly
    /// [`AmbiguityOrigin::Unclassified`] — origin is.
    #[error(
        "{} outcome is ambiguous{}; reconcile provider state before retrying",
        operation,
        AmbiguityText { code: *code, name: *name, origin: *origin }
    )]
    AmbiguousMutation {
        /// Public-safe operation name.
        operation: &'static str,
        /// Provider error code, when a provider rejection was decoded.
        code: Option<i32>,
        /// Provider-published name for `code`, when the endpoint's published
        /// error-code table defines it.
        name: Option<&'static str>,
        /// Transport-level origin of the ambiguity, when no provider
        /// rejection was decoded.
        origin: Option<AmbiguityOrigin>,
    },
    /// A library-owned background task terminated unexpectedly.
    #[error("{task} background task terminated unexpectedly")]
    BackgroundTaskFailed {
        /// Public-safe task name.
        task: &'static str,
    },
}
