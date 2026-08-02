// SPDX-FileCopyrightText: 2026 Kevin Monaghan
// SPDX-License-Identifier: MIT

//! Error types.

use thiserror::Error;

/// A provider-declared failed operation.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("ProjectX rejected the operation (code: {code:?})")]
pub struct ProviderError {
    /// Optional provider error code.
    pub code: Option<i32>,
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
    /// Authentication failed without exposing secret material.
    #[error("authentication failed: {0}")]
    Authentication(String),
    /// No authenticated bearer token is available.
    #[error("the client is not authenticated")]
    NotAuthenticated,
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
}
