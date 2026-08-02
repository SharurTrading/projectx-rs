// SPDX-FileCopyrightText: 2026 Kevin Monaghan
// SPDX-License-Identifier: MIT

//! Secret credential ownership.

use std::fmt;

use secrecy::{ExposeSecret, SecretString};

use crate::Error;

/// `ProjectX` API-key credentials.
///
/// Debug output is always redacted. The crate exposes no public secret getters.
pub struct Credentials {
    user_name: SecretString,
    api_key: SecretString,
}

impl Credentials {
    /// Creates credentials from a `ProjectX` username and API key.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Configuration`] when either value is empty or padded
    /// with whitespace.
    pub fn new(user_name: impl Into<String>, api_key: impl Into<String>) -> Result<Self, Error> {
        let user_name = user_name.into();
        let api_key = api_key.into();
        validate_secret("username", &user_name)?;
        validate_secret("API key", &api_key)?;
        Ok(Self {
            user_name: SecretString::from(user_name),
            api_key: SecretString::from(api_key),
        })
    }

    pub(crate) fn expose_user_name(&self) -> &str {
        self.user_name.expose_secret()
    }

    pub(crate) fn expose_api_key(&self) -> &str {
        self.api_key.expose_secret()
    }
}

impl fmt::Debug for Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Credentials")
            .field("user_name", &"[REDACTED]")
            .field("api_key", &"[REDACTED]")
            .finish()
    }
}

fn validate_secret(name: &str, value: &str) -> Result<(), Error> {
    if value.is_empty() || value.trim() != value {
        return Err(Error::Configuration(format!(
            "{name} must be non-empty and must not contain surrounding whitespace"
        )));
    }
    Ok(())
}
