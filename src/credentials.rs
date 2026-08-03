// SPDX-FileCopyrightText: 2026 Kevin Monaghan
// SPDX-License-Identifier: MIT

//! Secret credential ownership.

use std::fmt;

use secrecy::{ExposeSecret, SecretString};

use crate::Error;

/// Credentials accepted by one of the provider's authentication endpoints.
pub(crate) enum AuthenticationCredentials {
    /// API-key credentials sent only to `/api/Auth/loginKey`.
    ApiKey(Credentials),
    /// Authorized-application credentials sent only to `/api/Auth/loginApp`.
    Application(ApplicationCredentials),
}

impl fmt::Debug for AuthenticationCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ApiKey(credentials) => credentials.fmt(f),
            Self::Application(credentials) => credentials.fmt(f),
        }
    }
}

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

/// `ProjectX` authorized-application credentials.
///
/// Construct these credentials with [`ApplicationCredentials::builder`]. All
/// five values are retained as secrets, Debug output is always redacted, and
/// the crate exposes no public secret getters.
pub struct ApplicationCredentials {
    user_name: SecretString,
    password: SecretString,
    device_id: SecretString,
    app_id: SecretString,
    verify_key: SecretString,
}

impl ApplicationCredentials {
    /// Starts an authorized-application credential builder.
    pub fn builder(
        user_name: impl Into<String>,
        password: impl Into<String>,
    ) -> ApplicationCredentialsBuilder {
        ApplicationCredentialsBuilder {
            user_name: SecretString::from(user_name.into()),
            password: SecretString::from(password.into()),
            device_id: None,
            app_id: None,
            verify_key: None,
        }
    }

    pub(crate) fn expose_user_name(&self) -> &str {
        self.user_name.expose_secret()
    }

    pub(crate) fn expose_password(&self) -> &str {
        self.password.expose_secret()
    }

    pub(crate) fn expose_device_id(&self) -> &str {
        self.device_id.expose_secret()
    }

    pub(crate) fn expose_app_id(&self) -> &str {
        self.app_id.expose_secret()
    }

    pub(crate) fn expose_verify_key(&self) -> &str {
        self.verify_key.expose_secret()
    }
}

impl fmt::Debug for ApplicationCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ApplicationCredentials")
            .field("user_name", &"[REDACTED]")
            .field("password", &"[REDACTED]")
            .field("device_id", &"[REDACTED]")
            .field("app_id", &"[REDACTED]")
            .field("verify_key", &"[REDACTED]")
            .finish()
    }
}

/// Builder for validated [`ApplicationCredentials`].
#[must_use = "an ApplicationCredentialsBuilder does nothing until build is called"]
pub struct ApplicationCredentialsBuilder {
    user_name: SecretString,
    password: SecretString,
    device_id: Option<SecretString>,
    app_id: Option<SecretString>,
    verify_key: Option<SecretString>,
}

impl ApplicationCredentialsBuilder {
    /// Sets the provider device identifier.
    pub fn device_id(mut self, device_id: impl Into<String>) -> Self {
        self.device_id = Some(SecretString::from(device_id.into()));
        self
    }

    /// Sets the authorized application identifier.
    pub fn app_id(mut self, app_id: impl Into<String>) -> Self {
        self.app_id = Some(SecretString::from(app_id.into()));
        self
    }

    /// Sets the authorized application's verification key.
    pub fn verify_key(mut self, verify_key: impl Into<String>) -> Self {
        self.verify_key = Some(SecretString::from(verify_key.into()));
        self
    }

    /// Validates and builds the authorized-application credentials.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Configuration`] when a required value is missing,
    /// empty, or padded with whitespace.
    pub fn build(self) -> Result<ApplicationCredentials, Error> {
        let device_id = self
            .device_id
            .ok_or_else(|| Error::Configuration("application device ID is required".to_owned()))?;
        let app_id = self
            .app_id
            .ok_or_else(|| Error::Configuration("application identifier is required".to_owned()))?;
        let verify_key = self.verify_key.ok_or_else(|| {
            Error::Configuration("application verification key is required".to_owned())
        })?;
        validate_secret("username", self.user_name.expose_secret())?;
        validate_secret("password", self.password.expose_secret())?;
        validate_secret("device ID", device_id.expose_secret())?;
        validate_secret("application identifier", app_id.expose_secret())?;
        validate_secret("verification key", verify_key.expose_secret())?;
        Ok(ApplicationCredentials {
            user_name: self.user_name,
            password: self.password,
            device_id,
            app_id,
            verify_key,
        })
    }
}

impl fmt::Debug for ApplicationCredentialsBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ApplicationCredentialsBuilder")
            .field("user_name", &"[REDACTED]")
            .field("password", &"[REDACTED]")
            .field("device_id", &self.device_id.as_ref().map(|_| "[REDACTED]"))
            .field("app_id", &self.app_id.as_ref().map(|_| "[REDACTED]"))
            .field(
                "verify_key",
                &self.verify_key.as_ref().map(|_| "[REDACTED]"),
            )
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
