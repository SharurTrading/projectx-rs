// SPDX-FileCopyrightText: 2026 Kevin Monaghan
// SPDX-License-Identifier: MIT

//! Provider endpoint configuration.

use url::Url;

use crate::Error;

/// REST and real-time endpoints for one `ProjectX` deployment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Endpoints {
    api_base: String,
    realtime_base: String,
}

impl Endpoints {
    /// Returns the hosted `TopstepX` endpoints.
    pub fn topstepx() -> Self {
        Self {
            api_base: "https://api.topstepx.com/".to_owned(),
            realtime_base: "https://rtc.topstepx.com/".to_owned(),
        }
    }

    /// Creates a custom `ProjectX` endpoint pair.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid URLs or URLs that cannot serve as a base.
    pub fn custom(api_base: &str, realtime_base: &str) -> Result<Self, Error> {
        Ok(Self {
            api_base: parse_base(api_base)?,
            realtime_base: parse_base(realtime_base)?,
        })
    }

    pub(crate) fn api_url(&self, path: &str) -> Result<Url, Error> {
        Url::parse(&self.api_base)
            .map_err(Error::Url)?
            .join(path.trim_start_matches('/'))
            .map_err(Error::Url)
    }

    pub(crate) fn hub_url(&self, hub_path: &str) -> Result<Url, Error> {
        let mut url = Url::parse(&self.realtime_base).map_err(Error::Url)?;
        let scheme = match url.scheme() {
            "https" => "wss",
            "http" => "ws",
            _ => {
                return Err(Error::Configuration(
                    "real-time endpoint must use HTTP or HTTPS".to_owned(),
                ));
            }
        };
        url.set_scheme(scheme).map_err(|()| {
            Error::Configuration("real-time endpoint scheme could not be changed".to_owned())
        })?;
        url.join(&format!("hubs/{hub_path}")).map_err(Error::Url)
    }

    /// Returns the configured REST base URL.
    pub fn api_base(&self) -> &str {
        &self.api_base
    }

    /// Returns the configured real-time base URL.
    pub fn realtime_base(&self) -> &str {
        &self.realtime_base
    }
}

impl Default for Endpoints {
    fn default() -> Self {
        Self::topstepx()
    }
}

fn parse_base(raw: &str) -> Result<String, Error> {
    let mut url = Url::parse(raw).map_err(Error::Url)?;
    if url.cannot_be_a_base() || !matches!(url.scheme(), "http" | "https") {
        return Err(Error::Configuration(
            "endpoint must be an absolute HTTP(S) base URL".to_owned(),
        ));
    }
    if !url.path().ends_with('/') {
        let new_path = format!("{}/", url.path());
        url.set_path(&new_path);
    }
    Ok(url.into())
}
