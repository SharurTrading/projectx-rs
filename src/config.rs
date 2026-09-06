// SPDX-FileCopyrightText: 2026 Kevin Monaghan
// SPDX-License-Identifier: MIT-0

//! Provider endpoint configuration.

use url::{Host, Url};

use crate::Error;

/// REST and real-time endpoints for one `ProjectX` deployment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Endpoints {
    api_base: String,
    realtime_base: String,
}

impl Endpoints {
    /// Returns the hosted `TopstepX` endpoints.
    #[must_use]
    pub fn topstepx() -> Self {
        Self {
            api_base: "https://api.topstepx.com/".to_owned(),
            realtime_base: "https://rtc.topstepx.com/".to_owned(),
        }
    }

    /// Returns the hosted `TheFuturesDesk` endpoints.
    #[must_use]
    pub fn thefuturesdesk() -> Self {
        Self {
            api_base: "https://api.thefuturesdesk.projectx.com/".to_owned(),
            realtime_base: "https://rtc.thefuturesdesk.projectx.com/".to_owned(),
        }
    }

    /// Creates a custom `ProjectX` endpoint pair.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid URLs, URLs that cannot serve as a base, or
    /// non-HTTPS remote endpoints. Plain HTTP is accepted only for exact
    /// loopback hosts so deterministic local fixtures do not weaken production
    /// credential transport.
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

    pub(crate) fn uses_plaintext_transport(&self) -> bool {
        self.api_base.starts_with("http://") || self.realtime_base.starts_with("http://")
    }

    /// Returns the configured REST base URL.
    #[must_use]
    pub fn api_base(&self) -> &str {
        &self.api_base
    }

    /// Returns the configured real-time base URL.
    #[must_use]
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
    if url.cannot_be_a_base()
        || !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
    {
        return Err(Error::Configuration(
            "endpoint must be an absolute HTTP(S) base URL with a host".to_owned(),
        ));
    }
    if url.scheme() == "http" && !has_loopback_host(&url) {
        return Err(Error::Configuration(
            "remote endpoints must use HTTPS; plain HTTP is allowed only for loopback fixtures"
                .to_owned(),
        ));
    }
    if !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(Error::Configuration(
            "endpoint base URL must not contain credentials, a query, or a fragment".to_owned(),
        ));
    }
    if !url.path().ends_with('/') {
        let new_path = format!("{}/", url.path());
        url.set_path(&new_path);
    }
    Ok(url.into())
}

fn has_loopback_host(url: &Url) -> bool {
    match url.host() {
        Some(Host::Ipv4(address)) => address.is_loopback(),
        Some(Host::Ipv6(address)) => address.is_loopback(),
        Some(Host::Domain(domain)) => domain.eq_ignore_ascii_case("localhost"),
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hosted_endpoint_presets_match_the_documented_provider_urls() {
        let topstepx = Endpoints::topstepx();
        assert_eq!(topstepx.api_base(), "https://api.topstepx.com/");
        assert_eq!(topstepx.realtime_base(), "https://rtc.topstepx.com/");
        assert_eq!(
            topstepx
                .hub_url("user")
                .unwrap_or_else(|error| panic!("fixture hub URL must be valid: {error}"))
                .as_str(),
            "wss://rtc.topstepx.com/hubs/user"
        );
        assert_eq!(
            topstepx
                .api_url("api/Auth/loginKey")
                .unwrap_or_else(|error| panic!("fixture API URL must be valid: {error}"))
                .as_str(),
            "https://api.topstepx.com/api/Auth/loginKey"
        );

        let thefuturesdesk = Endpoints::thefuturesdesk();
        assert_eq!(
            thefuturesdesk.api_base(),
            "https://api.thefuturesdesk.projectx.com/"
        );
        assert_eq!(
            thefuturesdesk.realtime_base(),
            "https://rtc.thefuturesdesk.projectx.com/"
        );
        assert_eq!(
            thefuturesdesk
                .hub_url("market")
                .unwrap_or_else(|error| panic!("fixture hub URL must be valid: {error}"))
                .as_str(),
            "wss://rtc.thefuturesdesk.projectx.com/hubs/market"
        );
        assert_eq!(
            thefuturesdesk
                .api_url("api/Auth/loginKey")
                .unwrap_or_else(|error| panic!("fixture API URL must be valid: {error}"))
                .as_str(),
            "https://api.thefuturesdesk.projectx.com/api/Auth/loginKey"
        );

        assert_eq!(Endpoints::default(), topstepx);
        assert_ne!(thefuturesdesk, topstepx);
    }

    #[test]
    fn custom_endpoints_normalize_trailing_slashes() {
        let endpoints = Endpoints::custom(
            "https://example.test/gateway",
            "https://realtime.example.test/service",
        )
        .unwrap_or_else(|error| panic!("fixture endpoints must be valid: {error}"));

        assert_eq!(endpoints.api_base(), "https://example.test/gateway/");
        assert_eq!(
            endpoints.realtime_base(),
            "https://realtime.example.test/service/"
        );
    }

    #[test]
    fn custom_endpoints_reject_ambiguous_or_secret_bases() {
        for invalid in [
            "https://user:password@example.test/",
            "https://example.test/?tenant=secret",
            "https://example.test/#fragment",
            "file:///tmp/projectx",
            "https://",
            "http://gateway.example.test/",
        ] {
            assert!(Endpoints::custom(invalid, "https://example.test/").is_err());
            assert!(Endpoints::custom("https://example.test/", invalid).is_err());
        }
    }

    #[test]
    fn custom_endpoints_allow_plain_http_only_on_exact_loopback_hosts() {
        for loopback in [
            "http://127.0.0.1:8080",
            "http://[::1]:8080",
            "http://localhost:8080",
        ] {
            Endpoints::custom(loopback, loopback)
                .unwrap_or_else(|error| panic!("loopback fixture must be accepted: {error}"));
        }

        for remote in [
            "http://127.0.0.1.example.test/",
            "http://localhost.example.test/",
            "http://192.168.1.10/",
        ] {
            assert!(Endpoints::custom(remote, "https://example.test/").is_err());
            assert!(Endpoints::custom("https://example.test/", remote).is_err());
        }
    }
}
