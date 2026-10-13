// SPDX-FileCopyrightText: 2026 Kevin Monaghan
// SPDX-License-Identifier: MIT-0

//! Enables, changes and releases shared exit capacity without making requests.

use projectx_client::{Client, Credentials, Error, RateLimitConfig};

fn configured_client(credentials: Credentials, reserve_requests: usize) -> Result<Client, Error> {
    let limits = RateLimitConfig::default().with_exit_reserve(reserve_requests)?;
    Client::builder(credentials).rate_limits(limits).build()
}

fn main() -> Result<(), Error> {
    let credentials = Credentials::new("fixture-user", "fixture-key")?;
    let client = configured_client(credentials, 20)?;
    let reads = client.clone();
    reads.set_exit_reserve(40)?;
    client.set_exit_reserve(0)?;
    client.set_exit_reserve(20)?;
    match client.set_exit_reserve(200) {
        Err(Error::Configuration(_)) => Ok(()),
        _ => Err(Error::Configuration(
            "full-window reserve was accepted".into(),
        )),
    }
}
