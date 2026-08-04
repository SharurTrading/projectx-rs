// SPDX-FileCopyrightText: 2026 Kevin Monaghan
// SPDX-License-Identifier: MIT-0

//! Authenticates and runs periodic revision-fenced session validation.

use std::time::Duration;

use projectx_client::{Client, Credentials};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), projectx_client::Error> {
    let client = Client::builder(Credentials::new("user", "api-key")?).build()?;
    let validator = client
        .authenticate_with_validation(Duration::from_mins(15))
        .await?;

    // REST requests and real-time hubs created from `client` share the rotating token.

    validator.shutdown().await?;
    Ok(())
}
