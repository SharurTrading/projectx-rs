// SPDX-FileCopyrightText: 2026 Kevin Monaghan
// SPDX-License-Identifier: MIT-0

//! Authenticates and discovers the active accounts available to the user.

use projectx_client::{Client, Credentials};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), projectx_client::Error> {
    let credentials = Credentials::new("your-user-name", "your-api-key")?;
    let client = Client::builder(credentials).build()?;

    client.authenticate().await?;
    let accounts = client.search_active_accounts().await?;
    for account in accounts {
        println!("{}", account.name);
    }

    Ok(())
}
