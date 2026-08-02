// SPDX-FileCopyrightText: 2026 Kevin Monaghan
// SPDX-License-Identifier: MIT

//! Connects to the market hub with replay and transport-gap recovery hooks.

use projectx_client::{Client, ContractId, Credentials, Hub, RealtimeEvent};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::builder(Credentials::new("user", "api-key")?).build()?;
    client.authenticate().await?;

    let realtime = client.realtime(Hub::Market);
    let mut events = realtime
        .take_event_receiver()
        .ok_or_else(|| std::io::Error::other("event receiver was already claimed"))?;
    realtime.connect().await?;

    let contract = ContractId::new("CON.F.US.MNQ.M26")?;
    realtime.subscribe_contract_trades(&contract).await?;

    while let Some(event) = events.recv().await {
        match event {
            RealtimeEvent::Reconnected => {
                // Replay the application's canonical subscription set.
                realtime.subscribe_contract_trades(&contract).await?;
            }
            RealtimeEvent::TransportGap => {
                // Mark downstream state stale and start snapshot/reconciliation.
                // Only after that recovery fence is installed may reconnect resume.
                events.acknowledge_transport_gap();
            }
            _ => {}
        }
    }

    realtime.disconnect().await?;
    Ok(())
}
