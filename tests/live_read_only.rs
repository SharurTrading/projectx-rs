// SPDX-FileCopyrightText: 2026 Kevin Monaghan
// SPDX-License-Identifier: MIT-0

//! Deliberate, ignored, read-only live validation.

#![cfg(feature = "live-tests")]

use std::{env, error::Error as _, time::Duration};

use projectx_client::{
    BarUnit, Client, Contract, Credentials, HistoryRequest, Hub, MarketQuote, MarketTrade,
    RealtimeClient, RealtimeError, RealtimeEvent, RealtimeEventReceiver, Timestamp,
};
use serde_json::Value;

const LIVE_REQUEST_TIMEOUT: Duration = Duration::from_secs(90);
const MARKET_EVENT_TIMEOUT: Duration = Duration::from_mins(2);
const MARKET_CLOCK_SKEW: Duration = Duration::from_secs(30);
const CONNECTED_EVENT_TIMEOUT: Duration = Duration::from_secs(5);
const HISTORY_LOOKBACK: Duration = Duration::from_hours(168);
const HISTORY_LIMIT: i32 = 200;
const MNQ_SYMBOL_ID: &str = "F.US.MNQ";

struct LiveMnqFixture {
    client: Client,
    live: bool,
    contract: Contract,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MarketEventAction {
    Continue,
    Disconnected,
    Reconnected,
}

fn configured_data_subscription() -> bool {
    match env::var("PROJECTX_LIVE_DATA") {
        Ok(value) if value == "true" => true,
        Ok(value) if value == "false" => false,
        Ok(_) => panic!("PROJECTX_LIVE_DATA must be exactly `true` or `false`"),
        Err(env::VarError::NotPresent) => {
            panic!("PROJECTX_LIVE_DATA must be supplied deliberately as `true` or `false`")
        }
        Err(env::VarError::NotUnicode(_)) => {
            panic!("PROJECTX_LIVE_DATA must contain valid Unicode")
        }
    }
}

async fn authenticated_client() -> Client {
    let user_name = env::var("PROJECTX_USERNAME")
        .unwrap_or_else(|_| panic!("PROJECTX_USERNAME must be supplied deliberately"));
    let api_key = env::var("PROJECTX_API_KEY")
        .unwrap_or_else(|_| panic!("PROJECTX_API_KEY must be supplied deliberately"));
    let credentials = Credentials::new(user_name, api_key)
        .unwrap_or_else(|error| panic!("live credential shape is invalid: {error}"));
    let client = Client::builder(credentials)
        .build()
        .unwrap_or_else(|error| panic!("live client configuration failed: {error}"));
    tokio::time::timeout(LIVE_REQUEST_TIMEOUT, client.authenticate())
        .await
        .unwrap_or_else(|_| panic!("live authentication timed out"))
        .unwrap_or_else(|error| panic!("live authentication failed: {error}"));
    client
}

async fn active_mnq_contract(client: &Client, live: bool) -> Result<Contract, String> {
    let contracts = tokio::time::timeout(LIVE_REQUEST_TIMEOUT, client.available_contracts(live))
        .await
        .map_err(|_| "selected-catalog contract discovery timed out".to_owned())?
        .map_err(|error| format!("selected-catalog contract discovery failed: {error}"))?;
    contracts
        .into_iter()
        .find(|contract| contract.active_contract && contract.symbol_id.as_str() == MNQ_SYMBOL_ID)
        .ok_or_else(|| {
            "provider returned no active MNQ contract in the selected catalog".to_owned()
        })
}

async fn live_mnq_fixture() -> LiveMnqFixture {
    let live = configured_data_subscription();
    let client = authenticated_client().await;
    let contract = active_mnq_contract(&client, live)
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    LiveMnqFixture {
        client,
        live,
        contract,
    }
}

async fn connected_market(client: &Client) -> (RealtimeClient, RealtimeEventReceiver) {
    let realtime = client.realtime(Hub::Market);
    let mut events = realtime
        .take_event_receiver()
        .unwrap_or_else(|| panic!("market event receiver must be available"));
    tokio::time::timeout(LIVE_REQUEST_TIMEOUT, realtime.connect())
        .await
        .unwrap_or_else(|_| panic!("live market connection timed out"))
        .unwrap_or_else(|error| panic!("live market handshake failed: {error}"));
    let connected = tokio::time::timeout(CONNECTED_EVENT_TIMEOUT, events.recv())
        .await
        .unwrap_or_else(|_| panic!("market hub did not publish its connected event"));
    assert!(matches!(connected, Some(RealtimeEvent::Connected)));
    (realtime, events)
}

async fn observe_quote_and_trade(
    realtime: &RealtimeClient,
    events: &mut RealtimeEventReceiver,
    contract: &Contract,
) -> Result<(MarketQuote, MarketTrade), String> {
    let mut quote = None;
    let mut trade = None;
    let mut freshness_floor =
        subscribe_with_reconnects(realtime, events, contract, &mut quote, &mut trade).await?;

    loop {
        if let (Some(quote), Some(trade)) = (quote.as_ref(), trade.as_ref()) {
            return Ok((quote.clone(), trade.clone()));
        }

        if record_market_event(
            events.recv().await,
            contract,
            freshness_floor,
            &mut quote,
            &mut trade,
        )? == MarketEventAction::Reconnected
        {
            freshness_floor =
                subscribe_with_reconnects(realtime, events, contract, &mut quote, &mut trade)
                    .await?;
        }
    }
}

async fn subscribe_with_reconnects(
    realtime: &RealtimeClient,
    events: &mut RealtimeEventReceiver,
    contract: &Contract,
    quote: &mut Option<MarketQuote>,
    trade: &mut Option<MarketTrade>,
) -> Result<Timestamp, String> {
    let mut freshness_floor = market_freshness_floor()?;
    loop {
        match subscribe_while_draining(realtime, events, contract, freshness_floor, quote, trade)
            .await?
        {
            MarketEventAction::Continue => return Ok(freshness_floor),
            MarketEventAction::Reconnected => {
                freshness_floor = market_freshness_floor()?;
            }
            MarketEventAction::Disconnected => {
                loop {
                    match record_market_event(
                        events.recv().await,
                        contract,
                        freshness_floor,
                        quote,
                        trade,
                    )? {
                        MarketEventAction::Reconnected => break,
                        MarketEventAction::Continue | MarketEventAction::Disconnected => {}
                    }
                }
                freshness_floor = market_freshness_floor()?;
            }
        }
    }
}

async fn subscribe_to_mnq(
    realtime: &RealtimeClient,
    contract: &Contract,
) -> Result<(), RealtimeError> {
    let (quote, trade) = tokio::join!(
        realtime.subscribe_contract_quotes(&contract.id),
        realtime.subscribe_contract_trades(&contract.id),
    );
    quote?;
    trade?;
    Ok(())
}

fn subscription_crossed_generation(error: &RealtimeError) -> bool {
    matches!(
        error,
        RealtimeError::InvocationSessionEnded { .. }
            | RealtimeError::InvocationTimedOut { .. }
            | RealtimeError::NotConnected
            | RealtimeError::SendClosed
    )
}

async fn subscribe_while_draining(
    realtime: &RealtimeClient,
    events: &mut RealtimeEventReceiver,
    contract: &Contract,
    freshness_floor: Timestamp,
    quote: &mut Option<MarketQuote>,
    trade: &mut Option<MarketTrade>,
) -> Result<MarketEventAction, String> {
    let subscriptions = subscribe_to_mnq(realtime, contract);
    tokio::pin!(subscriptions);

    loop {
        tokio::select! {
            event = events.recv() => {
                let action = record_market_event(
                    event,
                    contract,
                    freshness_floor,
                    quote,
                    trade,
                )?;
                if action != MarketEventAction::Continue {
                    return Ok(action);
                }
            }
            result = &mut subscriptions => {
                return match result {
                    Ok(()) => Ok(MarketEventAction::Continue),
                    Err(error) if subscription_crossed_generation(&error) => {
                        Ok(MarketEventAction::Disconnected)
                    }
                    Err(error) => Err(format!("live MNQ subscription failed: {error}")),
                }
            }
        }
    }
}

fn record_market_event(
    event: Option<RealtimeEvent>,
    contract: &Contract,
    freshness_floor: Timestamp,
    quote: &mut Option<MarketQuote>,
    trade: &mut Option<MarketTrade>,
) -> Result<MarketEventAction, String> {
    match event {
        Some(RealtimeEvent::Invocation(invocation))
            if invocation.contract_id() == Some(&contract.id) =>
        {
            match invocation.target() {
                "GatewayQuote" => {
                    let payload_shape = redacted_json_shape(invocation.entity());
                    for decoded in invocation.decode_batch::<MarketQuote>() {
                        let decoded = decoded.map_err(|error| {
                            let cause = error.source().map_or_else(
                                || "unavailable".to_owned(),
                                ToString::to_string,
                            );
                            format!(
                                "MNQ quote failed to decode: {error}; cause: {cause}; redacted payload shape: {payload_shape}"
                            )
                        })?;
                        if decoded.raw_symbol != contract.symbol_id {
                            return Err(format!(
                                "MNQ quote used unexpected symbol {}",
                                decoded.raw_symbol
                            ));
                        }
                        let event_timestamp = decoded.timestamp.unwrap_or(decoded.last_updated);
                        if event_is_fresh("quote", event_timestamp, freshness_floor)?
                            && quote_has_market_value(&decoded)
                        {
                            *quote = Some(decoded);
                        }
                    }
                }
                "GatewayTrade" => {
                    for decoded in invocation.decode_batch::<MarketTrade>() {
                        let decoded = decoded
                            .map_err(|error| format!("MNQ trade failed to decode: {error}"))?;
                        if decoded.symbol_id != contract.symbol_id {
                            return Err(format!(
                                "MNQ trade used unexpected symbol {}",
                                decoded.symbol_id
                            ));
                        }
                        if event_is_fresh("trade", decoded.timestamp, freshness_floor)? {
                            *trade = Some(decoded);
                        }
                    }
                }
                _ => {}
            }
            Ok(MarketEventAction::Continue)
        }
        Some(RealtimeEvent::Disconnected) => {
            *quote = None;
            *trade = None;
            Ok(MarketEventAction::Disconnected)
        }
        Some(RealtimeEvent::Reconnected) => {
            *quote = None;
            *trade = None;
            Ok(MarketEventAction::Reconnected)
        }
        Some(RealtimeEvent::TransportGap) => {
            Err("market event queue reported a transport gap".to_owned())
        }
        None => Err("market event stream closed unexpectedly".to_owned()),
        _ => Ok(MarketEventAction::Continue),
    }
}

const fn quote_has_market_value(quote: &MarketQuote) -> bool {
    quote.last_price.is_some()
        || quote.best_bid.is_some()
        || quote.best_ask.is_some()
        || quote.change.is_some()
        || quote.change_percent.is_some()
        || quote.open.is_some()
        || quote.high.is_some()
        || quote.low.is_some()
        || quote.volume.is_some()
}

fn redacted_json_shape(value: &Value) -> String {
    match value {
        Value::Array(values) => values.iter().find(|value| !value.is_null()).map_or_else(
            || "array<null>".to_owned(),
            |value| format!("array<{}>", redacted_json_shape(value)),
        ),
        Value::Object(fields) => {
            let fields = fields
                .iter()
                .map(|(name, value)| format!("{name}:{}", json_kind(value)))
                .collect::<Vec<_>>()
                .join(",");
            format!("object{{{fields}}}")
        }
        value => json_kind(value).to_owned(),
    }
}

const fn json_kind(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

fn market_freshness_floor() -> Result<Timestamp, String> {
    jiff::Timestamp::now()
        .checked_sub(MARKET_CLOCK_SKEW)
        .map(Timestamp::from)
        .map_err(|error| format!("market freshness floor must be representable: {error}"))
}

fn event_is_fresh(
    kind: &str,
    timestamp: Timestamp,
    freshness_floor: Timestamp,
) -> Result<bool, String> {
    let freshness_ceiling = jiff::Timestamp::now()
        .checked_add(MARKET_CLOCK_SKEW)
        .map(Timestamp::from)
        .map_err(|error| format!("market freshness ceiling must be representable: {error}"))?;
    if timestamp > freshness_ceiling {
        return Err(format!(
            "MNQ {kind} timestamp is implausibly far in the future"
        ));
    }
    Ok(timestamp >= freshness_floor)
}

#[tokio::test]
#[ignore = "requires deliberate credentials, catalog selection, and real ProjectX network I/O"]
async fn authenticates_discovers_and_handshakes_market_hub() {
    let fixture = live_mnq_fixture().await;
    let accounts = tokio::time::timeout(
        LIVE_REQUEST_TIMEOUT,
        fixture.client.search_active_accounts(),
    )
    .await
    .unwrap_or_else(|_| panic!("live active-account discovery timed out"))
    .unwrap_or_else(|error| panic!("live active-account discovery failed: {error}"));
    assert!(!accounts.is_empty(), "provider returned no active accounts");
    assert_eq!(fixture.contract.symbol_id.as_str(), MNQ_SYMBOL_ID);

    let (realtime, _events) = connected_market(&fixture.client).await;
    tokio::time::timeout(LIVE_REQUEST_TIMEOUT, realtime.disconnect())
        .await
        .unwrap_or_else(|_| panic!("live market disconnect timed out"))
        .unwrap_or_else(|error| panic!("live market disconnect failed: {error}"));
}

#[tokio::test]
#[ignore = "requires deliberate credentials, catalog selection, and real ProjectX network I/O"]
async fn downloads_recent_history_for_dynamically_discovered_mnq_contract() {
    let fixture = live_mnq_fixture().await;
    let end = jiff::Timestamp::now()
        .round(
            jiff::TimestampRound::new()
                .smallest(jiff::Unit::Hour)
                .mode(jiff::RoundMode::Floor),
        )
        .unwrap_or_else(|error| panic!("history end time must round to the hour: {error}"));
    let start = end
        .checked_sub(HISTORY_LOOKBACK)
        .unwrap_or_else(|error| panic!("history start time must be representable: {error}"));
    let start = Timestamp::from(start);
    let end = Timestamp::from(end);
    let request =
        HistoryRequest::builder(fixture.contract.id, fixture.live, start, end, BarUnit::Hour)
            .limit(HISTORY_LIMIT)
            .build()
            .unwrap_or_else(|error| panic!("live history request must be valid: {error}"));

    let bars = tokio::time::timeout(LIVE_REQUEST_TIMEOUT, fixture.client.retrieve_bars(&request))
        .await
        .unwrap_or_else(|_| panic!("live MNQ history request timed out"))
        .unwrap_or_else(|error| panic!("live MNQ history request failed: {error}"));

    assert!(!bars.is_empty(), "provider returned no recent MNQ history");
    assert!(bars.len() <= HISTORY_LIMIT as usize);
    for bar in bars {
        assert!(bar.t >= start && bar.t <= end);
        assert!(bar.l <= bar.o && bar.o <= bar.h);
        assert!(bar.l <= bar.c && bar.c <= bar.h);
        assert!(bar.v >= 0);
    }
}

#[tokio::test]
#[ignore = "requires credentials, catalog selection, MNQ entitlement, and an active market session"]
async fn streams_quotes_and_trades_for_dynamically_discovered_mnq_contract() {
    let fixture = live_mnq_fixture().await;
    let (realtime, mut events) = connected_market(&fixture.client).await;

    let observation = tokio::time::timeout(
        MARKET_EVENT_TIMEOUT,
        observe_quote_and_trade(&realtime, &mut events, &fixture.contract),
    )
    .await
    .map_err(|_| {
        "timed out waiting for fresh MNQ quote and trade events; run while MNQ is active".to_owned()
    })
    .and_then(std::convert::identity);

    let disconnect = tokio::time::timeout(LIVE_REQUEST_TIMEOUT, realtime.disconnect()).await;
    disconnect
        .unwrap_or_else(|_| panic!("live market disconnect timed out"))
        .unwrap_or_else(|error| panic!("live market disconnect failed: {error}"));
    let (quote, trade) = observation
        .unwrap_or_else(|error| panic!("live MNQ market-data observation failed: {error}"));

    assert_eq!(quote.raw_symbol.as_str(), MNQ_SYMBOL_ID);
    assert_eq!(trade.symbol_id.as_str(), MNQ_SYMBOL_ID);
}
