// SPDX-FileCopyrightText: 2026 Kevin Monaghan
// SPDX-License-Identifier: MIT-0

//! One-use HTTP mutation handoffs exercised through the public client and loopback peers.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU8, AtomicUsize, Ordering},
    },
    time::Duration,
};

use httpmock::prelude::*;
use projectx_client::{
    AccountId, AmbiguityOrigin, CancelOrder, Client, CloseContract, ContractId, Credentials,
    Endpoints, Error, ModifyOrder, MutationHandoff, OrderId, OrderType, PartialCloseContract,
    PlaceOrder, RateLimit, RateLimitConfig, Side,
};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::{TcpListener, TcpStream},
    sync::oneshot,
};

const READY: u8 = 0;
const CLAIMED: u8 = 1;
const REVOKED: u8 = 2;

#[derive(Clone, Copy)]
enum Mutation {
    Place,
    Cancel,
    Modify,
    Close,
    PartialClose,
}

const MUTATIONS: [Mutation; 5] = [
    Mutation::Place,
    Mutation::Cancel,
    Mutation::Modify,
    Mutation::Close,
    Mutation::PartialClose,
];

impl Mutation {
    fn path(self) -> &'static str {
        match self {
            Self::Place => "/api/Order/place",
            Self::Cancel => "/api/Order/cancel",
            Self::Modify => "/api/Order/modify",
            Self::Close => "/api/Position/closeContract",
            Self::PartialClose => "/api/Position/partialCloseContract",
        }
    }

    fn expected_body(self) -> Value {
        match self {
            Self::Place => {
                json!({"accountId": 42, "contractId": "SYNTHETIC.CONTRACT", "type": 2, "side": 0, "size": 3, "customTag": "synthetic-original"})
            }
            Self::Cancel => json!({"accountId": 42, "orderId": 84}),
            Self::Modify => json!({"accountId": 42, "orderId": 84, "size": 3}),
            Self::Close => json!({"accountId": 42, "contractId": "SYNTHETIC.CONTRACT"}),
            Self::PartialClose => {
                json!({"accountId": 42, "contractId": "SYNTHETIC.CONTRACT", "size": 3})
            }
        }
    }

    async fn call<F>(self, client: &Client, handoff: MutationHandoff<F>) -> Result<(), Error>
    where
        F: FnOnce() -> bool + Send,
    {
        let account_id = AccountId::new(42).unwrap_or_else(|e| panic!("synthetic account: {e}"));
        let order_id = OrderId::new(84).unwrap_or_else(|e| panic!("synthetic order: {e}"));
        let contract_id = ContractId::new("SYNTHETIC.CONTRACT")
            .unwrap_or_else(|e| panic!("synthetic contract: {e}"));
        match self {
            Self::Place => {
                let request =
                    PlaceOrder::builder(account_id, contract_id, OrderType::Market, Side::Bid, 3)
                        .custom_tag("synthetic-original")
                        .build()
                        .unwrap_or_else(|e| panic!("synthetic placement: {e}"));
                client
                    .place_order_guarded(&request, handoff)
                    .await
                    .map(|response| assert_eq!(response.order_id.get(), 84))
            }
            Self::Cancel => client
                .cancel_order_guarded(
                    &CancelOrder {
                        account_id,
                        order_id,
                    },
                    handoff,
                )
                .await
                .map(|_| ()),
            Self::Modify => {
                let request = ModifyOrder::builder(account_id, order_id)
                    .size(3)
                    .build()
                    .unwrap_or_else(|e| panic!("synthetic modification: {e}"));
                client
                    .modify_order_guarded(&request, handoff)
                    .await
                    .map(|_| ())
            }
            Self::Close => client
                .close_contract_guarded(
                    &CloseContract {
                        account_id,
                        contract_id,
                    },
                    handoff,
                )
                .await
                .map(|_| ()),
            Self::PartialClose => {
                let request = PartialCloseContract::new(account_id, contract_id, 3)
                    .unwrap_or_else(|e| panic!("synthetic partial close: {e}"));
                client
                    .partial_close_contract_guarded(&request, handoff)
                    .await
                    .map(|_| ())
            }
        }
    }
}

fn context(
    state: &Arc<AtomicU8>,
    claims: &Arc<AtomicUsize>,
) -> MutationHandoff<impl FnOnce() -> bool + Send + use<>> {
    let state = Arc::clone(state);
    let claims = Arc::clone(claims);
    MutationHandoff::new(move || {
        claims.fetch_add(1, Ordering::SeqCst);
        state
            .compare_exchange(READY, CLAIMED, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    })
}

fn fixture_client(base: &str) -> Client {
    let credentials = Credentials::new("synthetic-user", "synthetic-key")
        .unwrap_or_else(|e| panic!("synthetic credentials: {e}"));
    let endpoints =
        Endpoints::custom(base, base).unwrap_or_else(|e| panic!("loopback endpoints: {e}"));
    Client::builder(credentials)
        .endpoints(endpoints)
        .max_retries(3)
        .timeout(Duration::from_secs(5))
        .retry_delays(Duration::from_millis(1), Duration::from_millis(2))
        .build()
        .unwrap_or_else(|e| panic!("loopback client: {e}"))
}

async fn authenticate(client: &Client, server: &MockServer) {
    let login = server
        .mock_async(|when, then| {
            when.method(POST).path("/api/Auth/loginKey");
            then.status(200)
                .json_body(json!({"success":true,"errorCode":0,"token":"synthetic-token"}));
        })
        .await;
    client
        .authenticate()
        .await
        .unwrap_or_else(|e| panic!("synthetic authentication: {e}"));
    login.assert_calls_async(1).await;
}

#[tokio::test]
async fn every_guarded_mutation_refuses_a_revoked_claim_once_without_a_request() {
    for mutation in MUTATIONS {
        let server = MockServer::start_async().await;
        let client = fixture_client(&server.base_url());
        authenticate(&client, &server).await;
        let sent = server
            .mock_async(|when, then| {
                when.method(POST).path(mutation.path());
                then.status(200)
                    .json_body(json!({"success":true,"errorCode":0,"orderId":84}));
            })
            .await;
        let state = Arc::new(AtomicU8::new(READY));
        let claims = Arc::new(AtomicUsize::new(0));
        let handoff = context(&state, &claims);
        assert_eq!(
            state.compare_exchange(READY, REVOKED, Ordering::SeqCst, Ordering::SeqCst),
            Ok(READY)
        );
        let result = mutation.call(&client, handoff).await;
        sent.assert_calls_async(0).await;
        assert!(matches!(result, Err(Error::MutationHandoffRefused)));
        assert_eq!(claims.load(Ordering::SeqCst), 1);
        assert_eq!(state.load(Ordering::SeqCst), REVOKED);
    }
}

#[tokio::test]
async fn every_guarded_mutation_claims_once_and_preserves_the_complete_original_request() {
    for mutation in MUTATIONS {
        let server = MockServer::start_async().await;
        let client = fixture_client(&server.base_url());
        authenticate(&client, &server).await;
        let sent = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path(mutation.path())
                    .header("authorization", "Bearer synthetic-token")
                    .json_body(mutation.expected_body());
                then.status(200)
                    .json_body(json!({"success":true,"errorCode":0,"orderId":84}));
            })
            .await;
        let claims = Arc::new(AtomicUsize::new(0));
        let state = Arc::new(AtomicU8::new(READY));
        mutation
            .call(&client, context(&state, &claims))
            .await
            .unwrap_or_else(|e| panic!("guarded mutation: {e}"));
        sent.assert_calls_async(1).await;
        assert_eq!(claims.load(Ordering::SeqCst), 1);
        assert_eq!(state.load(Ordering::SeqCst), CLAIMED);
    }
}

#[tokio::test]
async fn authentication_failure_does_not_invoke_or_consume_the_atomic_claim() {
    let server = MockServer::start_async().await;
    let client = fixture_client(&server.base_url());
    for mutation in MUTATIONS {
        let sent = server
            .mock_async(|when, then| {
                when.method(POST).path(mutation.path());
                then.status(500);
            })
            .await;
        let state = Arc::new(AtomicU8::new(READY));
        let claims = Arc::new(AtomicUsize::new(0));
        assert!(matches!(
            mutation.call(&client, context(&state, &claims)).await,
            Err(Error::NotAuthenticated)
        ));
        sent.assert_calls_async(0).await;
        assert_eq!(claims.load(Ordering::SeqCst), 0);
        assert_eq!(state.load(Ordering::SeqCst), READY);
    }
}

#[tokio::test]
async fn shared_rate_refusal_does_not_claim_and_queries_keep_their_existing_contract() {
    let server = MockServer::start_async().await;
    let budget = RateLimit::new(1, Duration::from_mins(1))
        .unwrap_or_else(|e| panic!("synthetic budget: {e}"));
    let credentials = Credentials::new("synthetic-user", "synthetic-key")
        .unwrap_or_else(|e| panic!("synthetic credentials: {e}"));
    let endpoints = Endpoints::custom(&server.base_url(), &server.base_url())
        .unwrap_or_else(|e| panic!("loopback endpoints: {e}"));
    let client = Client::builder(credentials)
        .endpoints(endpoints)
        .rate_limits(RateLimitConfig::new(budget, budget))
        .build()
        .unwrap_or_else(|e| panic!("loopback client: {e}"));
    authenticate(&client, &server).await;
    let query = server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/api/Account/search")
                .json_body(json!({"onlyActiveAccounts":true}));
            then.status(200)
                .json_body(json!({"success":true,"errorCode":0,"accounts":[]}));
        })
        .await;
    assert!(
        client
            .search_active_accounts()
            .await
            .unwrap_or_else(|e| panic!("query: {e}"))
            .is_explicitly_empty()
    );
    query.assert_calls_async(1).await;
    for mutation in MUTATIONS {
        let sent = server
            .mock_async(|when, then| {
                when.method(POST).path(mutation.path());
                then.status(500);
            })
            .await;
        let state = Arc::new(AtomicU8::new(READY));
        let claims = Arc::new(AtomicUsize::new(0));
        assert!(matches!(
            mutation
                .call(&client.clone(), context(&state, &claims))
                .await,
            Err(Error::LocallyRateLimited { .. })
        ));
        sent.assert_calls_async(0).await;
        assert_eq!(claims.load(Ordering::SeqCst), 0);
        assert_eq!(state.load(Ordering::SeqCst), READY);
    }
}

#[tokio::test]
async fn ambiguous_responses_never_resend_or_reclaim_any_guarded_mutation() {
    for (status, body, origin) in [
        (200, "invalid JSON", AmbiguityOrigin::Decode),
        (429, "{}", AmbiguityOrigin::RateLimited),
        (500, "{}", AmbiguityOrigin::HttpStatus(500)),
        (302, "{}", AmbiguityOrigin::HttpStatus(302)),
        (200, r#"{"success":true}"#, AmbiguityOrigin::Decode),
    ] {
        for mutation in MUTATIONS {
            let server = MockServer::start_async().await;
            let client = fixture_client(&server.base_url());
            authenticate(&client, &server).await;
            let sent = server
                .mock_async(|when, then| {
                    when.method(POST)
                        .path(mutation.path())
                        .json_body(mutation.expected_body());
                    then.status(status)
                        .header("Location", format!("{}/redirect-target", server.base_url()))
                        .body(body);
                })
                .await;
            let redirect = server
                .mock_async(|when, then| {
                    when.path("/redirect-target");
                    then.status(200);
                })
                .await;
            let state = Arc::new(AtomicU8::new(READY));
            let claims = Arc::new(AtomicUsize::new(0));
            let result = mutation.call(&client, context(&state, &claims)).await;
            assert!(
                matches!(result, Err(Error::AmbiguousMutation { origin: Some(actual), .. }) if actual == origin)
            );
            sent.assert_calls_async(1).await;
            redirect.assert_calls_async(0).await;
            assert_eq!(claims.load(Ordering::SeqCst), 1);
            assert_eq!(state.load(Ordering::SeqCst), CLAIMED);
        }
    }
}

#[tokio::test]
async fn documented_provider_rejections_remain_definitive_after_claim() {
    for mutation in MUTATIONS {
        let server = MockServer::start_async().await;
        let client = fixture_client(&server.base_url());
        authenticate(&client, &server).await;
        let sent = server
            .mock_async(|when, then| {
                when.method(POST).path(mutation.path());
                then.status(200)
                    .json_body(json!({"success":false,"errorCode":2}));
            })
            .await;
        let state = Arc::new(AtomicU8::new(READY));
        let claims = Arc::new(AtomicUsize::new(0));
        assert!(
            matches!(mutation.call(&client, context(&state, &claims)).await, Err(Error::Provider(e)) if e.code == 2)
        );
        sent.assert_calls_async(1).await;
        assert_eq!(claims.load(Ordering::SeqCst), 1);
        assert_eq!(state.load(Ordering::SeqCst), CLAIMED);
    }
}

async fn read_request(listener: &TcpListener) -> (TcpStream, String, Value) {
    let (mut stream, _) = listener
        .accept()
        .await
        .unwrap_or_else(|e| panic!("loopback accept: {e}"));
    let mut bytes = Vec::new();
    let mut chunk = [0; 2048];
    loop {
        let size = stream
            .read(&mut chunk)
            .await
            .unwrap_or_else(|e| panic!("loopback read: {e}"));
        assert_ne!(size, 0, "request must end after its body");
        bytes.extend_from_slice(&chunk[..size]);
        if let Some(end) = bytes.windows(4).position(|x| x == b"\r\n\r\n") {
            let end = end + 4;
            let headers = String::from_utf8_lossy(&bytes[..end]);
            let length = headers
                .lines()
                .find_map(|line| {
                    let (key, value) = line.split_once(':')?;
                    key.eq_ignore_ascii_case("content-length").then(|| {
                        value
                            .trim()
                            .parse::<usize>()
                            .unwrap_or_else(|e| panic!("content length: {e}"))
                    })
                })
                .unwrap_or(0);
            if bytes.len() >= end + length {
                let body = serde_json::from_slice(&bytes[end..end + length])
                    .unwrap_or_else(|e| panic!("synthetic JSON: {e}"));
                return (stream, headers.into_owned(), body);
            }
        }
    }
}

async fn respond(mut stream: TcpStream, body: &str) {
    let wire = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream
        .write_all(wire.as_bytes())
        .await
        .unwrap_or_else(|e| panic!("loopback response: {e}"));
    stream
        .shutdown()
        .await
        .unwrap_or_else(|e| panic!("loopback shutdown: {e}"));
}

#[tokio::test]
async fn claim_wins_before_a_held_response_and_preserves_original_account_body_and_result() {
    for mutation in MUTATIONS {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap_or_else(|e| panic!("loopback bind: {e}"));
        let address = listener
            .local_addr()
            .unwrap_or_else(|e| panic!("loopback address: {e}"));
        let (seen_tx, seen_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (login, headers, _) = read_request(&listener).await;
            assert!(headers.starts_with("POST /api/Auth/loginKey "));
            respond(
                login,
                r#"{"success":true,"errorCode":0,"token":"synthetic-token"}"#,
            )
            .await;
            let (stream, headers, body) = read_request(&listener).await;
            assert!(headers.starts_with(&format!("POST {} ", mutation.path())));
            seen_tx
                .send(body)
                .unwrap_or_else(|_| panic!("original result receiver"));
            release_rx
                .await
                .unwrap_or_else(|e| panic!("response release: {e}"));
            respond(stream, r#"{"success":true,"errorCode":0,"orderId":84}"#).await;
            assert!(
                listener
                    .poll_accept(&mut std::task::Context::from_waker(std::task::Waker::noop()))
                    .is_pending()
            );
            1
        });
        let client = fixture_client(&format!("http://{address}"));
        client
            .authenticate()
            .await
            .unwrap_or_else(|e| panic!("synthetic login: {e}"));
        let state = Arc::new(AtomicU8::new(READY));
        let claims = Arc::new(AtomicUsize::new(0));
        let handoff = context(&state, &claims);
        let mut task = tokio::spawn(async move { mutation.call(&client, handoff).await });
        let body = seen_rx
            .await
            .unwrap_or_else(|e| panic!("actual mutation seen: {e}"));
        assert_eq!(body, mutation.expected_body());
        assert_eq!(claims.load(Ordering::SeqCst), 1);
        assert_eq!(
            state.compare_exchange(READY, REVOKED, Ordering::SeqCst, Ordering::SeqCst),
            Err(CLAIMED)
        );
        assert!(matches!(
            futures_util::poll!(&mut task),
            std::task::Poll::Pending
        ));
        release_tx
            .send(())
            .unwrap_or_else(|()| panic!("original response gate"));
        task.await
            .unwrap_or_else(|e| panic!("original task: {e}"))
            .unwrap_or_else(|e| panic!("original result: {e}"));
        assert_eq!(server.await.unwrap_or_else(|e| panic!("peer join: {e}")), 1);
        assert_eq!(claims.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn claimed_request_timeout_remains_ambiguous_and_never_replays() {
    for mutation in MUTATIONS {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap_or_else(|e| panic!("loopback bind: {e}"));
        let address = listener
            .local_addr()
            .unwrap_or_else(|e| panic!("loopback address: {e}"));
        let (seen_tx, seen_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (login, _, _) = read_request(&listener).await;
            respond(
                login,
                r#"{"success":true,"errorCode":0,"token":"synthetic-token"}"#,
            )
            .await;
            let (stream, headers, body) = read_request(&listener).await;
            assert!(headers.starts_with(&format!("POST {} ", mutation.path())));
            seen_tx
                .send(body)
                .unwrap_or_else(|_| panic!("original observation receiver"));
            release_rx
                .await
                .unwrap_or_else(|e| panic!("timeout peer release: {e}"));
            drop(stream);
            assert!(
                listener
                    .poll_accept(&mut std::task::Context::from_waker(std::task::Waker::noop()))
                    .is_pending()
            );
            1
        });
        let client = fixture_client(&format!("http://{address}"));
        client
            .authenticate()
            .await
            .unwrap_or_else(|e| panic!("synthetic login: {e}"));
        let state = Arc::new(AtomicU8::new(READY));
        let claims = Arc::new(AtomicUsize::new(0));
        let handoff = context(&state, &claims);
        let task = tokio::spawn(async move { mutation.call(&client, handoff).await });
        assert_eq!(
            seen_rx
                .await
                .unwrap_or_else(|e| panic!("actual request: {e}")),
            mutation.expected_body()
        );
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(6)).await;
        let result = task.await.unwrap_or_else(|e| panic!("original task: {e}"));
        tokio::time::resume();
        assert!(matches!(
            result,
            Err(Error::AmbiguousMutation {
                origin: Some(AmbiguityOrigin::TransportTimeout),
                ..
            })
        ));
        assert_eq!(claims.load(Ordering::SeqCst), 1);
        assert_eq!(state.load(Ordering::SeqCst), CLAIMED);
        release_tx
            .send(())
            .unwrap_or_else(|()| panic!("timeout peer release"));
        assert_eq!(server.await.unwrap_or_else(|e| panic!("peer join: {e}")), 1);
    }
}

#[test]
fn handoff_debug_does_not_expose_captured_context() {
    let secret = String::from("synthetic-private-context");
    let handoff = MutationHandoff::new(move || !secret.is_empty());
    assert_eq!(
        format!("{handoff:?}"),
        "MutationHandoff { claim: \"[REDACTED]\" }"
    );
}
