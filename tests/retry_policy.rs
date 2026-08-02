// SPDX-FileCopyrightText: 2026 Kevin Monaghan
// SPDX-License-Identifier: MIT

//! Retry-policy contract tests using a deterministic local HTTP fixture.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use projectx_client::{
    AccountId, CancelOrder, Client, CloseContract, ContractId, Credentials, Endpoints, Error,
    ModifyOrder, OrderId, OrderType, PartialCloseContract, PlaceOrder, ProviderError, Side,
};
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::{TcpListener, TcpStream},
    sync::oneshot,
};
use tokio_util::sync::CancellationToken;

async fn accept_request(listener: &TcpListener) -> (TcpStream, String) {
    let (mut stream, _) = listener
        .accept()
        .await
        .unwrap_or_else(|error| panic!("fixture request must accept: {error}"));
    let mut request = Vec::new();
    let mut chunk = [0_u8; 2_048];
    loop {
        let read = stream
            .read(&mut chunk)
            .await
            .unwrap_or_else(|error| panic!("fixture request must read: {error}"));
        if read == 0 {
            break;
        }
        request.extend_from_slice(&chunk[..read]);
        let Some(header_end) = request.windows(4).position(|window| window == b"\r\n\r\n") else {
            continue;
        };
        let header_end = header_end + 4;
        let headers = String::from_utf8_lossy(&request[..header_end]);
        let content_length = headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().ok())
                    .flatten()
            })
            .unwrap_or(0);
        if request.len() >= header_end.saturating_add(content_length) {
            break;
        }
    }
    let request = String::from_utf8(request)
        .unwrap_or_else(|error| panic!("fixture HTTP request must be UTF-8: {error}"));
    (stream, request)
}

async fn respond(mut stream: TcpStream, status: u16, body: &str) {
    let reason = if status == 200 {
        "OK"
    } else {
        "Internal Server Error"
    };
    let response = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream
        .write_all(response.as_bytes())
        .await
        .unwrap_or_else(|error| panic!("fixture response must write: {error}"));
    stream
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("fixture response must close: {error}"));
}

async fn start_server(
    responses: Vec<(u16, &'static str)>,
) -> (std::net::SocketAddr, tokio::task::JoinHandle<usize>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap_or_else(|error| panic!("fixture listener must bind: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("fixture listener must have an address: {error}"));
    let task = tokio::spawn(async move {
        let mut count = 0;
        for (status, body) in responses {
            let (stream, _request) = accept_request(&listener).await;
            count += 1;
            respond(stream, status, body).await;
        }
        count
    });
    (address, task)
}

async fn start_counting_mutation_server() -> (
    std::net::SocketAddr,
    Arc<AtomicUsize>,
    CancellationToken,
    tokio::task::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap_or_else(|error| panic!("fixture listener must bind: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("fixture listener must have an address: {error}"));
    let count = Arc::new(AtomicUsize::new(0));
    let task_count = Arc::clone(&count);
    let cancellation = CancellationToken::new();
    let task_cancellation = cancellation.clone();
    let task = tokio::spawn(async move {
        loop {
            let accepted = tokio::select! {
                () = task_cancellation.cancelled() => break,
                accepted = listener.accept() => accepted,
            };
            let (mut stream, _) =
                accepted.unwrap_or_else(|error| panic!("fixture request must accept: {error}"));
            let request = {
                let mut request = Vec::new();
                let mut chunk = [0_u8; 2_048];
                loop {
                    let read = stream
                        .read(&mut chunk)
                        .await
                        .unwrap_or_else(|error| panic!("fixture request must read: {error}"));
                    if read == 0 {
                        break;
                    }
                    request.extend_from_slice(&chunk[..read]);
                    if request.windows(4).any(|window| window == b"\r\n\r\n") {
                        break;
                    }
                }
                String::from_utf8_lossy(&request).into_owned()
            };
            task_count.fetch_add(1, Ordering::SeqCst);
            if request.starts_with("POST /api/Auth/loginKey ") {
                respond(
                    stream,
                    200,
                    r#"{"success":true,"errorCode":0,"token":"synthetic-token"}"#,
                )
                .await;
            } else {
                respond(stream, 500, r"{}").await;
            }
        }
    });
    (address, count, cancellation, task)
}

fn assert_ambiguous_mutation<T>(
    result: Result<T, Error>,
    expected_operation: &'static str,
    provider_code: i32,
) {
    match result {
        Err(Error::AmbiguousMutation { operation }) => {
            assert_eq!(operation, expected_operation);
        }
        Err(error) => {
            panic!("expected ambiguous {expected_operation} for code {provider_code}, got {error}")
        }
        Ok(_) => {
            panic!("expected ambiguous {expected_operation} for code {provider_code}, got success")
        }
    }
}

fn assert_definitive_provider_rejection<T>(result: Result<T, Error>, expected_code: i32) {
    match result {
        Err(Error::Provider(ProviderError { code, .. })) => assert_eq!(code, expected_code),
        Err(error) => panic!("expected provider rejection {expected_code}, got {error}"),
        Ok(_) => panic!("expected provider rejection {expected_code}, got success"),
    }
}

async fn start_authentication_validation_race_server(
    validation_response: &'static str,
) -> (
    std::net::SocketAddr,
    oneshot::Receiver<()>,
    tokio::task::JoinHandle<String>,
) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap_or_else(|error| panic!("fixture listener must bind: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("fixture listener must have an address: {error}"));
    let (validation_seen_tx, validation_seen_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        let (initial_login, request) = accept_request(&listener).await;
        assert!(request.starts_with("POST /api/Auth/loginKey "));
        respond(
            initial_login,
            200,
            r#"{"success":true,"errorCode":0,"token":"initial-token"}"#,
        )
        .await;

        let (validation, request) = accept_request(&listener).await;
        assert!(request.starts_with("POST /api/Auth/validate "));
        assert!(
            request
                .to_ascii_lowercase()
                .contains("authorization: bearer initial-token")
        );
        validation_seen_tx
            .send(())
            .unwrap_or_else(|()| panic!("fixture validation signal must be observed"));

        let (reauthentication, request) = accept_request(&listener).await;
        assert!(request.starts_with("POST /api/Auth/loginKey "));
        respond(
            reauthentication,
            200,
            r#"{"success":true,"errorCode":0,"token":"reauthenticated-token"}"#,
        )
        .await;
        respond(validation, 200, validation_response).await;

        let (query, request) = accept_request(&listener).await;
        assert!(request.starts_with("POST /api/Account/search "));
        respond(
            query,
            200,
            r#"{"success":true,"errorCode":0,"accounts":[]}"#,
        )
        .await;
        request
    });
    (address, validation_seen_rx, task)
}

async fn start_withheld_validation_server() -> (
    std::net::SocketAddr,
    oneshot::Receiver<()>,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap_or_else(|error| panic!("fixture listener must bind: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("fixture listener must have an address: {error}"));
    let (validation_seen_tx, validation_seen_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        let (login, request) = accept_request(&listener).await;
        assert!(request.starts_with("POST /api/Auth/loginKey "));
        respond(
            login,
            200,
            r#"{"success":true,"errorCode":0,"token":"initial-token"}"#,
        )
        .await;

        let (validation, request) = accept_request(&listener).await;
        assert!(request.starts_with("POST /api/Auth/validate "));
        validation_seen_tx
            .send(())
            .unwrap_or_else(|()| panic!("fixture validation signal must be observed"));
        release_rx
            .await
            .unwrap_or_else(|error| panic!("fixture validation must be released: {error}"));
        drop(validation);
    });
    (address, validation_seen_rx, release_tx, task)
}

async fn start_reversed_authentication_server() -> (
    std::net::SocketAddr,
    oneshot::Receiver<()>,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<String>,
) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap_or_else(|error| panic!("fixture listener must bind: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("fixture listener must have an address: {error}"));
    let (first_seen_tx, first_seen_rx) = oneshot::channel();
    let (release_first_tx, release_first_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        let (first, request) = accept_request(&listener).await;
        assert!(request.starts_with("POST /api/Auth/loginKey "));
        first_seen_tx
            .send(())
            .unwrap_or_else(|()| panic!("fixture first-authentication signal must be observed"));

        let (second, request) = accept_request(&listener).await;
        assert!(request.starts_with("POST /api/Auth/loginKey "));
        respond(
            second,
            200,
            r#"{"success":true,"errorCode":0,"token":"completed-first-token"}"#,
        )
        .await;
        release_first_rx.await.unwrap_or_else(|error| {
            panic!("fixture first authentication must be released: {error}")
        });
        respond(
            first,
            200,
            r#"{"success":true,"errorCode":0,"token":"delayed-token"}"#,
        )
        .await;

        let (query, request) = accept_request(&listener).await;
        assert!(request.starts_with("POST /api/Account/search "));
        respond(
            query,
            200,
            r#"{"success":true,"errorCode":0,"accounts":[]}"#,
        )
        .await;
        request
    });
    (address, first_seen_rx, release_first_tx, task)
}

async fn start_reversed_validation_server() -> (
    std::net::SocketAddr,
    oneshot::Receiver<()>,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<String>,
) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap_or_else(|error| panic!("fixture listener must bind: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("fixture listener must have an address: {error}"));
    let (first_seen_tx, first_seen_rx) = oneshot::channel();
    let (release_first_tx, release_first_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        let (login, request) = accept_request(&listener).await;
        assert!(request.starts_with("POST /api/Auth/loginKey "));
        respond(
            login,
            200,
            r#"{"success":true,"errorCode":0,"token":"initial-token"}"#,
        )
        .await;

        let (first, request) = accept_request(&listener).await;
        assert!(request.starts_with("POST /api/Auth/validate "));
        assert!(
            request
                .to_ascii_lowercase()
                .contains("authorization: bearer initial-token")
        );
        first_seen_tx
            .send(())
            .unwrap_or_else(|()| panic!("fixture first-validation signal must be observed"));

        let (second, request) = accept_request(&listener).await;
        assert!(request.starts_with("POST /api/Auth/validate "));
        assert!(
            request
                .to_ascii_lowercase()
                .contains("authorization: bearer initial-token")
        );
        respond(
            second,
            200,
            r#"{"success":true,"errorCode":0,"newToken":"completed-first-token"}"#,
        )
        .await;
        release_first_rx
            .await
            .unwrap_or_else(|error| panic!("fixture first validation must be released: {error}"));
        respond(
            first,
            200,
            r#"{"success":true,"errorCode":0,"newToken":"delayed-token"}"#,
        )
        .await;

        let (query, request) = accept_request(&listener).await;
        assert!(request.starts_with("POST /api/Account/search "));
        respond(
            query,
            200,
            r#"{"success":true,"errorCode":0,"accounts":[]}"#,
        )
        .await;
        request
    });
    (address, first_seen_rx, release_first_tx, task)
}

fn client(address: std::net::SocketAddr, max_retries: u32) -> Client {
    let credentials = Credentials::new("synthetic-user", "synthetic-key")
        .unwrap_or_else(|error| panic!("fixture credentials must be valid: {error}"));
    let base = format!("http://{address}");
    let endpoints = Endpoints::custom(&base, &base)
        .unwrap_or_else(|error| panic!("fixture endpoints must be valid: {error}"));
    Client::builder(credentials)
        .endpoints(endpoints)
        .max_retries(max_retries)
        .retry_delays(Duration::from_millis(1), Duration::from_millis(2))
        .build()
        .unwrap_or_else(|error| panic!("fixture client must build: {error}"))
}

#[tokio::test]
async fn query_retries_one_transient_server_failure() {
    let (address, server) = start_server(vec![
        (
            200,
            r#"{"success":true,"errorCode":0,"token":"synthetic-token"}"#,
        ),
        (500, r"{}"),
        (200, r#"{"accounts":[],"success":true,"errorCode":0}"#),
    ])
    .await;
    let client = client(address, 1);
    client
        .authenticate()
        .await
        .unwrap_or_else(|error| panic!("fixture login must succeed: {error}"));
    let accounts = client
        .search_active_accounts()
        .await
        .unwrap_or_else(|error| panic!("query retry must succeed: {error}"));
    let count = server
        .await
        .unwrap_or_else(|error| panic!("fixture server must join: {error}"));
    assert!(accounts.is_empty());
    assert_eq!(count, 3);
}

#[tokio::test]
async fn authentication_rejects_inconsistent_provider_status() {
    let (address, server) = start_server(vec![(
        200,
        r#"{"success":true,"errorCode":3,"token":"bad-token"}"#,
    )])
    .await;
    let client = client(address, 0);

    let result = client.authenticate().await;
    let after_rejection = client.search_active_accounts().await;
    let count = server
        .await
        .unwrap_or_else(|error| panic!("fixture server must join: {error}"));

    assert!(matches!(
        result,
        Err(Error::InconsistentResponseStatus {
            success: true,
            code: 3
        })
    ));
    assert!(matches!(after_rejection, Err(Error::NotAuthenticated)));
    assert_eq!(count, 1);
}

#[tokio::test]
async fn delayed_validation_cannot_replace_a_concurrent_reauthentication() {
    let (address, validation_seen, server) = start_authentication_validation_race_server(
        r#"{"success":true,"errorCode":0,"newToken":"stale-validation-token"}"#,
    )
    .await;
    let client = client(address, 0);
    client
        .authenticate()
        .await
        .unwrap_or_else(|error| panic!("fixture initial authentication must succeed: {error}"));

    let validation_client = client.clone();
    let validation = tokio::spawn(async move { validation_client.validate_session().await });
    validation_seen
        .await
        .unwrap_or_else(|error| panic!("fixture validation must start: {error}"));
    client
        .authenticate()
        .await
        .unwrap_or_else(|error| panic!("fixture reauthentication must succeed: {error}"));
    validation
        .await
        .unwrap_or_else(|error| panic!("fixture validation task must join: {error}"))
        .unwrap_or_else(|error| panic!("fixture validation must succeed: {error}"));
    client
        .search_active_accounts()
        .await
        .unwrap_or_else(|error| panic!("fixture query must succeed: {error}"));
    let query = server
        .await
        .unwrap_or_else(|error| panic!("fixture server must join: {error}"));

    assert!(
        query
            .to_ascii_lowercase()
            .contains("authorization: bearer reauthenticated-token")
    );
}

#[tokio::test]
async fn delayed_terminal_rejection_cannot_invalidate_a_new_session() {
    let (address, validation_seen, server) =
        start_authentication_validation_race_server(r#"{"success":false,"errorCode":2}"#).await;
    let client = client(address, 0);
    client
        .authenticate()
        .await
        .unwrap_or_else(|error| panic!("fixture initial authentication must succeed: {error}"));

    let validation_client = client.clone();
    let validation = tokio::spawn(async move { validation_client.validate_session().await });
    validation_seen
        .await
        .unwrap_or_else(|error| panic!("fixture validation must start: {error}"));
    client
        .authenticate()
        .await
        .unwrap_or_else(|error| panic!("fixture reauthentication must succeed: {error}"));
    let rejection = validation
        .await
        .unwrap_or_else(|error| panic!("fixture validation task must join: {error}"));
    client
        .search_active_accounts()
        .await
        .unwrap_or_else(|error| panic!("new session query must succeed: {error}"));
    let query = server
        .await
        .unwrap_or_else(|error| panic!("fixture server must join: {error}"));

    assert!(matches!(
        rejection,
        Err(Error::SessionValidationRejected { code: 2 })
    ));
    assert!(
        query
            .to_ascii_lowercase()
            .contains("authorization: bearer reauthenticated-token")
    );
}

#[tokio::test]
async fn delayed_malformed_rotation_cannot_invalidate_a_new_session() {
    let (address, validation_seen, server) = start_authentication_validation_race_server(
        r#"{"success":true,"errorCode":0,"newToken":"invalid token"}"#,
    )
    .await;
    let client = client(address, 0);
    client
        .authenticate()
        .await
        .unwrap_or_else(|error| panic!("fixture initial authentication must succeed: {error}"));

    let validation_client = client.clone();
    let validation = tokio::spawn(async move { validation_client.validate_session().await });
    validation_seen
        .await
        .unwrap_or_else(|error| panic!("fixture validation must start: {error}"));
    client
        .authenticate()
        .await
        .unwrap_or_else(|error| panic!("fixture reauthentication must succeed: {error}"));
    let rejection = validation
        .await
        .unwrap_or_else(|error| panic!("fixture validation task must join: {error}"));
    client
        .search_active_accounts()
        .await
        .unwrap_or_else(|error| panic!("new session query must succeed: {error}"));
    let query = server
        .await
        .unwrap_or_else(|error| panic!("fixture server must join: {error}"));

    assert!(matches!(rejection, Err(Error::InvalidAuthenticationToken)));
    assert!(
        query
            .to_ascii_lowercase()
            .contains("authorization: bearer reauthenticated-token")
    );
}

#[tokio::test]
async fn cancelling_an_admitted_validation_invalidates_its_exact_session() {
    let (address, validation_seen, release_validation, server) =
        start_withheld_validation_server().await;
    let client = client(address, 0);
    client
        .authenticate()
        .await
        .unwrap_or_else(|error| panic!("fixture authentication must succeed: {error}"));

    let validation_client = client.clone();
    let validation = tokio::spawn(async move { validation_client.validate_session().await });
    validation_seen
        .await
        .unwrap_or_else(|error| panic!("fixture validation must be admitted: {error}"));
    validation.abort();
    let join_error = match validation.await {
        Ok(result) => panic!("aborted validation unexpectedly completed: {result:?}"),
        Err(error) => error,
    };
    let after_cancellation = client.search_active_accounts().await;
    release_validation
        .send(())
        .unwrap_or_else(|()| panic!("fixture validation must still be withheld"));
    server
        .await
        .unwrap_or_else(|error| panic!("fixture server must join: {error}"));

    assert!(join_error.is_cancelled());
    assert!(matches!(after_cancellation, Err(Error::NotAuthenticated)));
}

#[tokio::test]
async fn validator_shutdown_reports_an_in_flight_validation_as_ambiguous() {
    let (address, validation_seen, release_validation, server) =
        start_withheld_validation_server().await;
    let client = client(address, 0);
    let validator = client
        .authenticate_with_validation(Duration::from_millis(1))
        .await
        .unwrap_or_else(|error| panic!("fixture authentication must succeed: {error}"));
    validation_seen
        .await
        .unwrap_or_else(|error| panic!("fixture validation must be admitted: {error}"));

    let shutdown = validator.shutdown().await;
    let after_shutdown = client.search_active_accounts().await;
    release_validation
        .send(())
        .unwrap_or_else(|()| panic!("fixture validation must still be withheld"));
    server
        .await
        .unwrap_or_else(|error| panic!("fixture server must join: {error}"));

    assert!(matches!(shutdown, Err(Error::AmbiguousSessionValidation)));
    assert!(matches!(after_shutdown, Err(Error::NotAuthenticated)));
}

#[tokio::test]
async fn dropping_validator_synchronously_invalidates_an_in_flight_validation() {
    let (address, validation_seen, release_validation, server) =
        start_withheld_validation_server().await;
    let client = client(address, 0);
    let validator = client
        .authenticate_with_validation(Duration::from_millis(1))
        .await
        .unwrap_or_else(|error| panic!("fixture authentication must succeed: {error}"));
    validation_seen
        .await
        .unwrap_or_else(|error| panic!("fixture validation must be admitted: {error}"));

    drop(validator);
    let immediately_after_drop = client.search_active_accounts().await;
    release_validation
        .send(())
        .unwrap_or_else(|()| panic!("fixture validation must still be withheld"));
    server
        .await
        .unwrap_or_else(|error| panic!("fixture server must join: {error}"));

    assert!(matches!(
        immediately_after_drop,
        Err(Error::NotAuthenticated)
    ));
}

#[tokio::test]
async fn delayed_authentication_cannot_replace_a_completed_concurrent_authentication() {
    let (address, first_seen, release_first, server) = start_reversed_authentication_server().await;
    let client = client(address, 0);
    let first_client = client.clone();
    let first = tokio::spawn(async move { first_client.authenticate().await });
    first_seen
        .await
        .unwrap_or_else(|error| panic!("fixture first authentication must start: {error}"));

    let second_client = client.clone();
    let second = tokio::spawn(async move { second_client.authenticate().await });
    second
        .await
        .unwrap_or_else(|error| panic!("fixture second-authentication task must join: {error}"))
        .unwrap_or_else(|error| panic!("fixture second authentication must succeed: {error}"));
    release_first
        .send(())
        .unwrap_or_else(|()| panic!("fixture first authentication must still be waiting"));
    first
        .await
        .unwrap_or_else(|error| panic!("fixture first-authentication task must join: {error}"))
        .unwrap_or_else(|error| panic!("fixture delayed authentication must succeed: {error}"));
    client
        .search_active_accounts()
        .await
        .unwrap_or_else(|error| panic!("fixture query must succeed: {error}"));
    let query = server
        .await
        .unwrap_or_else(|error| panic!("fixture server must join: {error}"));

    assert!(
        query
            .to_ascii_lowercase()
            .contains("authorization: bearer completed-first-token")
    );
}

#[tokio::test]
async fn delayed_validation_cannot_replace_a_completed_concurrent_validation() {
    let (address, first_seen, release_first, server) = start_reversed_validation_server().await;
    let client = client(address, 0);
    client
        .authenticate()
        .await
        .unwrap_or_else(|error| panic!("fixture authentication must succeed: {error}"));

    let first_client = client.clone();
    let first = tokio::spawn(async move { first_client.validate_session().await });
    first_seen
        .await
        .unwrap_or_else(|error| panic!("fixture first validation must start: {error}"));
    let second_client = client.clone();
    let second = tokio::spawn(async move { second_client.validate_session().await });
    second
        .await
        .unwrap_or_else(|error| panic!("fixture second-validation task must join: {error}"))
        .unwrap_or_else(|error| panic!("fixture second validation must succeed: {error}"));
    release_first
        .send(())
        .unwrap_or_else(|()| panic!("fixture first validation must still be waiting"));
    first
        .await
        .unwrap_or_else(|error| panic!("fixture first-validation task must join: {error}"))
        .unwrap_or_else(|error| panic!("fixture delayed validation must succeed: {error}"));
    client
        .search_active_accounts()
        .await
        .unwrap_or_else(|error| panic!("fixture query must succeed: {error}"));
    let query = server
        .await
        .unwrap_or_else(|error| panic!("fixture server must join: {error}"));

    assert!(
        query
            .to_ascii_lowercase()
            .contains("authorization: bearer completed-first-token")
    );
}

#[tokio::test]
async fn unauthorized_response_invalidates_only_the_rejected_session() {
    let (address, server) = start_server(vec![
        (
            200,
            r#"{"success":true,"errorCode":0,"token":"synthetic-token"}"#,
        ),
        (401, r#"{"success":false}"#),
    ])
    .await;
    let client = client(address, 0);
    client
        .authenticate()
        .await
        .unwrap_or_else(|error| panic!("fixture authentication must succeed: {error}"));

    let rejected = client.search_active_accounts().await;
    let after_invalidation = client.search_active_accounts().await;
    let count = server
        .await
        .unwrap_or_else(|error| panic!("fixture server must join: {error}"));

    assert!(matches!(
        rejected,
        Err(Error::UnexpectedStatus { status: 401 })
    ));
    assert!(matches!(after_invalidation, Err(Error::NotAuthenticated)));
    assert_eq!(count, 2);
}

#[tokio::test]
async fn unauthorized_mutation_is_a_definitive_authentication_rejection() {
    let (address, server) = start_server(vec![
        (
            200,
            r#"{"success":true,"errorCode":0,"token":"synthetic-token"}"#,
        ),
        (401, r#"{"success":false}"#),
    ])
    .await;
    let client = client(address, 3);
    client
        .authenticate()
        .await
        .unwrap_or_else(|error| panic!("fixture authentication must succeed: {error}"));
    let account_id =
        AccountId::new(42).unwrap_or_else(|error| panic!("fixture account must be valid: {error}"));
    let contract_id = ContractId::new("CON.F.US.MNQ.M26")
        .unwrap_or_else(|error| panic!("fixture contract must be valid: {error}"));
    let order = PlaceOrder::builder(account_id, contract_id, OrderType::Market, Side::Bid, 1)
        .build()
        .unwrap_or_else(|error| panic!("fixture order must be valid: {error}"));

    let rejected = client.place_order(&order).await;
    let after_invalidation = client.search_active_accounts().await;
    let count = server
        .await
        .unwrap_or_else(|error| panic!("fixture server must join: {error}"));

    assert!(matches!(
        rejected,
        Err(Error::UnexpectedStatus { status: 401 })
    ));
    assert!(matches!(after_invalidation, Err(Error::NotAuthenticated)));
    assert_eq!(count, 2);
}

#[tokio::test]
async fn inconsistent_mutation_status_is_an_ambiguous_outcome() {
    let (address, server) = start_server(vec![
        (
            200,
            r#"{"success":true,"errorCode":0,"token":"synthetic-token"}"#,
        ),
        (200, r#"{"success":true,"errorCode":7,"orderId":84}"#),
    ])
    .await;
    let client = client(address, 3);
    client
        .authenticate()
        .await
        .unwrap_or_else(|error| panic!("fixture authentication must succeed: {error}"));
    let account_id =
        AccountId::new(42).unwrap_or_else(|error| panic!("fixture account must be valid: {error}"));
    let contract_id = ContractId::new("CON.F.US.MNQ.M26")
        .unwrap_or_else(|error| panic!("fixture contract must be valid: {error}"));
    let order = PlaceOrder::builder(account_id, contract_id, OrderType::Market, Side::Bid, 1)
        .build()
        .unwrap_or_else(|error| panic!("fixture order must be valid: {error}"));

    let result = client.place_order(&order).await;
    let count = server
        .await
        .unwrap_or_else(|error| panic!("fixture server must join: {error}"));

    assert!(matches!(
        result,
        Err(Error::AmbiguousMutation {
            operation: "order placement"
        })
    ));
    assert_eq!(count, 2);
}

#[tokio::test]
async fn endpoint_mutation_codes_distinguish_ambiguous_from_definitive_outcomes() {
    let (address, server) = start_server(vec![
        (
            200,
            r#"{"success":true,"errorCode":0,"token":"synthetic-token"}"#,
        ),
        (200, r#"{"success":false,"errorCode":6}"#),
        (200, r#"{"success":false,"errorCode":7}"#),
        (200, r#"{"success":false,"errorCode":99}"#),
        (200, r#"{"success":false,"errorCode":1}"#),
        (200, r#"{"success":false,"errorCode":4}"#),
        (200, r#"{"success":false,"errorCode":5}"#),
        (200, r#"{"success":false,"errorCode":99}"#),
        (200, r#"{"success":false,"errorCode":1}"#),
        (200, r#"{"success":false,"errorCode":4}"#),
        (200, r#"{"success":false,"errorCode":5}"#),
        (200, r#"{"success":false,"errorCode":99}"#),
        (200, r#"{"success":false,"errorCode":1}"#),
        (200, r#"{"success":false,"errorCode":6}"#),
        (200, r#"{"success":false,"errorCode":7}"#),
        (200, r#"{"success":false,"errorCode":99}"#),
        (200, r#"{"success":false,"errorCode":1}"#),
        (200, r#"{"success":false,"errorCode":7}"#),
        (200, r#"{"success":false,"errorCode":8}"#),
        (200, r#"{"success":false,"errorCode":99}"#),
        (200, r#"{"success":false,"errorCode":1}"#),
    ])
    .await;
    let client = client(address, 0);
    client
        .authenticate()
        .await
        .unwrap_or_else(|error| panic!("fixture authentication must succeed: {error}"));

    let account_id =
        AccountId::new(42).unwrap_or_else(|error| panic!("fixture account must be valid: {error}"));
    let contract_id = ContractId::new("CON.F.US.MNQ.M26")
        .unwrap_or_else(|error| panic!("fixture contract must be valid: {error}"));
    let order_id =
        OrderId::new(84).unwrap_or_else(|error| panic!("fixture order must be valid: {error}"));
    let place = PlaceOrder::builder(
        account_id,
        contract_id.clone(),
        OrderType::Market,
        Side::Bid,
        1,
    )
    .build()
    .unwrap_or_else(|error| panic!("fixture placement must be valid: {error}"));
    let cancel = CancelOrder {
        account_id,
        order_id,
    };
    let modify = ModifyOrder::builder(account_id, order_id)
        .size(1)
        .build()
        .unwrap_or_else(|error| panic!("fixture modification must be valid: {error}"));
    let close = CloseContract {
        account_id,
        contract_id: contract_id.clone(),
    };
    let partial_close = PartialCloseContract::new(account_id, contract_id, 1)
        .unwrap_or_else(|error| panic!("fixture partial close must be valid: {error}"));

    for code in [6, 7, 99] {
        assert_ambiguous_mutation(client.place_order(&place).await, "order placement", code);
    }
    assert_definitive_provider_rejection(client.place_order(&place).await, 1);

    for code in [4, 5, 99] {
        assert_ambiguous_mutation(
            client.cancel_order(&cancel).await,
            "order cancellation",
            code,
        );
    }
    assert_definitive_provider_rejection(client.cancel_order(&cancel).await, 1);

    for code in [4, 5, 99] {
        assert_ambiguous_mutation(
            client.modify_order(&modify).await,
            "order modification",
            code,
        );
    }
    assert_definitive_provider_rejection(client.modify_order(&modify).await, 1);

    for code in [6, 7, 99] {
        assert_ambiguous_mutation(client.close_contract(&close).await, "position close", code);
    }
    assert_definitive_provider_rejection(client.close_contract(&close).await, 1);

    for code in [7, 8, 99] {
        assert_ambiguous_mutation(
            client.partial_close_contract(&partial_close).await,
            "partial position close",
            code,
        );
    }
    assert_definitive_provider_rejection(client.partial_close_contract(&partial_close).await, 1);

    let count = server
        .await
        .unwrap_or_else(|error| panic!("fixture server must join: {error}"));
    assert_eq!(count, 21);
}

#[tokio::test]
async fn terminal_validation_rejection_invalidates_the_session() {
    let (address, server) = start_server(vec![
        (
            200,
            r#"{"success":true,"errorCode":0,"token":"synthetic-token"}"#,
        ),
        (200, r#"{"success":false,"errorCode":3}"#),
    ])
    .await;
    let client = client(address, 0);
    client
        .authenticate()
        .await
        .unwrap_or_else(|error| panic!("fixture authentication must succeed: {error}"));

    let rejected = client.validate_session().await;
    let after_invalidation = client.search_active_accounts().await;
    let count = server
        .await
        .unwrap_or_else(|error| panic!("fixture server must join: {error}"));

    assert!(matches!(
        rejected,
        Err(Error::SessionValidationRejected { code: 3 })
    ));
    assert!(matches!(after_invalidation, Err(Error::NotAuthenticated)));
    assert_eq!(count, 2);
}

#[tokio::test]
async fn unknown_validation_outcome_is_ambiguous_and_invalidates_the_session() {
    let (address, server) = start_server(vec![
        (
            200,
            r#"{"success":true,"errorCode":0,"token":"synthetic-token"}"#,
        ),
        (200, r#"{"success":false,"errorCode":4}"#),
    ])
    .await;
    let client = client(address, 0);
    client
        .authenticate()
        .await
        .unwrap_or_else(|error| panic!("fixture authentication must succeed: {error}"));

    let validation = client.validate_session().await;
    let after_validation = client.search_active_accounts().await;
    let count = server
        .await
        .unwrap_or_else(|error| panic!("fixture server must join: {error}"));

    assert!(matches!(validation, Err(Error::AmbiguousSessionValidation)));
    assert!(matches!(after_validation, Err(Error::NotAuthenticated)));
    assert_eq!(count, 2);
}

#[tokio::test]
async fn periodic_validator_stops_on_an_unknown_validation_outcome() {
    let (address, server) = start_server(vec![
        (
            200,
            r#"{"success":true,"errorCode":0,"token":"synthetic-token"}"#,
        ),
        (200, r#"{"success":false,"errorCode":4}"#),
    ])
    .await;
    let client = client(address, 0);
    let validator = client
        .authenticate_with_validation(Duration::from_millis(1))
        .await
        .unwrap_or_else(|error| panic!("fixture authentication must succeed: {error}"));

    tokio::time::timeout(Duration::from_secs(1), async {
        while !validator.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|error| panic!("validator must stop after an unknown outcome: {error}"));
    let validation = validator.shutdown().await;
    let count = server
        .await
        .unwrap_or_else(|error| panic!("fixture server must join: {error}"));

    assert!(matches!(validation, Err(Error::AmbiguousSessionValidation)));
    assert_eq!(count, 2);
}

#[tokio::test]
async fn malformed_rotation_token_invalidates_the_validated_session() {
    let (address, server) = start_server(vec![
        (
            200,
            r#"{"success":true,"errorCode":0,"token":"synthetic-token"}"#,
        ),
        (
            200,
            r#"{"success":true,"errorCode":0,"newToken":"invalid token"}"#,
        ),
    ])
    .await;
    let client = client(address, 0);
    client
        .authenticate()
        .await
        .unwrap_or_else(|error| panic!("fixture authentication must succeed: {error}"));

    let rejected = client.validate_session().await;
    let after_invalidation = client.search_active_accounts().await;
    let count = server
        .await
        .unwrap_or_else(|error| panic!("fixture server must join: {error}"));

    assert!(matches!(rejected, Err(Error::InvalidAuthenticationToken)));
    assert!(matches!(after_invalidation, Err(Error::NotAuthenticated)));
    assert_eq!(count, 2);
}

#[tokio::test]
async fn malformed_validation_schema_invalidates_the_validated_session() {
    let (address, server) = start_server(vec![
        (
            200,
            r#"{"success":true,"errorCode":0,"token":"synthetic-token"}"#,
        ),
        (200, r#"{"success":true,"errorCode":0,"newToken":7}"#),
    ])
    .await;
    let client = client(address, 0);
    client
        .authenticate()
        .await
        .unwrap_or_else(|error| panic!("fixture authentication must succeed: {error}"));

    let rejected = client.validate_session().await;
    let after_invalidation = client.search_active_accounts().await;
    let count = server
        .await
        .unwrap_or_else(|error| panic!("fixture server must join: {error}"));

    assert!(matches!(rejected, Err(Error::Decode(_))));
    assert!(matches!(after_invalidation, Err(Error::NotAuthenticated)));
    assert_eq!(count, 2);
}

#[tokio::test]
async fn inconsistent_validation_status_invalidates_the_validated_session() {
    let (address, server) = start_server(vec![
        (
            200,
            r#"{"success":true,"errorCode":0,"token":"synthetic-token"}"#,
        ),
        (200, r#"{"success":true,"errorCode":3}"#),
    ])
    .await;
    let client = client(address, 0);
    client
        .authenticate()
        .await
        .unwrap_or_else(|error| panic!("fixture authentication must succeed: {error}"));

    let rejected = client.validate_session().await;
    let after_invalidation = client.search_active_accounts().await;
    let count = server
        .await
        .unwrap_or_else(|error| panic!("fixture server must join: {error}"));

    assert!(matches!(
        rejected,
        Err(Error::InconsistentResponseStatus {
            success: true,
            code: 3
        })
    ));
    assert!(matches!(after_invalidation, Err(Error::NotAuthenticated)));
    assert_eq!(count, 2);
}

#[tokio::test]
async fn admitted_validation_server_error_is_ambiguous_and_never_retried() {
    let (address, server) = start_server(vec![
        (
            200,
            r#"{"success":true,"errorCode":0,"token":"synthetic-token"}"#,
        ),
        (500, r#"{"success":false,"errorCode":1}"#),
    ])
    .await;
    let client = client(address, 3);
    client
        .authenticate()
        .await
        .unwrap_or_else(|error| panic!("fixture authentication must succeed: {error}"));

    let rejected = client.validate_session().await;
    let after_invalidation = client.search_active_accounts().await;
    let count = server
        .await
        .unwrap_or_else(|error| panic!("fixture server must join: {error}"));

    assert!(matches!(rejected, Err(Error::AmbiguousSessionValidation)));
    assert!(matches!(after_invalidation, Err(Error::NotAuthenticated)));
    assert_eq!(count, 2);
}

#[tokio::test]
async fn validator_exposes_terminal_session_failure() {
    let (address, server) = start_server(vec![
        (
            200,
            r#"{"success":true,"errorCode":0,"token":"synthetic-token"}"#,
        ),
        (200, r#"{"success":false,"errorCode":1}"#),
    ])
    .await;
    let client = client(address, 0);
    let validator = client
        .authenticate_with_validation(Duration::from_millis(1))
        .await
        .unwrap_or_else(|error| panic!("fixture authentication must succeed: {error}"));

    tokio::time::timeout(Duration::from_secs(1), async {
        while !validator.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|error| panic!("validator must stop after terminal rejection: {error}"));
    let result = validator.shutdown().await;
    let count = server
        .await
        .unwrap_or_else(|error| panic!("fixture server must join: {error}"));

    assert!(matches!(
        result,
        Err(Error::SessionValidationRejected { code: 1 })
    ));
    assert_eq!(count, 2);
}

#[tokio::test]
async fn validator_exposes_unusable_rotation_as_a_terminal_failure() {
    for (rotation_response, expects_missing) in [
        (
            r#"{"success":true,"errorCode":0,"newToken":"invalid token"}"#,
            false,
        ),
        (r#"{"success":true,"errorCode":0,"newToken":""}"#, true),
    ] {
        let (address, server) = start_server(vec![
            (
                200,
                r#"{"success":true,"errorCode":0,"token":"synthetic-token"}"#,
            ),
            (200, rotation_response),
        ])
        .await;
        let client = client(address, 0);
        let validator = client
            .authenticate_with_validation(Duration::from_millis(1))
            .await
            .unwrap_or_else(|error| panic!("fixture authentication must succeed: {error}"));

        tokio::time::timeout(Duration::from_secs(1), async {
            while !validator.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|error| panic!("validator must stop after unusable rotation: {error}"));
        let result = validator.shutdown().await;
        let count = server
            .await
            .unwrap_or_else(|error| panic!("fixture server must join: {error}"));

        assert_eq!(
            matches!(result, Err(Error::MissingAuthenticationToken)),
            expects_missing
        );
        assert_eq!(count, 2);
    }
}

#[tokio::test]
async fn order_placement_never_retries_a_server_response() {
    let (address, request_count, cancellation, server) = start_counting_mutation_server().await;
    let client = client(address, 3);
    client
        .authenticate()
        .await
        .unwrap_or_else(|error| panic!("fixture login must succeed: {error}"));
    let account_id =
        AccountId::new(42).unwrap_or_else(|error| panic!("fixture account must be valid: {error}"));
    let contract_id = ContractId::new("CON.F.US.MNQ.M26")
        .unwrap_or_else(|error| panic!("fixture contract must be valid: {error}"));
    let order = PlaceOrder::builder(account_id, contract_id, OrderType::Market, Side::Bid, 1)
        .custom_tag("synthetic-order")
        .build()
        .unwrap_or_else(|error| panic!("fixture order must be valid: {error}"));
    let result = client.place_order(&order).await;
    let count = request_count.load(Ordering::SeqCst);
    cancellation.cancel();
    server
        .await
        .unwrap_or_else(|error| panic!("fixture server must join: {error}"));
    assert!(matches!(
        result,
        Err(Error::AmbiguousMutation {
            operation: "order placement"
        })
    ));
    assert_eq!(count, 2);
}

#[tokio::test]
async fn accepted_order_without_an_id_is_an_ambiguous_outcome() {
    let (address, server) = start_server(vec![
        (
            200,
            r#"{"success":true,"errorCode":0,"token":"synthetic-token"}"#,
        ),
        (200, r#"{"success":true,"errorCode":0}"#),
    ])
    .await;
    let client = client(address, 3);
    client
        .authenticate()
        .await
        .unwrap_or_else(|error| panic!("fixture login must succeed: {error}"));
    let account_id =
        AccountId::new(42).unwrap_or_else(|error| panic!("fixture account must be valid: {error}"));
    let contract_id = ContractId::new("CON.F.US.MNQ.M26")
        .unwrap_or_else(|error| panic!("fixture contract must be valid: {error}"));
    let order = PlaceOrder::builder(account_id, contract_id, OrderType::Market, Side::Bid, 1)
        .build()
        .unwrap_or_else(|error| panic!("fixture order must be valid: {error}"));

    let result = client.place_order(&order).await;
    let count = server
        .await
        .unwrap_or_else(|error| panic!("fixture server must join: {error}"));

    assert!(matches!(
        result,
        Err(Error::AmbiguousMutation {
            operation: "order placement"
        })
    ));
    assert_eq!(count, 2);
}

#[tokio::test]
async fn unrepresentable_validation_period_is_rejected_before_authentication() {
    let (address, request_count, cancellation, server) = start_counting_mutation_server().await;
    let client = client(address, 0);

    let result = client.authenticate_with_validation(Duration::MAX).await;

    assert!(matches!(result, Err(Error::Configuration(_))));
    assert_eq!(request_count.load(Ordering::SeqCst), 0);
    cancellation.cancel();
    server
        .await
        .unwrap_or_else(|error| panic!("fixture server must join: {error}"));
}
