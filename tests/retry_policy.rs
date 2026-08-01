//! Retry-policy contract tests using a deterministic local HTTP fixture.

use std::time::Duration;

use projectx_client::{
    AccountId, Client, ContractId, Credentials, Endpoints, Error, OrderType, PlaceOrder, Side,
};
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::TcpListener,
};

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
            let (mut stream, _) = listener
                .accept()
                .await
                .unwrap_or_else(|error| panic!("fixture request must accept: {error}"));
            let mut request = vec![0_u8; 8_192];
            let _ = stream
                .read(&mut request)
                .await
                .unwrap_or_else(|error| panic!("fixture request must read: {error}"));
            count += 1;
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
        count
    });
    (address, task)
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
        (200, r#"{"success":true,"token":"synthetic-token"}"#),
        (500, r"{}"),
        (200, r#"{"accounts":[],"success":true}"#),
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
async fn order_placement_never_retries_a_server_response() {
    let (address, server) = start_server(vec![
        (200, r#"{"success":true,"token":"synthetic-token"}"#),
        (500, r"{}"),
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
    let result = client
        .place_order(&PlaceOrder {
            account_id,
            contract_id,
            order_type: OrderType::Market,
            side: Side::Bid,
            size: 1,
            limit_price: None,
            stop_price: None,
            trail_price: None,
            custom_tag: Some("synthetic-order".to_owned()),
            stop_loss_bracket: None,
            take_profit_bracket: None,
        })
        .await;
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
