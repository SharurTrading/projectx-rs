// SPDX-FileCopyrightText: 2026 Kevin Monaghan
// SPDX-License-Identifier: MIT-0

//! Unit tests for `SignalR` invocation, handshake, and WebSocket upgrade decoding.

use data_encoding::BASE64;
use reqwest::{StatusCode, Version, header};
use serde_json::{Value, json};
use tokio_tungstenite::tungstenite::Message;

use crate::ContractId;

use super::error::RealtimeError;
use super::invocation::{SIGNALR_TERMINATOR, SignalRInvocation, completion, encode_invocation};
use super::socket::{
    WEBSOCKET_READ_BUFFER_SIZE, WEBSOCKET_WRITE_BUFFER_SIZE, validate_handshake,
    validate_websocket_upgrade, websocket_accept, websocket_config, websocket_key,
};

#[test]
fn invocation_extracts_market_contract_and_payload() {
    let value = json!({
        "type": 1,
        "target": "GatewayTrade",
        "arguments": ["CON.F.US.MNQ.M26", {"price": 1.25}],
    });
    let invocation = SignalRInvocation::from_value(&value)
        .and_then(|value| value.ok_or(RealtimeError::Protocol("missing invocation")))
        .unwrap_or_else(|error| panic!("fixture invocation must decode: {error}"));
    assert_eq!(invocation.target(), "GatewayTrade");
    assert_eq!(
        invocation.contract_id().map(ContractId::as_str),
        Some("CON.F.US.MNQ.M26")
    );
    assert_eq!(invocation.payload().get(), r#"{"price":1.25}"#);
}

#[test]
fn invocation_recognizer_reports_malformed_discriminators_instead_of_absence() {
    for value in [
        json!({"type": "1", "target": "GatewayTrade", "arguments": []}),
        json!({"type": 1.5}),
        json!({"type": -1}),
        json!({"type": null}),
        json!({"type": [1]}),
        json!({}),
        json!({"target": "GatewayTrade"}),
        json!([1, 2]),
        json!("frame"),
    ] {
        assert!(
            matches!(
                SignalRInvocation::from_value(&value),
                Err(RealtimeError::Protocol(_))
            ),
            "malformed discriminator {value} must be a typed protocol error"
        );
    }
    for value in [
        json!({"type": 6}),
        json!({"type": 3, "invocationId": "1"}),
        json!({"type": 7, "allowReconnect": false}),
    ] {
        assert!(
            matches!(SignalRInvocation::from_value(&value), Ok(None)),
            "another unsigned message type {value} must remain a recognition miss"
        );
    }
}

#[test]
fn invocation_rejects_malformed_market_arguments() {
    for value in [
        json!({
            "type": 1,
            "target": "GatewayTrade",
            "arguments": [42, {"price": 1.25}],
        }),
        json!({
            "type": 1,
            "target": "GatewayTrade",
            "arguments": ["CON.F.US.MNQ.M26", {"price": 1.25}, "extra"],
        }),
    ] {
        assert!(matches!(
            SignalRInvocation::from_value(&value),
            Err(RealtimeError::Protocol(_))
        ));
    }
}

#[test]
fn handshake_accepts_coalesced_tail() {
    let tail = validate_handshake("{}\u{001e}{\"type\":6}\u{001e}")
        .unwrap_or_else(|error| panic!("fixture handshake must decode: {error}"));
    assert_eq!(tail.as_deref(), Some("{\"type\":6}\u{001e}"));
}

#[test]
fn handshake_rejects_provider_error_without_retaining_detail() {
    let result = validate_handshake("{\"error\":\"secret provider detail\"}\u{001e}");
    assert!(matches!(result, Err(RealtimeError::Handshake(_))));
    assert!(!format!("{:?}", result.err()).contains("secret provider detail"));
}

#[test]
fn completion_requires_an_unambiguous_protocol_shape() {
    for value in [
        json!({"type": 3}),
        json!({"type": 3, "invocationId": 1}),
        json!({"type": 3, "invocationId": "1", "error": false}),
        json!({"type": 3, "invocationId": "1", "error": "rejected", "result": null}),
    ] {
        assert!(matches!(
            completion(&value),
            Err(RealtimeError::Protocol(_))
        ));
    }

    assert_eq!(
        completion(&json!({"type": 3, "invocationId": "1"}))
            .unwrap_or_else(|error| panic!("void completion must decode: {error}")),
        ("1", Ok(()))
    );
    assert_eq!(
        completion(&json!({"type": 3, "invocationId": "1", "result": 42}))
            .unwrap_or_else(|error| panic!("result completion must decode: {error}")),
        ("1", Ok(()))
    );
    assert_eq!(
        completion(&json!({"type": 3, "invocationId": "1", "error": "rejected"}))
            .unwrap_or_else(|error| panic!("error completion must decode: {error}")),
        ("1", Err(()))
    );
}

#[test]
fn websocket_configuration_accepts_large_provider_batches() {
    let config = websocket_config();
    assert_eq!(config.read_buffer_size, WEBSOCKET_READ_BUFFER_SIZE);
    assert_eq!(config.write_buffer_size, WEBSOCKET_WRITE_BUFFER_SIZE);
    assert_eq!(config.max_message_size, None);
    assert_eq!(config.max_frame_size, None);
    assert!(config.max_write_buffer_size > config.write_buffer_size);
}

#[test]
fn websocket_accept_matches_the_rfc_6455_vector() {
    assert_eq!(
        websocket_accept("dGhlIHNhbXBsZSBub25jZQ=="),
        "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
    );
    let key = websocket_key()
        .unwrap_or_else(|error| panic!("fixture key generation must succeed: {error}"));
    let decoded = BASE64
        .decode(key.as_bytes())
        .unwrap_or_else(|error| panic!("generated key must be base64: {error}"));
    assert_eq!(decoded.len(), 16);
}

#[test]
fn websocket_upgrade_validation_rejects_untrusted_responses() {
    let expected_accept = websocket_accept("dGhlIHNhbXBsZSBub25jZQ==");
    let mut valid = header::HeaderMap::new();
    valid.insert(
        header::CONNECTION,
        header::HeaderValue::from_static("keep-alive, Upgrade"),
    );
    valid.insert(
        header::UPGRADE,
        header::HeaderValue::from_static("WebSocket"),
    );
    valid.insert(
        header::SEC_WEBSOCKET_ACCEPT,
        header::HeaderValue::from_str(&expected_accept)
            .unwrap_or_else(|error| panic!("fixture accept header must be valid: {error}")),
    );

    assert!(
        validate_websocket_upgrade(
            StatusCode::SWITCHING_PROTOCOLS,
            Version::HTTP_11,
            &valid,
            &expected_accept,
        )
        .is_ok()
    );

    let mut cases = Vec::new();
    cases.push((StatusCode::OK, Version::HTTP_11, valid.clone()));
    cases.push((
        StatusCode::SWITCHING_PROTOCOLS,
        Version::HTTP_2,
        valid.clone(),
    ));
    for missing in [
        header::CONNECTION,
        header::UPGRADE,
        header::SEC_WEBSOCKET_ACCEPT,
    ] {
        let mut headers = valid.clone();
        headers.remove(missing);
        cases.push((StatusCode::SWITCHING_PROTOCOLS, Version::HTTP_11, headers));
    }
    for unsolicited in [
        header::SEC_WEBSOCKET_EXTENSIONS,
        header::SEC_WEBSOCKET_PROTOCOL,
    ] {
        let mut headers = valid.clone();
        headers.insert(unsolicited, header::HeaderValue::from_static("unsupported"));
        cases.push((StatusCode::SWITCHING_PROTOCOLS, Version::HTTP_11, headers));
    }
    let mut duplicate_accept = valid.clone();
    duplicate_accept.append(
        header::SEC_WEBSOCKET_ACCEPT,
        header::HeaderValue::from_str(&expected_accept)
            .unwrap_or_else(|error| panic!("fixture accept header must be valid: {error}")),
    );
    cases.push((
        StatusCode::SWITCHING_PROTOCOLS,
        Version::HTTP_11,
        duplicate_accept,
    ));

    for (status, version, headers) in cases {
        assert!(matches!(
            validate_websocket_upgrade(status, version, &headers, &expected_accept),
            Err(RealtimeError::Transport)
        ));
    }
}

#[test]
fn invocation_encoder_accepts_small_terminated_messages() {
    let message = encode_invocation("1", "SubscribeAccounts", &[])
        .unwrap_or_else(|error| panic!("small invocation must encode: {error}"));
    let Message::Text(text) = message else {
        panic!("invocation must encode as text");
    };
    assert!(text.ends_with(SIGNALR_TERMINATOR));
}

#[test]
fn invocation_encoder_preserves_large_argument_batches() {
    let payload = "x".repeat(2 * 1_024 * 1_024);
    let message = encode_invocation("1", "Batch", &[Value::String(payload.clone())])
        .unwrap_or_else(|error| panic!("large invocation: {error}"));
    let Message::Text(text) = message else {
        panic!("text invocation");
    };
    let value: Value = serde_json::from_str(text.trim_end_matches(SIGNALR_TERMINATOR))
        .unwrap_or_else(|error| panic!("decode: {error}"));
    assert_eq!(value["arguments"][0], payload);
}
