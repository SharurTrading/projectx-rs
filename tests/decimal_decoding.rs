// SPDX-FileCopyrightText: 2026 Kevin Monaghan
// SPDX-License-Identifier: MIT-0

//! Public monetary DTO decoding uses exact, independently synthetic values.

use projectx_client::{Account, Contract, SignalRInvocation};
use rust_decimal::Decimal;

fn account(token: &str) -> Result<Account, serde_json::Error> {
    serde_json::from_str(&format!(
        r#"{{"id":42,"name":"Synthetic","balance":{token},"canTrade":true,"isVisible":true}}"#
    ))
}

fn contract(token: &str) -> Result<Contract, serde_json::Error> {
    serde_json::from_str(&format!(
        r#"{{"id":"CON.F.US.TEST.Z26","name":"TESTZ26","description":"Synthetic","tickSize":{token},"tickValue":1,"activeContract":true,"symbolId":"F.US.TEST"}}"#
    ))
}

#[test]
fn public_required_and_optional_decimals_reject_lossy_values() {
    for token in [
        "0.12345678901234567890123456789",
        "-0.12345678901234567890123456789",
        r#""0.12345678901234567890123456789""#,
        "0.12345678901234567890123456789e0",
        r#""0.12345678901234567890123456789e0""#,
        "1e-29",
        "-9e-29",
        r#""0.00000000000000000000000000001""#,
        "79228162514264337593543950336",
        "-79228162514264337593543950336",
        "7922816251426433759354395033.51",
        "79228162514264337593543950335e1",
    ] {
        let optional = account(token);
        assert!(
            optional.is_err(),
            "optional value {token} rounded: {optional:?}"
        );
        let required = contract(token);
        assert!(
            required.is_err(),
            "required value {token} rounded: {required:?}"
        );
    }
}

#[test]
fn public_decimals_preserve_exact_plain_quoted_and_scientific_values() {
    for (token, expected) in [
        (
            "0.1000000000000000000000000001",
            "0.1000000000000000000000000001",
        ),
        (
            r#""0.1000000000000000000000000001""#,
            "0.1000000000000000000000000001",
        ),
        ("1.25e2", "125"),
        ("-1.25E+2", "-125"),
        (r#""125E-2""#, "1.25"),
        ("100e-30", "0.0000000000000000000000000001"),
        ("-1e-28", "-0.0000000000000000000000000001"),
        (
            "1.00000000000000000000000000000",
            "1.0000000000000000000000000000",
        ),
        (
            "792281625142643375935439503350e-1",
            "79228162514264337593543950335",
        ),
        (
            "-79228162514264337593543950335",
            "-79228162514264337593543950335",
        ),
        (
            "7922816251426433759354395033.5",
            "7922816251426433759354395033.5",
        ),
    ] {
        let decoded = account(token).unwrap_or_else(|error| panic!("{token}: {error}"));
        assert_eq!(
            decoded.balance.map(|value| value.to_string()).as_deref(),
            Some(expected)
        );
        let decoded = contract(token).unwrap_or_else(|error| panic!("{token}: {error}"));
        assert_eq!(decoded.tick_size.to_string(), expected);
    }
}

#[test]
fn optional_null_and_absence_remain_distinct_from_required_numbers() {
    assert_eq!(
        account("null")
            .unwrap_or_else(|error| panic!("null: {error}"))
            .balance,
        None
    );
    let missing: Account =
        serde_json::from_str(r#"{"id":42,"name":"Synthetic","canTrade":true,"isVisible":true}"#)
            .unwrap_or_else(|error| panic!("missing balance: {error}"));
    assert_eq!(missing.balance, None);
    assert!(contract("null").is_err());
    assert!(account(r#""null""#).is_err());
}

#[test]
fn realtime_batch_retains_exact_entries_and_refuses_only_lossy_entries() {
    let invocation = SignalRInvocation::from_json(
        r#"{"type":1,"target":"GatewayAccount","arguments":[[null,{"id":42,"name":"Synthetic","balance":0.1000000000000000000000000001,"canTrade":true,"isVisible":true},{"id":43,"name":"Synthetic","balance":0.12345678901234567890123456789,"canTrade":true,"isVisible":true}]]}"#,
    ).unwrap_or_else(|error| panic!("invocation: {error}"))
        .unwrap_or_else(|| panic!("missing invocation"));
    let entries = invocation.decode_batch::<Account>();
    assert_eq!(entries.len(), 2);
    assert_eq!(
        entries[0]
            .as_ref()
            .unwrap_or_else(|error| panic!("exact entry: {error}"))
            .balance
            .map(|value| value.to_string())
            .as_deref(),
        Some("0.1000000000000000000000000001")
    );
    assert!(
        entries[1].is_err(),
        "lossy entry accepted: {:?}",
        entries[1]
    );
}

#[test]
fn exactly_constructed_values_survive_scientific_rescaling() {
    let maximum = Decimal::MAX.mantissa();
    for coefficient in [0, 1, 9, 10, 79, maximum / 10, maximum - 1, maximum] {
        for scale in 0..=Decimal::MAX_SCALE {
            for negative in [false, true] {
                let mut expected = Decimal::try_from_i128_with_scale(coefficient, scale)
                    .unwrap_or_else(|error| panic!("exact constructor: {error}"));
                expected.set_sign_negative(negative);
                let sign = if negative { "-" } else { "" };
                let expanded = if coefficient == 0 {
                    "0".to_owned()
                } else {
                    format!("{coefficient}00")
                };
                // These forms denote the constructor's exact coefficient and
                // scale, independently of the decoder algorithm.
                for token in [
                    format!("{sign}{coefficient}e-{scale}"),
                    format!("{sign}{expanded}e-{}", scale + 2),
                ] {
                    let decoded = account(&token)
                        .unwrap_or_else(|error| panic!("{token}: {error}"))
                        .balance
                        .unwrap_or_else(|| panic!("missing {token}"));
                    assert_eq!(decoded, expected, "{token}");
                    assert_eq!(decoded.is_sign_negative(), negative, "{token}");
                }
            }
        }
    }
}
