// SPDX-FileCopyrightText: 2026 Kevin Monaghan
// SPDX-License-Identifier: MIT-0

//! Exact JSON-number serialization for provider decimal fields.
//!
//! This module uses `serde_json`'s raw-value boundary instead of enabling the
//! global `arbitrary_precision` feature. The latter participates in Cargo
//! feature unification and can change how unrelated downstream crates decode
//! internally tagged JSON enums.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize, ser::Error as _};
use serde_json::value::RawValue;

pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<Decimal, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let raw = Box::<RawValue>::deserialize(deserializer)?;
    parse(raw.get())
}

pub(crate) fn serialize<S>(value: &Decimal, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    RawValue::from_string(value.to_string())
        .map_err(S::Error::custom)?
        .serialize(serializer)
}

fn parse<E>(raw: &str) -> Result<Decimal, E>
where
    E: serde::de::Error,
{
    let owned;
    let value = if raw.starts_with('"') {
        owned = serde_json::from_str::<String>(raw).map_err(E::custom)?;
        owned.as_str()
    } else {
        raw
    };

    parse_exact(value).map_err(E::custom)
}

// Decimal's ordinary and scientific parsers may round. Validate the entire
// token first, then cancel only exact powers of ten before constructing it.
fn parse_exact(text: &str) -> Result<Decimal, &'static str> {
    let mut parts = text.split(['e', 'E']);
    let mantissa = parts.next().ok_or("missing decimal mantissa")?;
    let exponent = parse_exponent(parts.next())?;
    if parts.next().is_some() {
        return Err("decimal has more than one exponent");
    }
    let (negative, digits, fractional) = parse_mantissa(mantissa)?;
    let original_scale = exponent.and_then(|value| fractional.checked_sub(value));
    let Some(start) = digits.iter().position(|digit| *digit != 0) else {
        // Zero is exact even when its textual exponent cannot fit an integer.
        // Keep its sign and scale when that scale is representable.
        let scale = original_scale
            .and_then(|value| u32::try_from(value).ok())
            .filter(|value| *value <= Decimal::MAX_SCALE)
            .unwrap_or(0);
        return construct_decimal(0, scale, negative);
    };
    let original_scale = original_scale.ok_or("decimal exponent is out of range")?;
    let mut end = digits.len();
    while digits.get(end.saturating_sub(1)) == Some(&0) {
        end -= 1;
    }
    let cancelled =
        i64::try_from(digits.len() - end).map_err(|_| "decimal mantissa is too long")?;
    let scale = original_scale
        .checked_sub(cancelled)
        .ok_or("decimal exponent is out of range")?;
    let significant = &digits[start..end];
    if significant.len() > 29 {
        return Err("decimal coefficient exceeds 96 bits");
    }
    let mut coefficient = significant
        .iter()
        .try_fold(0_i128, |value, digit| {
            value.checked_mul(10)?.checked_add(i128::from(*digit))
        })
        .ok_or("decimal coefficient is out of range")?;
    let mut scale = if scale < 0 {
        let zeros = scale
            .checked_neg()
            .and_then(|value| u32::try_from(value).ok())
            .filter(|value| *value <= Decimal::MAX_SCALE)
            .ok_or("decimal value is out of range")?;
        coefficient = coefficient
            .checked_mul(
                10_i128
                    .checked_pow(zeros)
                    .ok_or("decimal value is out of range")?,
            )
            .ok_or("decimal value is out of range")?;
        0
    } else {
        u32::try_from(scale)
            .ok()
            .filter(|value| *value <= Decimal::MAX_SCALE)
            .ok_or("decimal precision exceeds 28 places")?
    };
    if coefficient > Decimal::MAX.mantissa() {
        return Err("decimal coefficient exceeds 96 bits");
    }
    // Preserve the supplied scale whenever it fits; normalization never
    // discards a nonzero digit. This loop has at most MAX_SCALE iterations.
    let desired = u32::try_from(original_scale.max(0))
        .unwrap_or(Decimal::MAX_SCALE)
        .min(Decimal::MAX_SCALE);
    while scale < desired {
        let Some(expanded) = coefficient
            .checked_mul(10)
            .filter(|value| *value <= Decimal::MAX.mantissa())
        else {
            break;
        };
        coefficient = expanded;
        scale += 1;
    }
    construct_decimal(coefficient, scale, negative)
}

fn construct_decimal(
    coefficient: i128,
    scale: u32,
    negative: bool,
) -> Result<Decimal, &'static str> {
    let mut value = Decimal::try_from_i128_with_scale(coefficient, scale)
        .map_err(|_| "decimal value is out of range")?;
    value.set_sign_negative(negative);
    Ok(value)
}

fn parse_mantissa(text: &str) -> Result<(bool, Vec<u8>, i64), &'static str> {
    let (negative, text) = match text.as_bytes().first() {
        Some(b'-') => (true, &text[1..]),
        Some(b'+') => (false, &text[1..]),
        _ => (false, text),
    };
    let mut digits = Vec::new();
    let mut point = false;
    let mut fractional = 0_i64;
    for byte in text.bytes() {
        match byte {
            b'0'..=b'9' => {
                digits.push(byte - b'0');
                if point {
                    fractional = fractional
                        .checked_add(1)
                        .ok_or("decimal mantissa is too long")?;
                }
            }
            b'.' if !point => point = true,
            // Retain the existing quoted Decimal separator syntax.
            b'_' if !digits.is_empty() => {}
            _ => return Err("invalid decimal mantissa"),
        }
    }
    if digits.is_empty() {
        return Err("decimal mantissa has no digits");
    }
    Ok((negative, digits, fractional))
}

fn parse_exponent(text: Option<&str>) -> Result<Option<i64>, &'static str> {
    let Some(text) = text else {
        return Ok(Some(0));
    };
    let (negative, digits) = match text.as_bytes().first() {
        Some(b'-') => (true, &text[1..]),
        Some(b'+') => (false, &text[1..]),
        _ => (false, text),
    };
    if digits.is_empty() {
        return Err("decimal exponent has no digits");
    }
    let mut exponent = Some(0_i64);
    for byte in digits.bytes() {
        if !byte.is_ascii_digit() {
            return Err("invalid decimal exponent");
        }
        // Continue validating after overflow, without exponent-sized work.
        exponent = exponent
            .and_then(|value| value.checked_mul(10))
            .and_then(|value| {
                if negative {
                    value.checked_sub(i64::from(byte - b'0'))
                } else {
                    value.checked_add(i64::from(byte - b'0'))
                }
            });
    }
    Ok(exponent)
}

pub(crate) mod option {
    use super::{Decimal, Deserialize, RawValue, parse};

    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<Option<Decimal>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Option::<Box<RawValue>>::deserialize(deserializer)?
            .map(|raw| parse(raw.get()))
            .transpose()
    }

    #[expect(
        clippy::ref_option,
        reason = "Serde's `with` module contract passes optional fields as `&Option<T>`"
    )]
    pub(crate) fn serialize<S>(value: &Option<Decimal>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match value {
            Some(value) => super::serialize(value, serializer),
            None => serializer.serialize_none(),
        }
    }
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use super::Decimal;

    #[derive(Debug, Deserialize, PartialEq, Serialize)]
    struct ExactNumber {
        #[serde(with = "crate::decimal_serde")]
        value: Decimal,
    }

    #[derive(Debug, Deserialize, PartialEq, Serialize)]
    #[serde(tag = "kind", rename_all = "snake_case")]
    enum TaggedFloat {
        Sample { value: f64 },
    }

    #[test]
    fn exact_decimal_uses_a_raw_json_number_without_rounding() {
        let decoded: ExactNumber =
            serde_json::from_str(r#"{"value":0.1000000000000000000000000001}"#)
                .unwrap_or_else(|error| panic!("exact fixture must decode: {error}"));
        assert_eq!(decoded.value.to_string(), "0.1000000000000000000000000001");
        assert_eq!(
            serde_json::to_string(&decoded)
                .unwrap_or_else(|error| panic!("exact fixture must encode: {error}")),
            r#"{"value":0.1000000000000000000000000001}"#
        );
    }

    #[test]
    fn decimal_support_does_not_change_tagged_float_representation() {
        let value = TaggedFloat::Sample { value: 6001.25 };
        let json = serde_json::to_string(&value)
            .unwrap_or_else(|error| panic!("tagged float must encode: {error}"));
        let decoded = serde_json::from_str::<TaggedFloat>(&json)
            .unwrap_or_else(|error| panic!("tagged float must decode: {error}"));
        assert_eq!(decoded, value);
    }

    #[test]
    fn parser_validates_all_text_instead_of_accepting_a_rounded_prefix() {
        for text in [
            "",
            " ",
            " 1",
            "1 ",
            "NaN",
            "Infinity",
            ".",
            "+",
            "_1",
            "1.2.3",
            "1e",
            "1e+",
            "1e--2",
            "1e2e3",
            "1e2_0",
            "0.123456789012345678901234567890garbage",
            "0.000000000000000000000000000000000000000000x",
        ] {
            let token = serde_json::to_string(text)
                .unwrap_or_else(|error| panic!("quoted fixture: {error}"));
            let decoded = super::parse::<serde_json::Error>(&token);
            assert!(decoded.is_err(), "invalid {text:?} accepted: {decoded:?}");
        }
    }

    #[test]
    fn quoted_decimal_compatibility_and_exact_scientific_cancellation() {
        for (text, expected) in [
            ("+1.25", "1.25"),
            (".5", "0.5"),
            ("1.", "1"),
            ("001.25", "1.25"),
            ("1_2.5_0_", "12.50"),
            ("1__", "1"),
            ("1._5", "1.5"),
            (".5e1", "5"),
            ("1e+000000000000000000000000000000000000002", "100"),
            (
                "0.00000000000000000000000000001e1",
                "0.0000000000000000000000000001",
            ),
        ] {
            let token = serde_json::to_string(text)
                .unwrap_or_else(|error| panic!("quoted fixture: {error}"));
            let decoded = super::parse::<serde_json::Error>(&token)
                .unwrap_or_else(|error| panic!("{text}: {error}"));
            assert_eq!(decoded.to_string(), expected, "{text}");
        }
    }

    #[test]
    fn hostile_exponents_do_not_require_exponent_sized_work() {
        let digits = "9".repeat(10_000);
        for text in [format!("1e{digits}"), format!("1e-{digits}")] {
            let token = serde_json::to_string(&text)
                .unwrap_or_else(|error| panic!("quoted exponent: {error}"));
            assert!(super::parse::<serde_json::Error>(&token).is_err());
        }
        for text in [format!("-0e{digits}"), format!("-0e-{digits}")] {
            let token =
                serde_json::to_string(&text).unwrap_or_else(|error| panic!("quoted zero: {error}"));
            let decoded = super::parse::<serde_json::Error>(&token)
                .unwrap_or_else(|error| panic!("exact zero: {error}"));
            assert!(decoded.is_zero());
            assert!(decoded.is_sign_negative());
        }
        let invalid_zero = serde_json::to_string(&format!("0e{digits}x"))
            .unwrap_or_else(|error| panic!("quoted invalid zero: {error}"));
        assert!(super::parse::<serde_json::Error>(&invalid_zero).is_err());
        let cancelled = serde_json::to_string(&format!("1{}e-10000", "0".repeat(10_000)))
            .unwrap_or_else(|error| panic!("quoted cancelled coefficient: {error}"));
        assert_eq!(
            super::parse::<serde_json::Error>(&cancelled)
                .unwrap_or_else(|error| panic!("cancelled coefficient: {error}")),
            Decimal::ONE
        );
    }

    #[test]
    fn signed_zero_preserves_its_sign_and_representable_scale() {
        for (text, negative, scale) in [
            ("0", false, 0),
            ("-0", true, 0),
            ("-0.0000", true, 4),
            ("-0.0000e2", true, 2),
            ("0e-28", false, 28),
        ] {
            let token =
                serde_json::to_string(text).unwrap_or_else(|error| panic!("quoted zero: {error}"));
            let decoded = super::parse::<serde_json::Error>(&token)
                .unwrap_or_else(|error| panic!("zero: {error}"));
            assert!(decoded.is_zero());
            assert_eq!(decoded.is_sign_negative(), negative, "{text}");
            assert_eq!(decoded.scale(), scale, "{text}");
        }
    }
}
