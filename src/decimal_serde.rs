// SPDX-FileCopyrightText: 2026 Kevin Monaghan
// SPDX-License-Identifier: MIT-0

//! Exact JSON-number serialization for provider decimal fields.
//!
//! This module uses `serde_json`'s raw-value boundary instead of enabling the
//! global `arbitrary_precision` feature. The latter participates in Cargo
//! feature unification and can change how unrelated downstream crates decode
//! internally tagged JSON enums.

use std::str::FromStr;

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

    Decimal::from_str(value)
        .or_else(|_| Decimal::from_scientific(value))
        .map_err(E::custom)
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
}
