// SPDX-FileCopyrightText: 2026 Kevin Monaghan
// SPDX-License-Identifier: MIT-0

//! Validated provider timestamp type.

use std::{fmt, str::FromStr};

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

/// An absolute provider timestamp parsed from an ISO-8601/RFC-3339 value.
///
/// Parsing normalizes equivalent offsets to one canonical instant. Serialization
/// emits the canonical representation accepted by the Gateway API.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct Timestamp(jiff::Timestamp);

impl Timestamp {
    /// Parses an absolute provider timestamp.
    ///
    /// # Errors
    ///
    /// Returns [`TimestampError`] when the input does not contain a valid
    /// absolute ISO-8601/RFC-3339 timestamp with an offset.
    pub fn new(value: &str) -> Result<Self, TimestampError> {
        value.parse()
    }

    /// Borrows the validated Jiff instant for allocation-free time arithmetic.
    #[must_use]
    pub const fn as_jiff(&self) -> &jiff::Timestamp {
        &self.0
    }
}

impl From<jiff::Timestamp> for Timestamp {
    fn from(value: jiff::Timestamp) -> Self {
        Self(value)
    }
}

impl From<Timestamp> for jiff::Timestamp {
    fn from(value: Timestamp) -> Self {
        value.0
    }
}

impl FromStr for Timestamp {
    type Err = TimestampError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value
            .parse::<jiff::Timestamp>()
            .map(Self)
            .map_err(|_| TimestampError)
    }
}

impl TryFrom<String> for Timestamp {
    type Error = TimestampError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl Serialize for Timestamp {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Timestamp {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let timestamp = jiff::Timestamp::deserialize(deserializer)?;
        Ok(Self(timestamp))
    }
}

/// A string was not a valid absolute provider timestamp.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("timestamp must be an absolute ISO-8601/RFC-3339 value with an offset")]
pub struct TimestampError;

/// A provider civil date parsed from an ISO-8601 `YYYY-MM-DD` value.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct ProviderDate(jiff::civil::Date);

impl ProviderDate {
    /// Parses a provider civil date.
    ///
    /// # Errors
    ///
    /// Returns [`ProviderDateError`] when the input is not a valid ISO-8601
    /// calendar date.
    pub fn new(value: &str) -> Result<Self, ProviderDateError> {
        value.parse()
    }

    /// Borrows the validated Jiff civil date.
    #[must_use]
    pub const fn as_jiff(&self) -> &jiff::civil::Date {
        &self.0
    }
}

impl From<jiff::civil::Date> for ProviderDate {
    fn from(value: jiff::civil::Date) -> Self {
        Self(value)
    }
}

impl From<ProviderDate> for jiff::civil::Date {
    fn from(value: ProviderDate) -> Self {
        value.0
    }
}

impl FromStr for ProviderDate {
    type Err = ProviderDateError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value
            .parse::<jiff::civil::Date>()
            .map(Self)
            .map_err(|_| ProviderDateError)
    }
}

impl TryFrom<String> for ProviderDate {
    type Error = ProviderDateError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl fmt::Display for ProviderDate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl Serialize for ProviderDate {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ProviderDate {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let date = jiff::civil::Date::deserialize(deserializer)?;
        Ok(Self(date))
    }
}

/// A string was not a valid provider civil date.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("provider date must be a valid ISO-8601 calendar date")]
pub struct ProviderDateError;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_rejects_malformed_or_offset_free_values() {
        assert!(Timestamp::new("not-a-date").is_err());
        assert!(Timestamp::new("2026-01-01T00:00:00").is_err());
    }

    #[test]
    fn timestamp_round_trips_a_canonical_instant() {
        let timestamp = Timestamp::new("2026-01-01T08:00:00+08:00")
            .unwrap_or_else(|error| panic!("fixture timestamp must be valid: {error}"));
        let encoded = serde_json::to_string(&timestamp)
            .unwrap_or_else(|error| panic!("fixture timestamp must serialize: {error}"));
        assert_eq!(encoded, "\"2026-01-01T00:00:00Z\"");
        let decoded: Timestamp = serde_json::from_str(&encoded)
            .unwrap_or_else(|error| panic!("fixture timestamp must deserialize: {error}"));
        assert_eq!(decoded, timestamp);

        let jiff_timestamp: jiff::Timestamp = timestamp.into();
        assert_eq!(Timestamp::from(jiff_timestamp), timestamp);
    }

    #[test]
    fn provider_date_validates_and_round_trips() {
        let date = ProviderDate::new("2026-02-28")
            .unwrap_or_else(|error| panic!("fixture date must be valid: {error}"));
        assert_eq!(date.to_string(), "2026-02-28");
        assert!(ProviderDate::new("2026-02-29").is_err());
        assert!(serde_json::from_str::<ProviderDate>("\"not-a-date\"").is_err());
        let jiff_date: jiff::civil::Date = date.into();
        assert_eq!(ProviderDate::from(jiff_date), date);
    }
}
