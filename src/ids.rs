// SPDX-FileCopyrightText: 2026 Kevin Monaghan
// SPDX-License-Identifier: MIT

//! Validated provider identifier types.

use std::{fmt, str::FromStr};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};

use crate::Error;

macro_rules! numeric_id {
    ($name:ident, $kind:literal) => {
        #[doc = concat!("Validated ProjectX ", $kind, ".")]
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(i64);

        impl $name {
            #[doc = concat!("Creates a ProjectX ", $kind, ".")]
            ///
            /// # Errors
            ///
            /// Returns an error unless the provider identifier is positive.
            pub fn new(value: i64) -> Result<Self, Error> {
                if value <= 0 {
                    return Err(Error::InvalidIdentifier {
                        kind: $kind,
                        reason: "value must be positive",
                    });
                }
                Ok(Self(value))
            }

            #[doc = concat!("Returns the raw ProjectX ", $kind, ".")]
            pub const fn get(self) -> i64 {
                self.0
            }
        }

        impl TryFrom<i64> for $name {
            type Error = Error;

            fn try_from(value: i64) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(f)
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serializer.serialize_i64(self.0)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let raw = i64::deserialize(deserializer)?;
                Self::new(raw).map_err(D::Error::custom)
            }
        }
    };
}

macro_rules! string_id {
    ($name:ident, $kind:literal) => {
        #[doc = concat!("Validated ProjectX ", $kind, ".")]
        #[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(String);

        impl $name {
            #[doc = concat!("Creates a ProjectX ", $kind, ".")]
            ///
            /// # Errors
            ///
            /// Returns an error for an empty or whitespace-padded identifier.
            pub fn new(value: impl Into<String>) -> Result<Self, Error> {
                let value = value.into();
                if value.is_empty() || value.trim() != value {
                    return Err(Error::InvalidIdentifier {
                        kind: $kind,
                        reason: "value must be non-empty and unpadded",
                    });
                }
                Ok(Self(value))
            }

            #[doc = concat!("Borrows the raw ProjectX ", $kind, ".")]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl FromStr for $name {
            type Err = Error;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::new(value)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(f)
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serializer.serialize_str(&self.0)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let raw = String::deserialize(deserializer)?;
                Self::new(raw).map_err(D::Error::custom)
            }
        }
    };
}

numeric_id!(AccountId, "account identifier");
numeric_id!(OrderId, "order identifier");
numeric_id!(PositionId, "position identifier");
numeric_id!(TradeId, "trade identifier");
string_id!(ContractId, "contract identifier");
string_id!(SymbolId, "symbol identifier");
