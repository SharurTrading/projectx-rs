// SPDX-FileCopyrightText: 2026 Kevin Monaghan
// SPDX-License-Identifier: MIT

//! Validated provider identifier types.

use std::{fmt, str::FromStr};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};

use crate::Error;

macro_rules! numeric_id {
    ($name:ident, $kind:literal, $repr:ty) => {
        #[doc = concat!("Validated ProjectX ", $kind, ".")]
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        #[repr(transparent)]
        pub struct $name($repr);

        impl $name {
            #[doc = concat!("Creates a ProjectX ", $kind, ".")]
            ///
            /// # Errors
            ///
            /// Returns an error unless the provider identifier is positive.
            pub fn new(value: $repr) -> Result<Self, Error> {
                if value <= 0 {
                    return Err(Error::InvalidIdentifier {
                        kind: $kind,
                        reason: "value must be positive",
                    });
                }
                Ok(Self(value))
            }

            #[doc = concat!("Returns the raw ProjectX ", $kind, ".")]
            #[must_use]
            pub const fn get(self) -> $repr {
                self.0
            }
        }

        impl TryFrom<$repr> for $name {
            type Error = Error;

            fn try_from(value: $repr) -> Result<Self, Self::Error> {
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
                self.0.serialize(serializer)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let raw = <$repr>::deserialize(deserializer)?;
                Self::new(raw).map_err(D::Error::custom)
            }
        }
    };
}

macro_rules! string_id {
    ($name:ident, $kind:literal) => {
        #[doc = concat!("Validated ProjectX ", $kind, ".")]
        #[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        #[repr(transparent)]
        pub struct $name(String);

        impl $name {
            #[doc = concat!("Creates a ProjectX ", $kind, ".")]
            ///
            /// # Errors
            ///
            /// Returns an error for an empty identifier or one containing
            /// whitespace or control characters.
            pub fn new(value: impl Into<String>) -> Result<Self, Error> {
                let value = value.into();
                if value.is_empty()
                    || value
                        .chars()
                        .any(|character| character.is_whitespace() || character.is_control())
                {
                    return Err(Error::InvalidIdentifier {
                        kind: $kind,
                        reason: "value must be non-empty and contain no whitespace or control characters",
                    });
                }
                Ok(Self(value))
            }

            #[doc = concat!("Borrows the raw ProjectX ", $kind, ".")]
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                self.as_str()
            }
        }

        impl FromStr for $name {
            type Err = Error;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::new(value)
            }
        }

        impl TryFrom<String> for $name {
            type Error = Error;

            fn try_from(value: String) -> Result<Self, Self::Error> {
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

numeric_id!(AccountId, "account identifier", i32);
numeric_id!(OrderId, "order identifier", i64);
numeric_id!(PositionId, "position identifier", i32);
numeric_id!(TradeId, "trade identifier", i64);
string_id!(ContractId, "contract identifier");
string_id!(SymbolId, "symbol identifier");
