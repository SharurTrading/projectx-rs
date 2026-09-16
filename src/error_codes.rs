// SPDX-FileCopyrightText: 2026 Kevin Monaghan
// SPDX-License-Identifier: MIT-0

//! Provider-published REST error-code tables.
//!
//! Every authenticated REST response carries an `errorCode` whose meaning is
//! defined per response contract. The provider publishes a name for each code
//! in its API reference (<https://api.topstepx.com/swagger/index.html>), and
//! this module carries those tables so a rejection reaches the caller with the
//! provider's own text instead of a bare number.
//!
//! The tables only carry the provider's published code names. Its free-form
//! `errorMessage` is untrusted remote text and is never exposed or logged.

/// The provider's published error-code table for one REST response contract.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ErrorCodeTable {
    /// `/api/Auth/loginKey` and `/api/Auth/loginApp`.
    Login,
    /// `/api/Auth/logout`.
    Logout,
    /// `/api/Auth/validate`.
    Validate,
    /// `/api/Account/search`, `/api/Contract/available`, and
    /// `/api/Contract/search`, whose published tables define only code `0`.
    SuccessOnly,
    /// `/api/Contract/searchById`.
    ContractSearchById,
    /// `/api/History/retrieveBars`.
    Bars,
    /// `/api/Order/search`, `/api/Order/searchOpen`, and `/api/Order/v2/query`.
    OrderSearch,
    /// `/api/Order/searchById`.
    OrderSearchById,
    /// `/api/Order/place`.
    OrderPlacement,
    /// `/api/Order/cancel`.
    OrderCancellation,
    /// `/api/Order/modify`.
    OrderModification,
    /// `/api/Position/searchOpen`.
    PositionSearch,
    /// `/api/Position/closeContract`.
    PositionClose,
    /// `/api/Position/partialCloseContract`.
    PartialPositionClose,
    /// `/api/Trade/search`.
    TradeSearch,
}

impl ErrorCodeTable {
    /// Returns the provider's published name for `code`.
    ///
    /// The name is `None` when this contract's table does not define the code,
    /// which includes codes the provider adds after these tables were
    /// transcribed. Rejections never carry code `0`, so the published `Success`
    /// name is only reachable through the table itself.
    pub(crate) fn name(self, code: i32) -> Option<&'static str> {
        usize::try_from(code)
            .ok()
            .and_then(|index| self.published_names().get(index))
            .copied()
    }

    /// Returns this contract's published names in code order, so a name's
    /// position is the numeric `errorCode` it describes. Code `0` is `Success`
    /// in every table.
    const fn published_names(self) -> &'static [&'static str] {
        match self {
            Self::Login => &[
                "Success",
                "UserNotFound",
                "PasswordVerificationFailed",
                "InvalidCredentials",
                "AppNotFound",
                "AppVerificationFailed",
                "InvalidDevice",
                "AgreementsNotSigned",
                "UnknownError",
                "ApiSubscriptionNotFound",
                "ApiKeyAuthenticationDisabled",
            ],
            Self::Logout => &["Success", "InvalidSession", "UnknownError"],
            Self::Validate => &[
                "Success",
                "InvalidSession",
                "SessionNotFound",
                "ExpiredToken",
                "UnknownError",
            ],
            Self::SuccessOnly => &["Success"],
            Self::ContractSearchById => &["Success", "ContractNotFound"],
            Self::Bars => &[
                "Success",
                "ContractNotFound",
                "UnitInvalid",
                "UnitNumberInvalid",
                "LimitInvalid",
            ],
            Self::OrderSearch | Self::PositionSearch | Self::TradeSearch => {
                &["Success", "AccountNotFound"]
            }
            Self::OrderSearchById => &["Success", "OrderNotFound"],
            Self::OrderPlacement => &[
                "Success",
                "AccountNotFound",
                "OrderRejected",
                "InsufficientFunds",
                "AccountViolation",
                "OutsideTradingHours",
                "OrderPending",
                "UnknownError",
                "ContractNotFound",
                "ContractNotActive",
                "AccountRejected",
            ],
            Self::OrderCancellation => &[
                "Success",
                "AccountNotFound",
                "OrderNotFound",
                "Rejected",
                "Pending",
                "UnknownError",
                "AccountRejected",
            ],
            Self::OrderModification => &[
                "Success",
                "AccountNotFound",
                "OrderNotFound",
                "Rejected",
                "Pending",
                "UnknownError",
                "AccountRejected",
                "ContractNotFound",
            ],
            Self::PositionClose => &[
                "Success",
                "AccountNotFound",
                "PositionNotFound",
                "ContractNotFound",
                "ContractNotActive",
                "OrderRejected",
                "OrderPending",
                "UnknownError",
                "AccountRejected",
            ],
            Self::PartialPositionClose => &[
                "Success",
                "AccountNotFound",
                "PositionNotFound",
                "ContractNotFound",
                "ContractNotActive",
                "InvalidCloseSize",
                "OrderRejected",
                "OrderPending",
                "UnknownError",
                "AccountRejected",
            ],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ErrorCodeTable;

    #[test]
    fn published_names_are_indexed_by_error_code() {
        let cases: &[(ErrorCodeTable, i32, &str)] = &[
            (ErrorCodeTable::Login, 0, "Success"),
            (ErrorCodeTable::Login, 1, "UserNotFound"),
            (ErrorCodeTable::Login, 10, "ApiKeyAuthenticationDisabled"),
            (ErrorCodeTable::Logout, 1, "InvalidSession"),
            (ErrorCodeTable::Validate, 3, "ExpiredToken"),
            (ErrorCodeTable::SuccessOnly, 0, "Success"),
            (ErrorCodeTable::ContractSearchById, 1, "ContractNotFound"),
            (ErrorCodeTable::Bars, 3, "UnitNumberInvalid"),
            (ErrorCodeTable::OrderSearch, 1, "AccountNotFound"),
            (ErrorCodeTable::OrderSearchById, 1, "OrderNotFound"),
            (ErrorCodeTable::OrderPlacement, 2, "OrderRejected"),
            (ErrorCodeTable::OrderPlacement, 6, "OrderPending"),
            (ErrorCodeTable::OrderPlacement, 10, "AccountRejected"),
            (ErrorCodeTable::OrderCancellation, 2, "OrderNotFound"),
            (ErrorCodeTable::OrderCancellation, 3, "Rejected"),
            (ErrorCodeTable::OrderModification, 7, "ContractNotFound"),
            (ErrorCodeTable::PositionSearch, 1, "AccountNotFound"),
            (ErrorCodeTable::PositionClose, 5, "OrderRejected"),
            (ErrorCodeTable::PartialPositionClose, 5, "InvalidCloseSize"),
            (ErrorCodeTable::PartialPositionClose, 9, "AccountRejected"),
            (ErrorCodeTable::TradeSearch, 1, "AccountNotFound"),
        ];

        for &(table, code, expected) in cases {
            assert_eq!(
                table.name(code),
                Some(expected),
                "{table:?} code {code} must be named {expected}"
            );
        }
    }

    #[test]
    fn undocumented_and_impossible_codes_have_no_name() {
        let documented: &[(ErrorCodeTable, i32)] = &[
            (ErrorCodeTable::Login, 10),
            (ErrorCodeTable::Logout, 2),
            (ErrorCodeTable::Validate, 4),
            (ErrorCodeTable::SuccessOnly, 0),
            (ErrorCodeTable::ContractSearchById, 1),
            (ErrorCodeTable::Bars, 4),
            (ErrorCodeTable::OrderSearch, 1),
            (ErrorCodeTable::OrderSearchById, 1),
            (ErrorCodeTable::OrderPlacement, 10),
            (ErrorCodeTable::OrderCancellation, 6),
            (ErrorCodeTable::OrderModification, 7),
            (ErrorCodeTable::PositionSearch, 1),
            (ErrorCodeTable::PositionClose, 8),
            (ErrorCodeTable::PartialPositionClose, 9),
            (ErrorCodeTable::TradeSearch, 1),
        ];

        for &(table, last_documented) in documented {
            let undocumented = last_documented.saturating_add(1);
            assert_eq!(
                table.name(undocumented),
                None,
                "{table:?} code {undocumented} must stay unnamed"
            );
            assert_eq!(table.name(-1), None, "{table:?} codes are never negative");
        }
    }
}
