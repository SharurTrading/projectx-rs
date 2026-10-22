// SPDX-FileCopyrightText: 2026 Kevin Monaghan
// SPDX-License-Identifier: MIT-0

//! `SignalR` frame decoding and invocation encoding with exact provider tokens.

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, value::RawValue};
use tokio_tungstenite::tungstenite::Message;

use crate::ContractId;

use super::error::RealtimeError;

pub(super) const SIGNALR_TERMINATOR: char = '\u{001e}';

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct OutboundInvocation<'a> {
    #[serde(rename = "type")]
    message_type: u8,
    invocation_id: &'a str,
    target: &'a str,
    arguments: &'a [Value],
}

/// A borrowed record header; payloads stay raw until the owning arm decodes them.
#[derive(Deserialize)]
pub(super) struct SignalRFrame<'a> {
    #[serde(rename = "type", borrow)]
    message_type: Option<&'a RawValue>,
    #[serde(borrow)]
    target: Option<&'a RawValue>,
    #[serde(borrow)]
    arguments: Option<&'a RawValue>,
}

impl SignalRFrame<'_> {
    pub(super) fn message_type(&self) -> Result<u64, RealtimeError> {
        let raw = self
            .message_type
            .ok_or(RealtimeError::Protocol("message type is missing"))?;
        serde_json::from_str(raw.get())
            .map_err(|_| RealtimeError::Protocol("message type was not an unsigned integer"))
    }
}

/// A decoded `SignalR` invocation frame with exact provider JSON tokens.
/// Equality compares the target, contract identifier, and exact payload text.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct SignalRInvocation {
    target: String,
    contract_id: Option<ContractId>,
    payload: Box<RawValue>,
}

impl PartialEq for SignalRInvocation {
    fn eq(&self, other: &Self) -> bool {
        self.target == other.target
            && self.contract_id == other.contract_id
            && self.payload.get() == other.payload.get()
    }
}

impl SignalRInvocation {
    /// Decodes a type-1 `SignalR` invocation from an in-memory JSON value.
    ///
    /// Other unsigned message types return `Ok(None)`. Missing or malformed
    /// types are errors. Precision already lost when building the value cannot
    /// be recovered; prefer [`Self::from_json`] for provider records.
    ///
    /// # Errors
    /// Returns an error for an invalid type-1 target, contract, or argument list,
    /// or when the value cannot be serialized.
    pub fn from_value(value: &Value) -> Result<Option<Self>, RealtimeError> {
        value
            .get("type")
            .ok_or(RealtimeError::Protocol("message type is missing"))?
            .as_u64()
            .ok_or(RealtimeError::Protocol(
                "message type was not an unsigned integer",
            ))?;
        let raw = serde_json::value::to_raw_value(value).map_err(RealtimeError::Decode)?;
        Self::from_json(raw.get())
    }

    /// Decodes a type-1 `SignalR` invocation directly from its JSON record.
    ///
    /// The original payload is retained without converting its numbers to
    /// floating point. Other unsigned message types return `Ok(None)`.
    ///
    /// # Errors
    /// Returns an error when the record is malformed or has an invalid target,
    /// contract identifier, or payload argument list.
    pub fn from_json(json: &str) -> Result<Option<Self>, RealtimeError> {
        let frame = serde_json::from_str(json).map_err(RealtimeError::Decode)?;
        Self::from_frame(&frame)
    }

    pub(super) fn from_frame(frame: &SignalRFrame<'_>) -> Result<Option<Self>, RealtimeError> {
        if frame.message_type()? != 1 {
            return Ok(None);
        }
        let target = frame
            .target
            .ok_or(RealtimeError::Protocol("invocation target is missing"))?;
        let target = serde_json::from_str(target.get()).map_err(RealtimeError::Decode)?;
        let arguments = frame
            .arguments
            .ok_or(RealtimeError::Protocol("invocation arguments are missing"))?;
        let arguments: Vec<&RawValue> =
            serde_json::from_str(arguments.get()).map_err(RealtimeError::Decode)?;
        let (contract_id, payload) = match arguments.as_slice() {
            [payload] => (None, *payload),
            [contract, payload] => {
                let contract: String = serde_json::from_str(contract.get()).map_err(|_| {
                    RealtimeError::Protocol("market contract identifier was not a string")
                })?;
                let contract_id = ContractId::new(contract)
                    .map_err(|_| RealtimeError::Protocol("contract identifier is invalid"))?;
                (Some(contract_id), *payload)
            }
            [] => return Err(RealtimeError::Protocol("invocation payload is missing")),
            _ => {
                return Err(RealtimeError::Protocol(
                    "invocation contained too many arguments",
                ));
            }
        };
        Ok(Some(Self {
            target,
            contract_id,
            payload: payload.to_owned(),
        }))
    }

    /// Returns the provider invocation target.
    #[must_use]
    pub fn target(&self) -> &str {
        &self.target
    }

    /// Returns the market contract argument, when supplied.
    #[must_use]
    pub fn contract_id(&self) -> Option<&ContractId> {
        self.contract_id.as_ref()
    }

    /// Returns the exact provider payload. [`RawValue::get`] exposes its JSON text.
    #[must_use]
    pub fn payload(&self) -> &RawValue {
        &self.payload
    }

    /// Returns the exact event entity, unwrapping an object or array in `data`.
    ///
    /// # Errors
    /// Returns an error if the raw object cannot be decoded as a data envelope.
    pub fn entity(&self) -> Result<&RawValue, RealtimeError> {
        #[derive(Deserialize)]
        struct Envelope<'a> {
            #[serde(borrow)]
            data: Option<&'a RawValue>,
        }
        if self.payload.get().starts_with('{') {
            let envelope: Envelope<'_> =
                serde_json::from_str(self.payload.get()).map_err(RealtimeError::Decode)?;
            if let Some(data) = envelope.data
                && (data.get().starts_with('{') || data.get().starts_with('['))
            {
                return Ok(data);
            }
        }
        Ok(&self.payload)
    }

    /// Deserializes the event entity into a provider model using exact tokens.
    ///
    /// # Errors
    /// Returns an error when the provider payload does not match `T`.
    pub fn decode<T>(&self) -> Result<T, RealtimeError>
    where
        T: DeserializeOwned,
    {
        serde_json::from_str(self.entity()?.get()).map_err(RealtimeError::Decode)
    }

    /// Deserializes a single entity or every non-null entry in an entity array.
    ///
    /// Each entry is decoded independently so one malformed provider record
    /// does not hide other records. `ProjectX` null padding is omitted.
    #[must_use]
    pub fn decode_batch<T>(&self) -> Vec<Result<T, RealtimeError>>
    where
        T: DeserializeOwned,
    {
        let entity = match self.entity() {
            Ok(entity) => entity,
            Err(error) => return vec![Err(error)],
        };
        if !entity.get().starts_with('[') {
            return vec![serde_json::from_str(entity.get()).map_err(RealtimeError::Decode)];
        }
        let values = match serde_json::from_str::<Vec<&RawValue>>(entity.get()) {
            Ok(values) => values,
            Err(error) => return vec![Err(RealtimeError::Decode(error))],
        };
        values
            .into_iter()
            .filter(|value| value.get() != "null")
            .map(|value| serde_json::from_str(value.get()).map_err(RealtimeError::Decode))
            .collect()
    }
}

pub(super) fn encode_invocation(
    invocation_id: &str,
    target: &str,
    arguments: &[Value],
) -> Result<Message, RealtimeError> {
    let payload = OutboundInvocation {
        message_type: 1,
        invocation_id,
        target,
        arguments,
    };
    let mut text = serde_json::to_string(&payload).map_err(RealtimeError::Encode)?;
    text.push(SIGNALR_TERMINATOR);
    Ok(Message::Text(text.into()))
}

pub(super) fn completion(value: &Value) -> Result<(&str, Result<(), ()>), RealtimeError> {
    let object = value
        .as_object()
        .ok_or(RealtimeError::Protocol("completion was not an object"))?;
    let invocation_id =
        object
            .get("invocationId")
            .and_then(Value::as_str)
            .ok_or(RealtimeError::Protocol(
                "completion invocationId was not a string",
            ))?;
    if object.contains_key("error") && object.contains_key("result") {
        return Err(RealtimeError::Protocol(
            "completion contained both error and result",
        ));
    }
    let result = match object.get("error") {
        Some(Value::String(_)) => Err(()),
        Some(_) => {
            return Err(RealtimeError::Protocol("completion error was not a string"));
        }
        None => Ok(()),
    };
    Ok((invocation_id, result))
}
