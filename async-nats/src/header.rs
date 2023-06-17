// Copyright 2020-2023 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// NOTE(thomastaylor312): This clippy lint is coming from serialize and deserialize and is likely a
// false positive due to the bytes crate, see
// https://rust-lang.github.io/rust-clippy/master/index.html#/mutable_key_type for more details.
// Sorry to make this global to this module, rather than on the `HeaderMap` struct, but because it
// is coming from the derive, it didn't work to set it on the struct.
#![allow(clippy::mutable_key_type)]

//! NATS [Message][crate::Message] headers, modeled loosely after the [http::header] crate.

use std::{collections::HashMap, fmt, slice, str::FromStr};

use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// A struct for handling NATS headers.
/// Has a similar API to [http::header], but properly serializes and deserializes
/// according to NATS requirements.
///
/// # Examples
///
/// ```
/// # #[tokio::main]
/// # async fn main() -> Result<(), async_nats::Error> {
/// let client = async_nats::connect("demo.nats.io").await?;
/// let mut headers = async_nats::HeaderMap::new();
/// headers.insert("Key", "Value");
/// client
///     .publish_with_headers("subject".to_string(), headers, "payload".into())
///     .await?;
/// # Ok(())
/// # }
/// ```

#[derive(Clone, PartialEq, Eq, Debug, Default, Deserialize, Serialize)]
pub struct HeaderMap {
    inner: HashMap<HeaderName, HeaderValue>,
}

impl FromIterator<(HeaderName, HeaderValue)> for HeaderMap {
    fn from_iter<T: IntoIterator<Item = (HeaderName, HeaderValue)>>(iter: T) -> Self {
        let mut header_map = HeaderMap::new();
        for (key, value) in iter {
            header_map.insert(key, value);
        }
        header_map
    }
}

impl HeaderMap {
    pub fn iter(&self) -> std::collections::hash_map::Iter<'_, HeaderName, HeaderValue> {
        self.inner.iter()
    }
}

impl HeaderMap {
    /// Create an empty `HeaderMap`.
    ///
    /// # Examples
    ///
    /// ```
    /// # use async_nats::HeaderMap;
    /// let map = HeaderMap::new();
    ///
    /// assert!(map.is_empty());
    /// ```
    pub fn new() -> Self {
        HeaderMap::default()
    }

    /// Returns true if the map contains no elements.
    ///
    /// # Examples
    ///
    /// ```
    /// # use async_nats::HeaderMap;
    /// # use async_nats::header::NATS_SUBJECT;
    /// let mut map = HeaderMap::new();
    ///
    /// assert!(map.is_empty());
    ///
    /// map.insert(NATS_SUBJECT, "FOO.BAR");
    ///
    /// assert!(!map.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

impl HeaderMap {
    /// Inserts a new value to a [HeaderMap].
    ///
    /// # Examples
    ///
    /// ```
    /// use async_nats::HeaderMap;
    ///
    /// let mut headers = HeaderMap::new();
    /// headers.insert("Key", "Value");
    /// ```
    pub fn insert<K: IntoHeaderName, V: IntoHeaderValue>(&mut self, name: K, value: V) {
        self.inner
            .insert(name.into_header_name(), value.into_header_value());
    }

    /// Appends a new value to the list of values to a given key.
    /// If the key did not exist, it will be inserted with provided value.
    ///
    /// # Examples
    ///
    /// ```
    /// use async_nats::HeaderMap;
    ///
    /// let mut headers = HeaderMap::new();
    /// headers.append("Key", "Value");
    /// headers.append("Key", "Another");
    /// ```
    pub fn append<K: IntoHeaderName, V: ToString>(&mut self, name: K, value: V) {
        let key = name.into_header_name();
        let v = self.inner.get_mut(&key);
        match v {
            Some(v) => {
                v.value.push(value.to_string());
            }
            None => {
                self.insert(key, value.to_string().into_header_value());
            }
        }
    }

    /// Gets a value for a given key. If key is not found, [Option::None] is returned.
    ///
    /// # Examples
    ///
    /// ```
    /// # use async_nats::HeaderMap;
    ///
    /// let mut headers = HeaderMap::new();
    /// headers.append("Key", "Value");
    /// let key = headers.get("Key").unwrap();
    /// ```
    pub fn get<T: IntoHeaderName>(&self, name: T) -> Option<&HeaderValue> {
        self.inner.get(&name.into_header_name())
    }

    pub(crate) fn to_bytes(&self) -> Vec<u8> {
        let mut buf = vec![];
        buf.extend_from_slice(b"NATS/1.0\r\n");
        for (k, vs) in &self.inner {
            for v in vs.iter() {
                buf.extend_from_slice(k.as_str().as_bytes());
                buf.extend_from_slice(b": ");
                buf.extend_from_slice(v.as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
        }
        buf.extend_from_slice(b"\r\n");
        buf
    }
}

/// A struct representing value of a given header.
/// Can contain one or more elements.
///
/// # Examples
///
/// ```
/// # use async_nats::HeaderMap;
///
/// let mut headers = HeaderMap::new();
/// headers.insert("Key", "Value");
/// headers.insert("Another", "AnotherValue");
/// ```
#[derive(Clone, PartialEq, Eq, Debug, Default, Serialize, Deserialize)]
pub struct HeaderValue {
    value: Vec<String>,
}

impl ToString for HeaderValue {
    fn to_string(&self) -> String {
        self.iter()
            .next()
            .cloned()
            .unwrap_or_else(|| String::from(""))
    }
}

impl From<HeaderValue> for String {
    fn from(header: HeaderValue) -> Self {
        header.to_string()
    }
}
impl From<&HeaderValue> for String {
    fn from(header: &HeaderValue) -> Self {
        header.to_string()
    }
}

impl<'a> From<&'a HeaderValue> for &'a str {
    fn from(header: &'a HeaderValue) -> Self {
        header
            .iter()
            .next()
            .map(|v| v.as_str())
            .unwrap_or_else(|| "")
    }
}

impl FromStr for HeaderValue {
    type Err = ParseHeaderValueError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.contains(['\r', '\n']) {
            return Err(ParseHeaderValueError);
        }

        let mut set = HeaderValue::new();
        set.value.push(s.to_string());
        Ok(set)
    }
}

impl From<u64> for HeaderValue {
    fn from(v: u64) -> Self {
        let mut set = HeaderValue::new();
        set.value.push(v.to_string());
        set
    }
}
impl From<&str> for HeaderValue {
    fn from(v: &str) -> Self {
        let mut set = HeaderValue::new();
        set.value.push(v.to_string());
        set
    }
}

impl IntoIterator for HeaderValue {
    type Item = String;

    type IntoIter = std::vec::IntoIter<String>;

    fn into_iter(self) -> Self::IntoIter {
        self.value.into_iter()
    }
}

impl HeaderValue {
    pub fn new() -> HeaderValue {
        HeaderValue::default()
    }

    pub fn iter(&self) -> slice::Iter<String> {
        self.value.iter()
    }

    pub fn as_str(&self) -> &str {
        self.into()
    }
}

#[derive(Debug, Clone)]
pub struct ParseHeaderValueError;

impl fmt::Display for ParseHeaderValueError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            r#"invalid character found in header value (value cannot contain '\r' or '\n')"#
        )
    }
}

impl std::error::Error for ParseHeaderValueError {}

pub trait IntoHeaderName {
    fn into_header_name(self) -> HeaderName;
}

impl IntoHeaderName for &str {
    fn into_header_name(self) -> HeaderName {
        HeaderName {
            inner: HeaderRepr::Custom(self.into()),
        }
    }
}

impl IntoHeaderName for HeaderName {
    fn into_header_name(self) -> HeaderName {
        self
    }
}

pub trait IntoHeaderValue {
    fn into_header_value(self) -> HeaderValue;
}
impl IntoHeaderValue for &str {
    fn into_header_value(self) -> HeaderValue {
        let mut set = HeaderValue::new();
        set.value.push(self.to_string());
        set
    }
}

impl IntoHeaderValue for HeaderValue {
    fn into_header_value(self) -> HeaderValue {
        self
    }
}

macro_rules! standard_headers {
    (
        $(
            $(#[$docs:meta])*
            ($variant:ident, $constant:ident, $bytes:literal);
        )+
    ) => {
        #[allow(clippy::enum_variant_names)]
        #[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Serialize, Deserialize)]
        enum StandardHeader {
            $(
                $variant,
            )+
        }

        $(
            $(#[$docs])*
            pub const $constant: HeaderName = HeaderName {
                inner: HeaderRepr::Standard(StandardHeader::$variant),
            };
        )+

        impl StandardHeader {
            #[inline]
            fn as_str(&self) -> &'static str {
                match *self {
                    $(
                    StandardHeader::$variant => unsafe { std::str::from_utf8_unchecked( $bytes ) },
                    )+
                }
            }

            const fn from_bytes(bytes: &[u8]) -> Option<StandardHeader> {
                match bytes {
                    $(
                        $bytes => Some(StandardHeader::$variant),
                    )+
                    _ => None,
                }
            }
        }

        #[cfg(test)]
        mod standard_header_tests {
            use super::HeaderName;
            use std::str::{self, FromStr};

            const TEST_HEADERS: &'static [(&'static HeaderName, &'static [u8])] = &[
                $(
                (&super::$constant, $bytes),
                )+
            ];

            #[test]
            fn from_str() {
                for &(header, bytes) in TEST_HEADERS {
                    let utf8 = str::from_utf8(bytes).expect("string constants isn't utf8");
                    assert_eq!(HeaderName::from_str(utf8).unwrap(), *header);
                }
            }
        }
    }
}

// Generate constants for all standard NATS headers.
standard_headers! {
    /// The name of the stream the message belongs to.
    (NatsStream, NATS_STREAM, b"Nats-Stream");
    /// The sequence number of the message within the stream.
    (NatsSequence, NATS_SEQUENCE, b"Nats-Sequence");
    /// The timestamp of when the message was sent.
    (NatsTimeStamp, NATS_TIME_STAMP, b"Nats-Time-Stamp");
    /// The subject of the message, used for routing and filtering messages.
    (NatsSubject, NATS_SUBJECT, b"Nats-Subject");
    /// A unique identifier for the message.
    (NatsMessageId, NATS_MESSAGE_ID, b"Nats-Msg-Id");
    /// The last known stream the message was part of.
    (NatsLastStream, NATS_LAST_STREAM, b"Nats-Last-Stream");
    /// The last known consumer that processed the message.
    (NatsLastConsumer, NATS_LAST_CONSUMER, b"Nats-Last-Consumer");
    /// The last known sequence number of the message.
    (NatsLastSequence, NATS_LAST_SEQUENCE, b"Nats-Last-Sequence");
    /// The expected last sequence number of the subject.
    (NatsExpectgedLastSubjectSequence, NATS_EXPECTED_LAST_SUBJECT_SEQUENCE, b"Nats-Expected-Last-Subject-Sequence");
    /// The expected last message ID within the stream.
    (NatsExpectedLastMessageId, NATS_EXPECTED_LAST_MESSAGE_ID, b"Nats-Expected-Last-Msg-Id");
    /// The expected last sequence number within the stream.
    (NatsExpectedLastSequence, NATS_EXPECTED_LAST_SEQUENCE, b"Nats-Expected-Last-Sequence");
    /// The expected stream the message should be part of.
    (NatsExpectedStream, NATS_EXPECTED_STREAM, b"Nats-Expected-Stream");
}

#[derive(Debug, Hash, PartialEq, Eq, Clone, Serialize, Deserialize)]
struct CustomHeader {
    bytes: Bytes,
}

impl CustomHeader {
    #[inline]
    pub(crate) const fn from_static(value: &'static str) -> CustomHeader {
        CustomHeader {
            bytes: Bytes::from_static(value.as_bytes()),
        }
    }

    #[inline]
    pub(crate) fn as_str(&self) -> &str {
        unsafe { std::str::from_utf8_unchecked(self.bytes.as_ref()) }
    }
}

impl From<String> for CustomHeader {
    #[inline]
    fn from(value: String) -> CustomHeader {
        CustomHeader {
            bytes: Bytes::from(value),
        }
    }
}

impl<'a> From<&'a str> for CustomHeader {
    #[inline]
    fn from(value: &'a str) -> CustomHeader {
        CustomHeader {
            bytes: Bytes::copy_from_slice(value.as_bytes()),
        }
    }
}

#[derive(Debug, Hash, PartialEq, Eq, Clone, Serialize, Deserialize)]
enum HeaderRepr {
    Standard(StandardHeader),
    Custom(CustomHeader),
}

/// Defines a NATS header field name
///
/// Header field names identify the header. Header sets may include multiple
/// headers with the same name.
///
/// # Representation
///
/// `HeaderName` represents standard header names using an `enum`, as such they
/// will not require an allocation for storage.
#[derive(Clone, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
pub struct HeaderName {
    inner: HeaderRepr,
}

impl HeaderName {
    /// Converts a static string to a NATS header name.
    #[inline]
    pub const fn from_static(value: &'static str) -> HeaderName {
        if let Some(standard) = StandardHeader::from_bytes(value.as_bytes()) {
            return HeaderName {
                inner: HeaderRepr::Standard(standard),
            };
        }

        HeaderName {
            inner: HeaderRepr::Custom(CustomHeader::from_static(value)),
        }
    }

    /// Returns a `str` representation of the header.
    #[inline]
    fn as_str(&self) -> &str {
        match self.inner {
            HeaderRepr::Standard(v) => v.as_str(),
            HeaderRepr::Custom(ref v) => v.as_str(),
        }
    }
}

impl FromStr for HeaderName {
    type Err = ParseHeaderNameError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.contains(|c: char| !c.is_ascii_alphanumeric() && c != '-') {
            return Err(ParseHeaderNameError);
        }

        match StandardHeader::from_bytes(s.as_ref()) {
            Some(v) => Ok(HeaderName {
                inner: HeaderRepr::Standard(v),
            }),
            None => Ok(HeaderName {
                inner: HeaderRepr::Custom(CustomHeader::from(s)),
            }),
        }
    }
}

impl fmt::Display for HeaderName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.as_str(), f)
    }
}

impl AsRef<[u8]> for HeaderName {
    fn as_ref(&self) -> &[u8] {
        self.as_str().as_bytes()
    }
}

impl AsRef<str> for HeaderName {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

#[derive(Debug, Clone)]
pub struct ParseHeaderNameError;

impl std::fmt::Display for ParseHeaderNameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid header name (name cannot contain non-ascii alphanumeric characters other than '-')")
    }
}

impl std::error::Error for ParseHeaderNameError {}

#[cfg(test)]
mod tests {
    use super::{HeaderMap, HeaderName, HeaderValue};
    use std::str::{from_utf8, FromStr};

    #[test]
    fn try_from() {
        let mut headers = HeaderMap::new();
        headers.insert("name", "something".parse::<HeaderValue>().unwrap());
        headers.insert("name", "something2");
    }

    #[test]
    fn append() {
        let mut headers = HeaderMap::new();
        headers.append("Key", "value");
        headers.append("Key", "second_value");

        assert_eq!(
            headers.get("Key").unwrap().value,
            Vec::from_iter(["value".to_string(), "second_value".to_string()])
        );
    }

    #[test]
    fn get_string() {
        let mut headers = HeaderMap::new();
        headers.append("Key", "value");
        headers.append("Key", "other");

        assert_eq!(headers.get("Key").unwrap().to_string(), "value");

        let key: String = headers.get("Key").unwrap().into();
        assert_eq!(key, "value".to_string());

        let key: String = headers.get("Key").unwrap().to_owned().into();
        assert_eq!(key, "value".to_string());

        assert_eq!(headers.get("Key").unwrap().as_str(), "value");
    }

    #[test]
    fn insert() {
        let mut headers = HeaderMap::new();
        headers.insert("Key", "Value");

        assert_eq!(
            headers.get("Key").unwrap().value,
            Vec::from_iter(["Value".to_string()])
        );
    }

    #[test]
    fn serialize() {
        let mut headers = HeaderMap::new();
        headers.append("Key", "value");
        headers.append("Key", "second_value");
        headers.insert("Second", "SecondValue");

        let bytes = headers.to_bytes();

        println!("bytes: {:?}", from_utf8(&bytes));
    }

    #[test]
    fn is_empty() {
        let mut headers = HeaderMap::new();
        assert!(headers.is_empty());

        headers.append("Key", "value");
        headers.append("Key", "second_value");
        headers.insert("Second", "SecondValue");
        assert!(!headers.is_empty());
    }

    #[test]
    fn parse_value() {
        assert!("Foo\r".parse::<HeaderValue>().is_err());
        assert!("Foo\n".parse::<HeaderValue>().is_err());
        assert!("Foo\r\n".parse::<HeaderValue>().is_err());
    }

    #[test]
    fn valid_header_name() {
        let valid_header_name = "X-Custom-Header";
        let parsed_header = HeaderName::from_str(valid_header_name);

        assert!(
            parsed_header.is_ok(),
            "Expected Ok(HeaderName), but got an error: {:?}",
            parsed_header.err()
        );
    }

    #[test]
    fn invalid_header_name_with_space() {
        let invalid_header_name = "X Custom Header";
        let parsed_header = HeaderName::from_str(invalid_header_name);

        assert!(
            parsed_header.is_err(),
            "Expected Err(InvalidHeaderNameError), but got Ok: {:?}",
            parsed_header.ok()
        );
    }

    #[test]
    fn invalid_header_name_with_special_chars() {
        let invalid_header_name = "X-Header!@#";
        let parsed_header = HeaderName::from_str(invalid_header_name);

        assert!(
            parsed_header.is_err(),
            "Expected Err(InvalidHeaderNameError), but got Ok: {:?}",
            parsed_header.ok()
        );
    }

    #[test]
    fn from_static_eq() {
        let a = HeaderName::from_static("NATS-Stream");
        let b = HeaderName::from_static("NATS-Stream");

        assert_eq!(a, b);
    }
}
