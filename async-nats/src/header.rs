// Copyright 2020-2022 The NATS Authors
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

//! NATS [Message][crate::Message] headers, leveraging [http::header] crate.
// pub use http::header::{HeaderMap, HeaderName, HeaderValue};

use std::{
    collections::{self, HashMap, HashSet},
    str::FromStr,
};

use serde::Serialize;

pub const NATS_LAST_STREAM: &str = "nats-last-stream";

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Default)]
pub struct HeaderMap {
    headers: HashMap<HeaderName, HeaderValue>,
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
        self.headers.iter()
    }
}

impl HeaderMap {
    pub fn new() -> Self {
        HeaderMap::default()
    }
}

impl HeaderMap {
    pub fn insert<K: IntoHeaderName, V: IntoHeaderValue>(&mut self, name: K, value: V) {
        self.headers
            .insert(name.into_header_name(), value.into_header_value());
    }

    pub fn append<K: IntoHeaderName, V: ToString>(&mut self, name: K, value: V) {
        let key = name.into_header_name();
        let v = self.headers.get_mut(&key);
        match v {
            Some(v) => {
                v.value.insert(value.to_string());
            }
            None => {
                self.insert(key, value.to_string().into_header_value());
            }
        }
    }

    pub fn get<T: IntoHeaderName>(&self, name: T) -> Option<&HeaderValue> {
        self.headers.get(&name.into_header_name())
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = vec![];
        buf.extend_from_slice(b"NATS/1.0\r\n");
        for (k, vs) in &self.headers {
            for v in vs.iter() {
                buf.extend_from_slice(k.value.as_bytes());
                buf.extend_from_slice(b": ");
                buf.extend_from_slice(v.as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
        }
        buf.extend_from_slice(b"\r\n");
        buf
    }
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Default)]
pub struct HeaderValue {
    value: HashSet<String>,
}

impl FromStr for HeaderValue {
    type Err = crate::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut set = HeaderValue::new();
        set.value.insert(s.to_string());
        Ok(set)
    }
}

impl IntoIterator for HeaderValue {
    type Item = String;

    type IntoIter = collections::hash_set::IntoIter<String>;

    fn into_iter(self) -> Self::IntoIter {
        self.value.into_iter()
    }
}

impl HeaderValue {
    pub fn new() -> HeaderValue {
        HeaderValue::default()
    }

    pub fn iter(&self) -> collections::hash_set::Iter<String> {
        self.value.iter()
    }
}

pub trait IntoHeaderName {
    fn into_header_name(self) -> HeaderName;
}
impl IntoHeaderName for &str {
    fn into_header_name(self) -> HeaderName {
        HeaderName {
            value: self.to_string(),
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
        set.value.insert(self.to_string());
        set
    }
}

impl IntoHeaderValue for HeaderValue {
    fn into_header_value(self) -> HeaderValue {
        self
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug, Serialize)]
pub struct HeaderName {
    value: String,
}
impl FromStr for HeaderName {
    type Err = crate::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(HeaderName {
            value: s.to_string(),
        })
    }
}

impl AsRef<[u8]> for HeaderName {
    fn as_ref(&self) -> &[u8] {
        self.value.as_bytes()
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, str::from_utf8};

    use crate::HeaderMap;

    #[test]
    fn append() {
        let mut headers = HeaderMap::new();
        headers.append("Key", "value");
        headers.append("Key", "second_value");

        assert_eq!(
            headers.get("Key").unwrap().value,
            HashSet::from_iter(["value".to_string(), "second_value".to_string()])
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
}
