// Copyright 2020-2021 The NATS Authors
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

use std::{
    collections::{HashMap, HashSet},
    convert::TryFrom,
    iter::{FromIterator, IntoIterator},
    ops::Deref,
};

use log::trace;

const HEADER_LINE: &str = "NATS/1.0";
const HEADER_LINE_LEN: usize = HEADER_LINE.len();

/// Status
pub const STATUS: &str = "Status";
/// Description
pub const DESCRIPTION: &str = "Description";

/// Nats-Msg-Id
pub const NATS_MSG_ID: &str = "Nats-Msg-Id";

/// Nats-Expected-Stream
pub const NATS_EXPECTED_STREAM: &str = "Nats-Expected-Stream";

/// Nats-Expected-Last-Msg-Id
pub const NATS_EXPECTED_LAST_MSG_ID: &str = "Nats-Expected-Last-Msg-Id";

/// Nats-Expected-Last-Sequence
pub const NATS_EXPECTED_LAST_SEQUENCE: &str = "Nats-Expected-Last-Sequence";

/// Nats-Expected-Last-Subject-Sequence
pub const NATS_EXPECTED_LAST_SUBJECT_SEQUENCE: &str = "Nats-Expected-Last-Subject-Sequence";

/// A multi-map from header name to a set of values for that header
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct HeaderMap {
    /// A multi-map from header name to a set of values for that header
    pub inner: HashMap<String, HashSet<String>>,
}

impl FromIterator<(String, String)> for HeaderMap {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = (String, String)>,
    {
        let mut inner = HashMap::default();
        for (k, v) in iter {
            let entry = inner.entry(k).or_insert_with(HashSet::default);
            entry.insert(v);
        }
        HeaderMap { inner }
    }
}

impl<'a> FromIterator<(&'a String, &'a String)> for HeaderMap {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = (&'a String, &'a String)>,
    {
        let mut inner = HashMap::default();
        for (k, v) in iter {
            let k = k.to_string();
            let v = v.to_string();
            let entry = inner.entry(k).or_insert_with(HashSet::default);
            entry.insert(v);
        }
        HeaderMap { inner }
    }
}

impl<'a> FromIterator<&'a (&'a String, &'a String)> for HeaderMap {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = &'a (&'a String, &'a String)>,
    {
        let mut inner = HashMap::default();
        for (k, v) in iter {
            let k = k.to_string();
            let v = v.to_string();
            let entry = inner.entry(k).or_insert_with(HashSet::default);
            entry.insert(v);
        }
        HeaderMap { inner }
    }
}

impl<'a> FromIterator<(&'a str, &'a str)> for HeaderMap {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = (&'a str, &'a str)>,
    {
        let mut inner = HashMap::default();
        for (k, v) in iter {
            let k = k.to_string();
            let v = v.to_string();
            let entry = inner.entry(k).or_insert_with(HashSet::default);
            entry.insert(v);
        }
        HeaderMap { inner }
    }
}

impl<'a> FromIterator<&'a (&'a str, &'a str)> for HeaderMap {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = &'a (&'a str, &'a str)>,
    {
        let mut inner = HashMap::default();
        for (k, v) in iter {
            let k = k.to_string();
            let v = v.to_string();
            let entry = inner.entry(k).or_insert_with(HashSet::default);
            entry.insert(v);
        }
        HeaderMap { inner }
    }
}

fn parse_error<T, E: AsRef<str>>(e: E) -> std::io::Result<T> {
    trace!("header parse error: {}", e.as_ref());
    Err(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        e.as_ref(),
    ))
}

fn is_continuation(c: char) -> bool {
    c == ' ' || c == '\t'
}

impl TryFrom<&[u8]> for HeaderMap {
    type Error = std::io::Error;

    fn try_from(buf: &[u8]) -> std::io::Result<Self> {
        let mut inner = HashMap::default();
        let mut lines = if let Ok(line) = std::str::from_utf8(buf) {
            line.lines().peekable()
        } else {
            return parse_error("invalid header received");
        };

        if let Some(line) = lines.next() {
            if !line.starts_with(HEADER_LINE) {
                return parse_error("version line does not begin with NATS/1.0");
            }

            if let Some(slice) = line.get(HEADER_LINE_LEN..).map(|s| s.trim()) {
                match slice.split_once(' ') {
                    Some((status, description)) => {
                        if !status.is_empty() {
                            let entry = inner
                                .entry(STATUS.to_string())
                                .or_insert_with(HashSet::default);
                            entry.insert(status.trim().to_string());
                        }

                        if !description.is_empty() {
                            let entry = inner
                                .entry(DESCRIPTION.to_string())
                                .or_insert_with(HashSet::default);
                            entry.insert(description.trim().to_string());
                        }
                    }
                    None => {
                        if !slice.is_empty() {
                            let entry = inner
                                .entry(STATUS.to_string())
                                .or_insert_with(HashSet::default);
                            entry.insert(slice.to_string());
                        }
                    }
                }
            }
        } else {
            return parse_error("expected header information not present");
        };

        while let Some(line) = lines.next() {
            if line.is_empty() {
                continue;
            }

            if let Some((k, v)) = line.split_once(':') {
                let entry = inner
                    .entry(k.trim().to_string())
                    .or_insert_with(HashSet::default);

                let mut s = String::from(v.trim());
                while let Some(v) = lines.next_if(|s| s.starts_with(is_continuation)) {
                    s.push(' ');
                    s.push_str(v.trim());
                }

                entry.insert(s);
            } else {
                return parse_error("malformed header line");
            }
        }

        Ok(HeaderMap { inner })
    }
}

impl Deref for HeaderMap {
    type Target = HashMap<String, HashSet<String>>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl HeaderMap {
    pub(crate) fn to_bytes(&self) -> Vec<u8> {
        // `<version line>\r\n[headers]\r\n\r\n[payload]\r\n`
        let mut buf = vec![];
        buf.extend_from_slice(b"NATS/1.0\r\n");
        for (k, vs) in &self.inner {
            for v in vs {
                buf.extend_from_slice(k.trim().as_bytes());
                buf.push(b':');
                buf.extend_from_slice(v.trim().as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
        }
        buf.extend_from_slice(b"\r\n");
        buf
    }
}

#[cfg(test)]
mod try_from {
    use super::*;

    #[test]
    fn inline_status() {
        // With single spacing.
        let headers = HeaderMap::try_from("NATS/1.0 100".as_bytes()).unwrap();

        assert_eq!(
            headers.inner.get(&STATUS.to_string()),
            Some(&HashSet::from_iter(vec!["100".to_string(),]))
        );

        // With double spacing.
        let headers = HeaderMap::try_from("NATS/1.0  100".as_bytes()).unwrap();

        assert_eq!(
            headers.inner.get(&STATUS.to_string()),
            Some(&HashSet::from_iter(vec!["100".to_string(),]))
        );
    }

    #[test]
    fn inline_status_with_description() {
        // With single spacing
        let headers = HeaderMap::try_from("NATS/1.0 100 Idle Heartbeat".as_bytes()).unwrap();

        assert_eq!(
            headers.inner.get(&STATUS.to_string()),
            Some(&HashSet::from_iter(vec!["100".to_string()]))
        );

        assert_eq!(
            headers.inner.get(&DESCRIPTION.to_string()),
            Some(&HashSet::from_iter(vec!["Idle Heartbeat".to_string()]))
        );

        // With double spacing.
        let headers = HeaderMap::try_from("NATS/1.0  100  Idle Heartbeat".as_bytes()).unwrap();

        assert_eq!(
            headers.inner.get(&STATUS.to_string()),
            Some(&HashSet::from_iter(vec!["100".to_string()]))
        );

        assert_eq!(
            headers.inner.get(&DESCRIPTION.to_string()),
            Some(&HashSet::from_iter(vec!["Idle Heartbeat".to_string()]))
        );
    }

    #[test]
    fn malformed_line() {
        let error = HeaderMap::try_from("NATS/1.0 200\r\n\nX-Test-A a\r\n".as_bytes()).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[test]
    fn empty_lines() {
        let headers = HeaderMap::try_from(
            "NATS/1.0 200\r\n\nX-Test-A: a\r\n\nX-Test-B: b\r\n\nX-Test-C: c\r\n\n".as_bytes(),
        )
        .unwrap();

        assert_eq!(
            headers.inner.get(&"X-Test-A".to_string()),
            Some(&HashSet::from_iter(vec!["a".to_string()]))
        );

        assert_eq!(
            headers.inner.get(&"X-Test-B".to_string()),
            Some(&HashSet::from_iter(vec!["b".to_string()]))
        );

        assert_eq!(
            headers.inner.get(&"X-Test-C".to_string()),
            Some(&HashSet::from_iter(vec!["c".to_string()]))
        );
    }

    #[test]
    fn single_line() {
        let headers = HeaderMap::try_from(
            "NATS/1.0 200\r\nAccept-Encoding: json\r\nAuthorization: s3cr3t\r\n".as_bytes(),
        )
        .unwrap();

        assert_eq!(
            headers.inner.get(&"Accept-Encoding".to_string()),
            Some(&HashSet::from_iter(vec!["json".to_string()]))
        );

        assert_eq!(
            headers.inner.get(&"Authorization".to_string()),
            Some(&HashSet::from_iter(vec!["s3cr3t".to_string()]))
        );
    }

    #[test]
    fn multi_line_with_tabs() {
        let headers =
            HeaderMap::try_from("NATS/1.0 200\r\nX-Test: one,\r\n\ttwo,\r\n\tthree\r\n".as_bytes())
                .unwrap();

        assert_eq!(
            headers.inner.get(&"X-Test".to_string()),
            Some(&HashSet::from_iter(vec!["one, two, three".to_string()]))
        );
    }

    #[test]
    fn multi_line_with_spaces() {
        let headers =
            HeaderMap::try_from("NATS/1.0 200\r\nX-Test: one,\r\n two,\r\n three\r\n".as_bytes())
                .unwrap();

        assert_eq!(
            headers.inner.get(&"X-Test".to_string()),
            Some(&HashSet::from_iter(vec!["one, two, three".to_string()]))
        );
    }
}
