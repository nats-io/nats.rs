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

/// A multi-map from header name to a set of values for that header
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Headers {
    /// A multi-map from header name to a set of values for that header
    pub inner: HashMap<String, HashSet<String>>,
}

impl FromIterator<(String, String)> for Headers {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = (String, String)>,
    {
        let mut inner = HashMap::default();
        for (k, v) in iter {
            let entry = inner.entry(k).or_insert_with(HashSet::default);
            entry.insert(v);
        }
        Headers { inner }
    }
}

impl<'a> FromIterator<(&'a String, &'a String)> for Headers {
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
        Headers { inner }
    }
}

impl<'a> FromIterator<&'a (&'a String, &'a String)> for Headers {
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
        Headers { inner }
    }
}

impl<'a> FromIterator<(&'a str, &'a str)> for Headers {
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
        Headers { inner }
    }
}

impl<'a> FromIterator<&'a (&'a str, &'a str)> for Headers {
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
        Headers { inner }
    }
}

fn parse_error<T, E: AsRef<str>>(e: E) -> std::io::Result<T> {
    trace!("header parse error: {}", e.as_ref());
    Err(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        e.as_ref(),
    ))
}

// Header status processing.
const HDR_PRE: &str = "NATS/1.0";
const HDR_PRE_END: usize = HDR_PRE.len();

// Public
pub const STATUS_HDR: &str = "Status";
pub const DESC_HDR: &str = "Description";

impl TryFrom<&[u8]> for Headers {
    type Error = std::io::Error;

    fn try_from(buf: &[u8]) -> std::io::Result<Self> {
        let mut inner = HashMap::default();
        let mut lines = if let Ok(line) = std::str::from_utf8(buf) {
            line.lines()
        } else {
            return parse_error("invalid header received");
        };

        if let Some(line) = lines.next() {
            if !line.starts_with(HDR_PRE) {
                return parse_error("version line does not begin with NATS/");
            }
            // Check for an inline status and optional description.
            if line.len() > HDR_PRE_END {
                let status_line = &line[HDR_PRE_END..];
                let mut parts: Vec<&str> = status_line.split_whitespace().collect();
                let code = parts.pop().unwrap();
                let entry = inner
                    .entry(STATUS_HDR.to_string())
                    .or_insert_with(HashSet::default);
                entry.insert(code.to_string());
                // Optional description.
                if let Some(description) = parts.pop() {
                    let entry = inner
                        .entry(DESC_HDR.to_string())
                        .or_insert_with(HashSet::default);
                    entry.insert(description.to_string());
                }
            }
        } else {
            return parse_error("expected header information not present");
        };

        for line in lines {
            let splits = line.splitn(2, ':').map(str::trim).collect::<Vec<_>>();
            match splits[..] {
                [k, v] => {
                    let entry = inner.entry(k.to_string()).or_insert_with(HashSet::default);
                    entry.insert(v.to_string());
                }
                [""] => continue,
                _ => {
                    return parse_error("malformed header input");
                }
            }
        }

        Ok(Headers { inner })
    }
}

impl Deref for Headers {
    type Target = HashMap<String, HashSet<String>>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl Headers {
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
