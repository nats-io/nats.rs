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

//! NATS status codes.

// Heavily borrowed from the http crate, would re-export since it is already a dependency
// but we have our own range of constants.
use std::convert::TryFrom;
use std::error::Error;
use std::fmt;
use std::num::NonZeroU16;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// A possible error value when converting a `StatusCode` from a `u16` or `&str`
///
/// This error indicates that the supplied input was not a valid number, was less
/// than 100, or was greater than 999.
pub struct InvalidStatusCode {}

impl fmt::Debug for InvalidStatusCode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("InvalidStatusCode").finish()
    }
}

impl fmt::Display for InvalidStatusCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("invalid status code")
    }
}

impl Error for InvalidStatusCode {}

impl InvalidStatusCode {
    fn new() -> InvalidStatusCode {
        InvalidStatusCode {}
    }
}

/// An NATS status code.
///
/// Constants are provided for known status codes.
///
/// Status code values in the range 100-999 (inclusive) are supported by this
/// type. Values in the range 100-599 are semantically classified by the most
/// significant digit. See [`StatusCode::is_success`], etc.
///
/// # Examples
///
/// ```
/// use async_nats::StatusCode;
///
/// assert_eq!(StatusCode::OK.as_u16(), 200);
/// assert!(StatusCode::OK.is_success());
/// ```
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct StatusCode(NonZeroU16);

impl StatusCode {
    /// Converts a u16 to a status code.
    ///
    /// The function validates the correctness of the supplied u16. It must be
    /// greater or equal to 100 and less than 1000.
    ///
    /// # Example
    ///
    /// ```
    /// use async_nats::status::StatusCode;
    ///
    /// let ok = StatusCode::from_u16(200).unwrap();
    /// assert_eq!(ok, StatusCode::OK);
    ///
    /// let err = StatusCode::from_u16(99);
    /// assert!(err.is_err());
    ///
    /// let err = StatusCode::from_u16(1000);
    /// assert!(err.is_err());
    /// ```
    #[inline]
    pub fn from_u16(src: u16) -> Result<StatusCode, InvalidStatusCode> {
        if !(100..1000).contains(&src) {
            return Err(InvalidStatusCode::new());
        }

        NonZeroU16::new(src)
            .map(StatusCode)
            .ok_or_else(InvalidStatusCode::new)
    }

    /// Converts a `&[u8]` to a status code
    pub fn from_bytes(src: &[u8]) -> Result<StatusCode, InvalidStatusCode> {
        if src.len() != 3 {
            return Err(InvalidStatusCode::new());
        }

        let a = src[0].wrapping_sub(b'0') as u16;
        let b = src[1].wrapping_sub(b'0') as u16;
        let c = src[2].wrapping_sub(b'0') as u16;

        if a == 0 || a > 9 || b > 9 || c > 9 {
            return Err(InvalidStatusCode::new());
        }

        let status = (a * 100) + (b * 10) + c;
        NonZeroU16::new(status)
            .map(StatusCode)
            .ok_or_else(InvalidStatusCode::new)
    }

    /// Returns the `u16` corresponding to this `StatusCode`.
    ///
    /// # Example
    ///
    /// ```
    /// let status = async_nats::StatusCode::OK;
    /// assert_eq!(status.as_u16(), 200);
    /// ```
    #[inline]
    pub fn as_u16(&self) -> u16 {
        (*self).into()
    }

    /// Check if status is within 100-199.
    #[inline]
    pub fn is_informational(&self) -> bool {
        (100..200).contains(&self.0.get())
    }

    /// Check if status is within 200-299.
    #[inline]
    pub fn is_success(&self) -> bool {
        (200..300).contains(&self.0.get())
    }

    /// Check if status is within 300-399.
    #[inline]
    pub fn is_redirection(&self) -> bool {
        (300..400).contains(&self.0.get())
    }

    /// Check if status is within 400-499.
    #[inline]
    pub fn is_client_error(&self) -> bool {
        (400..500).contains(&self.0.get())
    }

    /// Check if status is within 500-599.
    #[inline]
    pub fn is_server_error(&self) -> bool {
        (500..600).contains(&self.0.get())
    }
}

impl fmt::Debug for StatusCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.0, f)
    }
}

/// Formats the status code.
///
/// # Example
///
/// ```
/// # use async_nats::StatusCode;
/// assert_eq!(format!("{}", StatusCode::OK), "200");
/// ```
impl fmt::Display for StatusCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // TODO(caspervonb) display a canonical statically known reason / human readable description of the status
        write!(f, "{}", u16::from(*self))
    }
}

impl Default for StatusCode {
    #[inline]
    fn default() -> StatusCode {
        StatusCode::OK
    }
}

impl PartialEq<u16> for StatusCode {
    #[inline]
    fn eq(&self, other: &u16) -> bool {
        self.as_u16() == *other
    }
}

impl PartialEq<StatusCode> for u16 {
    #[inline]
    fn eq(&self, other: &StatusCode) -> bool {
        *self == other.as_u16()
    }
}

impl From<StatusCode> for u16 {
    #[inline]
    fn from(status: StatusCode) -> u16 {
        status.0.get()
    }
}

impl FromStr for StatusCode {
    type Err = InvalidStatusCode;

    fn from_str(s: &str) -> Result<StatusCode, InvalidStatusCode> {
        StatusCode::from_bytes(s.as_ref())
    }
}

impl<'a> From<&'a StatusCode> for StatusCode {
    #[inline]
    fn from(t: &'a StatusCode) -> Self {
        *t
    }
}

impl<'a> TryFrom<&'a [u8]> for StatusCode {
    type Error = InvalidStatusCode;

    #[inline]
    fn try_from(t: &'a [u8]) -> Result<Self, Self::Error> {
        StatusCode::from_bytes(t)
    }
}

impl<'a> TryFrom<&'a str> for StatusCode {
    type Error = InvalidStatusCode;

    #[inline]
    fn try_from(t: &'a str) -> Result<Self, Self::Error> {
        t.parse()
    }
}

impl TryFrom<u16> for StatusCode {
    type Error = InvalidStatusCode;

    #[inline]
    fn try_from(t: u16) -> Result<Self, Self::Error> {
        StatusCode::from_u16(t)
    }
}

impl StatusCode {
    pub const IDLE_HEARTBEAT: StatusCode = StatusCode(new_nonzero_u16(100));
    pub const OK: StatusCode = StatusCode(new_nonzero_u16(200));
    pub const NOT_FOUND: StatusCode = StatusCode(new_nonzero_u16(404));
    pub const TIMEOUT: StatusCode = StatusCode(new_nonzero_u16(408));
    pub const NO_RESPONDERS: StatusCode = StatusCode(new_nonzero_u16(503));
    pub const REQUEST_TERMINATED: StatusCode = StatusCode(new_nonzero_u16(409));
}

/// This function is needed until const Option::unwrap becomes stable.
/// Ref: https://github.com/nats-io/nats.rs/issues/804
const fn new_nonzero_u16(n: u16) -> NonZeroU16 {
    match NonZeroU16::new(n) {
        Some(d) => d,
        None => {
            panic!("Invalid non-zero u16");
        }
    }
}
