use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::Deref;
use std::str::{from_utf8, Utf8Error};

/// A `Subject` is an immutable string type that guarantees valid UTF-8 contents.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Subject {
    bytes: Bytes,
}

impl Subject {
    /// Creates a new `Subject` from a static string.
    ///
    /// # Examples
    ///
    /// ```
    /// use async_nats::Subject;
    ///
    /// let subject = Subject::from_static("Static string");
    /// assert_eq!(subject.as_str(), "Static string");
    /// ```
    pub fn from_static(input: &'static str) -> Self {
        Subject {
            bytes: Bytes::from_static(input.as_bytes()),
        }
    }

    /// Creates a new `Subject` from a UTF-8 encoded byte vector.
    ///
    /// Returns an error if the input is not valid UTF-8.
    ///
    /// # Examples
    ///
    /// ```
    /// use async_nats::Subject;
    ///
    /// let utf8_input = vec![72, 101, 108, 108, 111]; // "Hello" in UTF-8
    /// let subject = Subject::from_utf8(utf8_input).unwrap();
    /// assert_eq!(subject.as_ref(), "Hello");
    /// ```
    pub fn from_utf8<T>(input: T) -> Result<Self, Utf8Error>
    where
        T: Into<Bytes>,
    {
        let bytes = input.into();
        from_utf8(bytes.as_ref())?;

        Ok(Subject { bytes })
    }

    /// Extracts a string slice containing the entire `Subject`.
    ///
    /// # Examples
    ///
    /// Basic usage:
    ///
    /// ```
    /// use async_nats::Subject;
    ///
    /// let s = Subject::from("foo");
    /// assert_eq!("foo", s.as_str());
    /// ```
    #[inline]
    pub fn as_str(&self) -> &str {
        self
    }
}

impl<'a> From<&'a str> for Subject {
    fn from(s: &'a str) -> Self {
        // Since &str is guaranteed to be valid UTF-8, we can create the Subject instance by copying the contents of the &str
        Subject {
            bytes: Bytes::copy_from_slice(s.as_bytes()),
        }
    }
}

impl From<String> for Subject {
    fn from(s: String) -> Self {
        // Since the input `String` is guaranteed to be valid UTF-8, we can
        // safely transmute the internal Vec<u8> to a Bytes value.
        let bytes = Bytes::from(s.into_bytes());
        Subject { bytes }
    }
}

impl AsRef<str> for Subject {
    fn as_ref(&self) -> &str {
        self
    }
}

impl Deref for Subject {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        // SAFETY: It is safe to perform an unchecked conversion from bytes to a string slice
        // here because we guarantee that the bytes in the `Subject` struct are valid UTF-8.
        // This is enforced during the construction of `Subject` through the `from_static`,
        // and `from_utf8` methods. In both cases, the input is either checked for UTF-8 validity or
        // known to be valid UTF-8 as a static string.
        unsafe { std::str::from_utf8_unchecked(&self.bytes) }
    }
}

impl fmt::Display for Subject {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

pub trait ToSubject {
    fn to_subject(&self) -> Subject;
}

impl ToSubject for Subject {
    fn to_subject(&self) -> Subject {
        self.to_owned()
    }
}

impl ToSubject for &'static str {
    fn to_subject(&self) -> Subject {
        Subject::from_static(self)
    }
}

impl ToSubject for String {
    fn to_subject(&self) -> Subject {
        Subject::from(self.as_str())
    }
}
