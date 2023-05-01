use bytes::Bytes;
use std::ops::Deref;
use std::str::{from_utf8, Utf8Error};

/// A `Subject` is an immutable string type that guarantees valid UTF-8 contents.
///
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
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
    /// assert_eq!(subject, "Static string");
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
    /// assert_eq!(subject, "Hello");
    /// ```
    pub fn from_utf8<T>(input: T) -> Result<Self, Utf8Error>
    where
        T: Into<Bytes>
    {
        let bytes = input.into();
        if let Err(err) = from_utf8(bytes.as_ref()) {
            return Err(err);
        }

        Ok(Subject {
            bytes,
        })
    }
}

impl<'a> From<&'a str> for Subject {
    fn from(s: &'a str) -> Self {
        Subject::from_static(s)
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
        &*self
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
