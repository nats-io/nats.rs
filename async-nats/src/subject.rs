use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::Deref;
use std::str::{from_utf8, Utf8Error};

/// Error type for subject validation failures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubjectError {
    /// The subject format is invalid (contains whitespace, control characters,
    /// starts/ends with `.`, or is empty).
    InvalidFormat,
}

impl fmt::Display for SubjectError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SubjectError::InvalidFormat => write!(f, "invalid subject format"),
        }
    }
}

impl std::error::Error for SubjectError {}

/// A `Subject` is an immutable string type that guarantees valid UTF-8 contents.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Subject {
    bytes: Bytes,
}

impl Subject {
    /// Creates a new `Subject` from Bytes.
    ///
    /// # Safety
    /// Function is unsafe because it does not check if the bytes are valid UTF-8.
    ///
    /// # Examples
    ///
    /// ```
    /// use async_nats::Subject;
    /// use bytes::Bytes;
    ///
    /// let bytes = Bytes::from_static(b"Static string");
    ///
    /// let subject = unsafe { Subject::from_bytes_unchecked(bytes) };
    /// assert_eq!(subject.as_str(), "Static string");
    /// ```
    pub const unsafe fn from_bytes_unchecked(bytes: Bytes) -> Self {
        Subject { bytes }
    }

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
    pub const fn from_static(input: &'static str) -> Self {
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

    /// Turns the `Subject` into a `String`, consuming it.
    ///
    /// Note that this function is not implemented as `From<Subject> for String` as the conversion
    /// from the underlying type could involve an allocation. If the `Subject` is owned data, it
    /// will not allocate, but if it was constructed from borrowed data, it will.
    ///
    /// # Examples
    ///
    /// ```
    /// use async_nats::Subject;
    ///
    /// let s = Subject::from("foo");
    /// let sub = s.into_string();
    /// ```
    pub fn into_string(self) -> String {
        // SAFETY: We have guaranteed that the bytes in the `Subject` struct are valid UTF-8.
        unsafe { String::from_utf8_unchecked(self.bytes.into()) }
    }

    /// Returns `true` if this subject follows NATS subject rules.
    ///
    /// A valid subject must:
    /// - Not be empty
    /// - Not start or end with `.`
    /// - Not contain consecutive dots (`..`)
    /// - Not contain whitespace or control characters
    #[inline]
    pub fn is_valid(&self) -> bool {
        crate::is_valid_subject(self)
    }

    /// Creates a new `Subject` from a string, validating it follows NATS subject rules.
    ///
    /// Returns an error if the subject is invalid.
    ///
    /// # Examples
    ///
    /// ```
    /// use async_nats::Subject;
    ///
    /// let subject = Subject::validated("events.data").unwrap();
    /// assert_eq!(subject.as_str(), "events.data");
    ///
    /// assert!(Subject::validated("invalid subject").is_err());
    /// assert!(Subject::validated("").is_err());
    /// assert!(Subject::validated(".invalid").is_err());
    /// ```
    pub fn validated(s: impl AsRef<str>) -> Result<Subject, SubjectError> {
        let s = s.as_ref();
        if !crate::is_valid_subject(s) {
            return Err(SubjectError::InvalidFormat);
        }
        Ok(Subject::from(s))
    }

    /// Creates a new validated `Subject` from a static string with compile-time validation.
    ///
    /// This function validates the subject at compile time using const panics.
    /// Invalid subjects will cause a compilation error.
    ///
    /// # Examples
    ///
    /// ```
    /// use async_nats::Subject;
    ///
    /// const SUBJECT: Subject = Subject::from_static_validated("events.data");
    /// ```
    ///
    /// The following will fail to compile:
    /// ```compile_fail
    /// use async_nats::Subject;
    /// const INVALID: Subject = Subject::from_static_validated("invalid subject");
    /// ```
    pub const fn from_static_validated(subject: &'static str) -> Self {
        let bytes = subject.as_bytes();
        let len = bytes.len();

        if len == 0 {
            panic!("subject cannot be empty");
        }

        if bytes[0] == b'.' {
            panic!("subject cannot start with '.'");
        }

        if bytes[len - 1] == b'.' {
            panic!("subject cannot end with '.'");
        }

        let mut i = 0;
        while i < len {
            let c = bytes[i];
            if c == b' ' || c == b'\t' || c == b'\r' || c == b'\n' {
                panic!("subject cannot contain whitespace or control characters");
            }
            if c == b'.' && i + 1 < len && bytes[i + 1] == b'.' {
                panic!("subject cannot contain consecutive dots");
            }
            i += 1;
        }

        Subject::from_static(subject)
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

impl TryFrom<Bytes> for Subject {
    type Error = Utf8Error;

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        from_utf8(bytes.as_ref())?;
        Ok(Subject { bytes })
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

impl Serialize for Subject {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for Subject {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Ok(String::deserialize(deserializer)?.into())
    }
}
