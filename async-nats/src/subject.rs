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

impl ToSubject for ValidatedSubject {
    fn to_subject(&self) -> Subject {
        self.inner.clone()
    }
}

impl ToSubject for &ValidatedSubject {
    fn to_subject(&self) -> Subject {
        self.inner.clone()
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

/// A validated subject that guarantees the subject follows NATS subject rules.
///
/// This type provides:
/// - Compile-time validation via `from_static()` using const panics
/// - Runtime validation via `new()` that returns a Result
/// - Zero-cost conversion to `Subject` since validation is already done
///
/// # Examples
///
/// ```
/// use async_nats::subject::ValidatedSubject;
///
/// // Compile-time validation
/// const SUBJECT: ValidatedSubject = ValidatedSubject::from_static("events.data");
///
/// // Runtime validation
/// let subject = ValidatedSubject::new("foo.bar").unwrap();
/// ```
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ValidatedSubject {
    inner: Subject,
}

impl ValidatedSubject {
    /// Creates a new validated subject from a string slice.
    ///
    /// Returns an error if the subject is invalid (contains whitespace, control characters,
    /// starts/ends with `.`, or is empty).
    ///
    /// # Examples
    ///
    /// ```
    /// use async_nats::subject::ValidatedSubject;
    ///
    /// let subject = ValidatedSubject::new("events.data").unwrap();
    /// assert_eq!(subject.as_str(), "events.data");
    ///
    /// assert!(ValidatedSubject::new("invalid subject").is_err());
    /// assert!(ValidatedSubject::new("").is_err());
    /// assert!(ValidatedSubject::new(".invalid").is_err());
    /// ```
    pub fn new<S: AsRef<str>>(subject: S) -> Result<Self, SubjectError> {
        let s = subject.as_ref();
        if !crate::is_valid_subject(s) {
            return Err(SubjectError::InvalidFormat);
        }
        Ok(ValidatedSubject {
            inner: Subject::from(s),
        })
    }

    /// Creates a new validated subject from a static string with compile-time validation.
    ///
    /// This function validates the subject at compile time using const panics.
    /// Invalid subjects will cause a compilation error.
    ///
    /// # Examples
    ///
    /// ```
    /// use async_nats::subject::ValidatedSubject;
    ///
    /// const SUBJECT: ValidatedSubject = ValidatedSubject::from_static("events.data");
    /// ```
    ///
    /// The following will fail to compile:
    /// ```compile_fail
    /// use async_nats::subject::ValidatedSubject;
    /// const INVALID: ValidatedSubject = ValidatedSubject::from_static("invalid subject");
    /// ```
    pub const fn from_static(subject: &'static str) -> Self {
        // Compile-time validation
        let bytes = subject.as_bytes();
        let len = bytes.len();

        // Check if empty
        if len == 0 {
            panic!("subject cannot be empty");
        }

        // Check if starts with '.'
        if bytes[0] == b'.' {
            panic!("subject cannot start with '.'");
        }

        // Check if ends with '.'
        if bytes[len - 1] == b'.' {
            panic!("subject cannot end with '.'");
        }

        // Check for invalid characters (whitespace and control characters)
        let mut i = 0;
        while i < len {
            let c = bytes[i];
            if c == b' ' || c == b'\t' || c == b'\r' || c == b'\n' {
                panic!("subject cannot contain whitespace or control characters");
            }
            i += 1;
        }

        ValidatedSubject {
            inner: Subject::from_static(subject),
        }
    }

    /// Returns the subject as a string slice.
    #[inline]
    pub fn as_str(&self) -> &str {
        self.inner.as_str()
    }

    /// Converts this validated subject into a `Subject` without validation overhead.
    ///
    /// This is a zero-cost operation since the subject is already validated.
    #[inline]
    pub fn into_subject(self) -> Subject {
        self.inner
    }

    /// Converts this validated subject into a `Subject` (infallible version).
    ///
    /// This method exists for API compatibility and always succeeds.
    #[inline]
    pub fn to_subject_validated(self) -> Result<Subject, SubjectError> {
        Ok(self.inner)
    }
}

impl AsRef<str> for ValidatedSubject {
    fn as_ref(&self) -> &str {
        self.inner.as_str()
    }
}

impl Deref for ValidatedSubject {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.inner.as_str()
    }
}

impl fmt::Display for ValidatedSubject {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.inner)
    }
}
