use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::Deref;
use std::str::{from_utf8, Utf8Error};

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

    /// Returns true if this subject needs validation before use.
    /// Default implementation returns true for safety.
    fn needs_validation(&self) -> bool {
        true
    }
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

    fn needs_validation(&self) -> bool {
        false // Already validated!
    }
}

impl ToSubject for &ValidatedSubject {
    fn to_subject(&self) -> Subject {
        self.inner.clone()
    }

    fn needs_validation(&self) -> bool {
        false // Already validated!
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

/// A `ValidatedSubject` is a subject that has been pre-validated to ensure it conforms to NATS subject rules.
/// This type guarantees at the type level that the subject is valid, allowing publish operations to skip validation.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ValidatedSubject {
    inner: Subject,
}

/// Error returned when validating a subject
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubjectError {
    /// The subject contains invalid characters or format
    InvalidFormat,
}

impl fmt::Display for SubjectError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SubjectError::InvalidFormat => {
                write!(
                    f,
                    "Invalid subject: contains spaces, control characters, or starts/ends with '.'"
                )
            }
        }
    }
}

impl std::error::Error for SubjectError {}

impl ValidatedSubject {
    /// Creates a new `ValidatedSubject` from a string, validating it first.
    ///
    /// # Examples
    /// ```
    /// use async_nats::subject::{SubjectError, ValidatedSubject};
    ///
    /// let subject = ValidatedSubject::new("events.data").unwrap();
    /// assert!(ValidatedSubject::new("events data").is_err());
    /// ```
    pub fn new<S: Into<Subject>>(s: S) -> Result<Self, SubjectError> {
        let subject = s.into();
        if !crate::is_valid_subject(&subject) {
            return Err(SubjectError::InvalidFormat);
        }
        Ok(Self { inner: subject })
    }

    /// Creates a new `ValidatedSubject` from a static string.
    /// This should only be used with compile-time constants that are known to be valid.
    ///
    /// # Safety
    /// The caller must ensure the subject is valid according to NATS rules.
    ///
    /// # Examples
    /// ```
    /// use async_nats::subject::ValidatedSubject;
    ///
    /// const EVENTS_TOPIC: ValidatedSubject = ValidatedSubject::from_static("events.data");
    /// ```
    pub const fn from_static(s: &'static str) -> Self {
        // Note: We can't validate at compile time in stable Rust,
        // so this is marked as a const fn that should only be used with known-valid strings
        Self {
            inner: Subject::from_static(s),
        }
    }

    /// Converts the `ValidatedSubject` into the inner `Subject`.
    pub fn into_inner(self) -> Subject {
        self.inner
    }

    /// Returns a reference to the inner `Subject`.
    pub fn as_subject(&self) -> &Subject {
        &self.inner
    }

    /// Extracts a string slice containing the entire subject.
    pub fn as_str(&self) -> &str {
        self.inner.as_str()
    }
}

impl TryFrom<String> for ValidatedSubject {
    type Error = SubjectError;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        ValidatedSubject::new(s)
    }
}

impl TryFrom<&str> for ValidatedSubject {
    type Error = SubjectError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        ValidatedSubject::new(s)
    }
}

impl TryFrom<Subject> for ValidatedSubject {
    type Error = SubjectError;

    fn try_from(subject: Subject) -> Result<Self, Self::Error> {
        if !crate::is_valid_subject(&subject) {
            return Err(SubjectError::InvalidFormat);
        }
        Ok(Self { inner: subject })
    }
}

impl From<ValidatedSubject> for Subject {
    fn from(validated: ValidatedSubject) -> Self {
        validated.inner
    }
}

impl AsRef<str> for ValidatedSubject {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl AsRef<Subject> for ValidatedSubject {
    fn as_ref(&self) -> &Subject {
        &self.inner
    }
}

impl fmt::Display for ValidatedSubject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl Serialize for ValidatedSubject {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for ValidatedSubject {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        ValidatedSubject::try_from(s).map_err(serde::de::Error::custom)
    }
}
