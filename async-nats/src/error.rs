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

use std::fmt::{Debug, Display};

/// The error type for the NATS client, generic by the kind of error.
#[derive(Debug)]
pub struct Error<Kind>
where
    Kind: Clone + Debug + Display + PartialEq,
{
    pub(crate) kind: Kind,
    pub(crate) source: Option<crate::Error>,
}

impl<Kind> Error<Kind>
where
    Kind: Clone + Debug + Display + PartialEq,
{
    pub fn new(kind: Kind) -> Self {
        Self { kind, source: None }
    }

    pub fn with_source<S>(kind: Kind, source: S) -> Self
    where
        S: Into<crate::Error>,
    {
        Self {
            kind,
            source: Some(source.into()),
        }
    }

    // In some cases the kind doesn't implement `Copy` trait
    pub fn kind(&self) -> Kind {
        self.kind.clone()
    }
}

impl<Kind> Display for Error<Kind>
where
    Kind: Clone + Debug + Display + PartialEq,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(err) = &self.source {
            write!(f, "{}: {}", self.kind, err)
        } else {
            write!(f, "{}", self.kind)
        }
    }
}

impl<Kind> std::error::Error for Error<Kind>
where
    Kind: Clone + Debug + Display + PartialEq,
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_ref()
            .map(|boxed| boxed.as_ref() as &(dyn std::error::Error + 'static))
    }
}

/// Marker trait for types that can be used as the kind of [`Error`].
///
/// It bounds the `From<Kind> for Error<Kind>` conversion instead of the plain
/// `Clone + Debug + Display + PartialEq` bounds: a `From` implementation generic
/// over all types matching those bounds makes a universal coherence claim, which
/// foreign crates can conflict with by adding implementations of their own
/// (E0119, as `time` 0.3.48 did). A local marker trait cannot be implemented by
/// upstream crates for their types, so the conversion provably never overlaps
/// with foreign implementations.
///
/// Implement it for custom kinds used with [`Error`]:
///
/// ```
/// use std::fmt::{self, Display, Formatter};
///
/// #[derive(Clone, Debug, PartialEq)]
/// enum MyErrorKind {
///     Failed,
/// }
///
/// impl Display for MyErrorKind {
///     fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
///         write!(f, "failed")
///     }
/// }
///
/// impl async_nats::error::ErrorKind for MyErrorKind {}
///
/// let error: async_nats::error::Error<MyErrorKind> = MyErrorKind::Failed.into();
/// ```
pub trait ErrorKind: Clone + Debug + Display + PartialEq {}

/// Implements [`ErrorKind`] for the given types. Only list concrete kind types
/// defined in this crate. Do not implement the trait for foreign types or as a
/// blanket implementation over the required bounds; either would reintroduce the
/// coherence hazard this trait exists to remove.
macro_rules! error_kinds {
    ($($kind:ty),* $(,)?) => {
        $(impl $crate::error::ErrorKind for $kind {})*
    };
}
pub(crate) use error_kinds;

impl<Kind> From<Kind> for Error<Kind>
where
    Kind: ErrorKind,
{
    fn from(kind: Kind) -> Self {
        Self { kind, source: None }
    }
}

#[cfg(test)]
mod test {
    #![allow(dead_code)]

    use super::*;
    use std::fmt::Formatter;

    // Define a custom error kind as a public enum
    #[derive(Clone, Debug, PartialEq)]
    enum FooErrorKind {
        Bar,
        Baz,
    }

    impl Display for FooErrorKind {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            match self {
                Self::Bar => write!(f, "bar error"),
                Self::Baz => write!(f, "baz error"),
            }
        }
    }

    error_kinds!(FooErrorKind);

    // Define a custom error type as a public struct
    type FooError = Error<FooErrorKind>;

    #[test]
    fn new() {
        let error = FooError::new(FooErrorKind::Bar);
        assert_eq!(error.kind, FooErrorKind::Bar);
        assert!(error.source.is_none());
    }

    #[test]
    fn with_source() {
        let source = std::io::Error::other("foo");
        let error = FooError::with_source(FooErrorKind::Bar, source);
        assert_eq!(error.kind, FooErrorKind::Bar);
        assert_eq!(error.source.unwrap().to_string(), "foo");
    }

    #[test]
    fn kind() {
        let error: FooError = FooErrorKind::Bar.into();
        let kind = error.kind();
        // ensure the kind can be invoked multiple times even though Copy is not implemented
        let _ = error.kind();
        assert_eq!(kind, FooErrorKind::Bar);
    }

    #[test]
    fn display_without_source() {
        let error: FooError = FooErrorKind::Bar.into();
        assert_eq!(format!("{error}"), "bar error");
    }

    #[test]
    fn from() {
        let error: FooError = FooErrorKind::Bar.into();
        assert_eq!(error.kind, FooErrorKind::Bar);
        assert!(error.source.is_none());
    }
}
