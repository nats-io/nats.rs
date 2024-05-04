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
    pub(crate) fn new(kind: Kind) -> Self {
        Self { kind, source: None }
    }

    pub(crate) fn with_source<S>(kind: Kind, source: S) -> Self
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

impl<Kind> From<Kind> for Error<Kind>
where
    Kind: Clone + Debug + Display + PartialEq,
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
        let source = std::io::Error::new(std::io::ErrorKind::Other, "foo");
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
        assert_eq!(format!("{}", error), "bar error");
    }

    #[test]
    fn from() {
        let error: FooError = FooErrorKind::Bar.into();
        assert_eq!(error.kind, FooErrorKind::Bar);
        assert!(error.source.is_none());
    }
}
