use std::error::Error;
use std::fmt::{Debug, Display};

/// The error type for the NATS client, generic by the kind of error.
#[derive(Debug)]
pub struct NatsError<Kind>
where
    Kind: Clone + Debug + Display + PartialEq,
{
    pub(crate) kind: Kind,
    pub(crate) source: Option<crate::Error>,
}

impl<Kind> NatsError<Kind>
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

impl<Kind> Display for NatsError<Kind>
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

impl<Kind> Error for NatsError<Kind> where Kind: Clone + Debug + Display + PartialEq {}

impl<Kind> From<Kind> for NatsError<Kind>
where
    Kind: Clone + Debug + Display + PartialEq,
{
    fn from(kind: Kind) -> Self {
        Self { kind, source: None }
    }
}

/// Enables wrapping source errors to the crate-specific error type
/// by additionally specifying the kind of the target error.
trait WithKind<Kind>
where
    Kind: Clone + Debug + Display + PartialEq,
    Self: Into<crate::Error>,
{
    fn with_kind(self, kind: Kind) -> NatsError<Kind> {
        NatsError::<Kind> {
            kind,
            source: Some(self.into()),
        }
    }
}

impl<E, Kind> WithKind<Kind> for E
where
    Kind: Clone + Debug + Display + PartialEq,
    E: Into<crate::Error>,
{
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
    type FooError = NatsError<FooErrorKind>;

    #[test]
    fn new() {
        let error = FooError::new(FooErrorKind::Bar);
        assert_eq!(error.kind(), FooErrorKind::Bar);
        assert!(error.source().is_none());
    }

    #[test]
    fn with_source() {
        let source = std::io::Error::new(std::io::ErrorKind::Other, "foo");
        let error = FooError::with_source(FooErrorKind::Bar, source);
        assert_eq!(error.kind(), FooErrorKind::Bar);
        assert_eq!(error.source.unwrap().to_string(), "foo");
    }

    #[test]
    fn kind() {
        let error: FooError = FooErrorKind::Bar.into();
        let kind = error.kind();
        let _ = error.kind(); // ensure the kind can be invoked multiple times
        assert_eq!(kind, FooErrorKind::Bar);
    }

    #[test]
    fn display_with_source() {
        let source = std::io::Error::new(std::io::ErrorKind::Other, "foo");
        let error = source.with_kind(FooErrorKind::Bar);
        assert_eq!(format!("{}", error), "bar error: foo");
    }

    #[test]
    fn display_without_source() {
        let error: FooError = FooErrorKind::Bar.into();
        assert_eq!(format!("{}", error), "bar error");
    }

    #[test]
    fn from() {
        let error: FooError = FooErrorKind::Bar.into();
        assert_eq!(error.kind(), FooErrorKind::Bar);
        assert!(error.source().is_none());
    }

    #[test]
    fn with_kind() {
        let source = std::io::Error::new(std::io::ErrorKind::Other, "foo");
        let error: FooError = source.with_kind(FooErrorKind::Baz);
        assert_eq!(error.kind(), FooErrorKind::Baz);
        assert_eq!(format!("{}", error), "baz error: foo");
    }
}
