use std::error::Error;
use std::fmt::{Debug, Display};

/// The error type for the NATS client, generic by the kind of error.
#[derive(Debug)]
pub struct NatsError<Kind>
where
    Kind: Clone + Debug + PartialEq,
{
    pub(crate) kind: Kind,
    pub(crate) source: Option<crate::Error>,
}

impl<Kind> NatsError<Kind>
where
    Kind: Clone + Debug + PartialEq,
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

    // TODO: shouldn't this method to be pub(crate) ?
    //       or maybe it is better to expose the source error?
    pub fn format_source(&self) -> String {
        self.source
            .as_ref()
            .map(|err| err.to_string())
            .unwrap_or("unknown".to_string())
    }

    pub fn kind(&self) -> Kind {
        self.kind.clone()
    }
}

impl<Kind> Error for NatsError<Kind>
where
    Kind: PartialEq + Clone + Debug,
    NatsError<Kind>: Debug + Display,
{
}

impl<Kind> From<Kind> for NatsError<Kind>
where
    Kind: PartialEq + Clone + Debug,
{
    fn from(kind: Kind) -> Self {
        Self { kind, source: None }
    }
}

/// Enables wrapping source errors to the crate-specific error type
/// by additionally specifying the kind of the target error.
trait WithKind<Kind>
where
    Kind: Clone + Debug + PartialEq,
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
    Kind: Clone + Debug + PartialEq,
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

    // Define a custom error type as a public struct
    type FooError = NatsError<FooErrorKind>;

    // Implement the Display trait for the custom error type
    // to unlock the implementation of the Error trait.
    impl Display for FooError {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "foo error")
        }
    }

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
    fn format_source_with_source() {
        let source = std::io::Error::new(std::io::ErrorKind::Other, "foo");
        let error = FooError::with_source(FooErrorKind::Bar, source);
        assert_eq!(error.format_source(), "foo");
    }

    #[test]
    fn format_source_without_source() {
        let error: FooError = FooErrorKind::Bar.into();
        assert_eq!(error.format_source(), "unknown");
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
        assert!(error.source.unwrap().to_string().contains("foo"));
    }
}
