//! Subjects representing the object store API.

use std::io;

use crate::SubjectBuf;

/// Subject for requests to create a new stream.
pub fn object_meta(store: &str, object: &str) -> io::Result<SubjectBuf> {
    SubjectBuf::new(format!("$O.{}.M.{}", store, object))
        .map_err(Into::into)
}

/// Subject for requests to create a new stream.
pub fn object_all_meta(store: &str) -> io::Result<SubjectBuf> {
    SubjectBuf::new(format!("$O.{}.M.>", store))
        .map_err(Into::into)
}

/// Subject for requests to create a new stream.
pub fn object_chunk(store: &str, object: &str) -> io::Result<SubjectBuf> {
    SubjectBuf::new(format!("$O.{}.C.{}", store, object))
        .map_err(Into::into)
}

/// Subject for requests to create a new stream.
pub fn object_all_chunks(store: &str) -> io::Result<SubjectBuf> {
    SubjectBuf::new(format!("$O.{}.C.>", store))
        .map_err(Into::into)
}