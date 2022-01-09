//! Subjects representing the JetStream API.

use std::io;

use crate::SubjectBuf;

/// Subject for requests to create a new stream.
pub fn account_info(prefix: &str) -> SubjectBuf {
    SubjectBuf::new_unchecked(format!("{}INFO", prefix))
}

/// Subject for requests to create a new stream.
pub fn create_stream(prefix: &str, name: &str) -> io::Result<SubjectBuf> {
    SubjectBuf::new(format!("{}STREAM.CREATE.{}", prefix, name))
        .map_err(Into::into)
}

/// Subject for requests to update an existing stream.
pub fn update_stream(prefix: &str, name: &str) -> io::Result<SubjectBuf> {
    SubjectBuf::new(format!("{}STREAM.UPDATE.{}", prefix, name))
        .map_err(Into::into)
}

/// Subject for requests to purge an existing stream.
pub fn purge_stream(prefix: &str, name: &str) -> io::Result<SubjectBuf> {
    SubjectBuf::new(format!("{}STREAM.PURGE.{}", prefix, name))
        .map_err(Into::into)
}

/// Subject for requests to delete an existing stream.
pub fn delete_stream(prefix: &str, name: &str) -> io::Result<SubjectBuf> {
    SubjectBuf::new(format!("{}STREAM.DELETE.{}", prefix, name))
        .map_err(Into::into)
}

/// Subject to requests the names of existing streams.
pub fn stream_names(prefix: &str) -> SubjectBuf {
    SubjectBuf::new_unchecked(format!("{}STREAM.NAMES", prefix))
}

/// Subject to requests the list of existing streams.
///
/// This provides more information than [`stream_names()`].
pub fn stream_list(prefix: &str) -> SubjectBuf {
    SubjectBuf::new_unchecked(format!("{}STREAM.LIST", prefix))
}

/// Subject to requests detailed information about a streams.
pub fn stream_info(prefix: &str, stream: &str) -> SubjectBuf {
    SubjectBuf::new_unchecked(format!("{}STREAM.INFO.{}", prefix, stream))
}

/// Subject to requests a specific message from a stream.
pub fn stream_get_message(prefix: &str, stream: &str) -> SubjectBuf {
    SubjectBuf::new_unchecked(format!("{}STREAM.MSG.GET.{}", prefix, stream))
}

/// Subject to requests a specific message from a stream.
pub fn stream_delete_message(prefix: &str, stream: &str) -> SubjectBuf {
    SubjectBuf::new_unchecked(format!("{}STREAM.MSG.DELETE.{}", prefix, stream))
}

/// Subject to requests to create a new consumer for a stream.
pub fn create_consumer(prefix: &str, stream: &str) -> io::Result<SubjectBuf> {
    SubjectBuf::new(format!("{}CONSUMER.CREATE.{}", prefix, stream))
        .map_err(Into::into)
}

/// Subject to requests to create a new durable consumer for a stream.
pub fn create_durable_consumer(prefix: &str, stream: &str, consumer: &str) -> io::Result<SubjectBuf> {
    SubjectBuf::new(format!("{}CONSUMER.DURABLE.CREATE.{}.{}", prefix, stream, consumer))
        .map_err(Into::into)
}

/// Subject to requests to delete a consumer.
pub fn delete_consumer(prefix: &str, stream: &str, consumer: &str) -> io::Result<SubjectBuf> {
    SubjectBuf::new(format!("{}CONSUMER.DELETE.{}.{}", prefix, stream, consumer))
        .map_err(Into::into)
}

/// Subject to requests the list of consumers of a streams.
pub fn consumer_list(prefix: &str, stream: &str) -> SubjectBuf {
    SubjectBuf::new_unchecked(format!("{}CONSUMER.LIST.{}", prefix, stream))
}

/// Subject to requests the information about a consumer.
pub fn consumer_info(prefix: &str, stream: &str, consumer: &str) -> SubjectBuf {
    SubjectBuf::new_unchecked(format!("{}CONSUMER.INFO.{}.{}", prefix, stream, consumer))
}