// Copyright 2020-2022 The NATS Authors
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
//
//! JetStream is a built-in persistence layer for NATS that provides powerful
//! [stream][crate::jetstream::stream::Stream]-based messaging capabilities,
//! with integrated support for both *at least once* and *exactly once* delivery semantics.
//!
//! To begin using JetStream, you need to create a new [Context] object, which serves as the entry point to the JetStream API.
//!
//! # Examples
//!
//! Below are some examples that demonstrate how to use JetStream for publishing and consuming messages.
//!
//! ### Publishing and Consuming Messages
//!
//! This example demonstrates how to publish messages to a JetStream stream and consume them using a pull-based consumer.
//!
//! ```no_run
//! # #[tokio::main]
//! # async fn mains() -> Result<(), async_nats::Error> {
//! use futures::StreamExt;
//! use futures::TryStreamExt;
//!
//! // Connect to NATS server
//! let client = async_nats::connect("localhost:4222").await?;
//!
//! // Create a JetStream instance
//! let jetstream = async_nats::jetstream::new(client);
//!
//! // Get or create a stream
//! let stream = jetstream
//!     .get_or_create_stream(async_nats::jetstream::stream::Config {
//!         name: "events".to_string(),
//!         max_messages: 10_000,
//!         ..Default::default()
//!     })
//!     .await?;
//!
//! // Publish a message to the stream
//! jetstream.publish("events", "data".into()).await?;
//!
//! // Get or create a pull-based consumer
//! let consumer = stream
//!     .get_or_create_consumer(
//!         "consumer",
//!         async_nats::jetstream::consumer::pull::Config {
//!             durable_name: Some("consumer".to_string()),
//!             ..Default::default()
//!         },
//!     )
//!     .await?;
//!
//! // Consume messages from the consumer
//! let mut messages = consumer.messages().await?.take(100);
//! while let Ok(Some(message)) = messages.try_next().await {
//!     println!("message receiver: {:?}", message);
//!     message.ack().await?;
//! }
//!
//! Ok(())
//! # }
//! ```
//!
//! ### Consuming Messages in Batches
//!
//! This example demonstrates how to consume messages in batches from a JetStream stream using a sequence-based consumer.
//!
//! ```no_run
//! # #[tokio::main]
//! # async fn mains() -> Result<(), async_nats::Error> {
//! use futures::StreamExt;
//! use futures::TryStreamExt;
//!
//! // Connect to NATS server
//! let client = async_nats::connect("localhost:4222").await?;
//!
//! // Create a JetStream instance
//! let jetstream = async_nats::jetstream::new(client);
//!
//! // Get or create a stream
//! let stream = jetstream
//!     .get_or_create_stream(async_nats::jetstream::stream::Config {
//!         name: "events".to_string(),
//!         max_messages: 10_000,
//!         ..Default::default()
//!     })
//!     .await?;
//!
//! // Publish a message to the stream
//! jetstream.publish("events", "data".into()).await?;
//!
//! // Get or create a pull-based consumer
//! let consumer = stream
//!     .get_or_create_consumer(
//!         "consumer",
//!         async_nats::jetstream::consumer::pull::Config {
//!             durable_name: Some("consumer".to_string()),
//!             ..Default::default()
//!         },
//!     )
//!     .await?;
//!
//! // Consume messages from the consumer in batches
//! let mut batches = consumer.sequence(50)?.take(10);
//! while let Ok(Some(mut batch)) = batches.try_next().await {
//!     while let Some(Ok(message)) = batch.next().await {
//!         println!("message receiver: {:?}", message);
//!         message.ack().await?;
//!     }
//! }
//!
//! Ok(())
//! # }
//! ```

use crate::Client;

pub mod account;
pub mod consumer;
pub mod context;
mod errors;
pub mod kv;
pub mod message;
pub mod object_store;
pub mod publish;
pub mod response;
pub mod stream;

pub use context::Context;
pub use errors::Error;
pub use errors::ErrorCode;
pub use message::{AckKind, Message};

/// Creates a new JetStream [Context] that provides JetStream API for managing and using [Streams][crate::jetstream::stream::Stream],
/// [Consumers][crate::jetstream::consumer::Consumer], key value and object store.
///
/// # Examples
///
/// ```no_run
/// # #[tokio::main]
/// # async fn main() -> Result<(), async_nats::Error> {
///
/// let client = async_nats::connect("localhost:4222").await?;
/// let jetstream = async_nats::jetstream::new(client);
///
/// jetstream.publish("subject", "data".into()).await?;
/// # Ok(())
/// # }
/// ```
pub fn new(client: Client) -> Context {
    Context::new(client)
}

/// Creates a new JetStream [Context] with given JetStream domain.
///
/// # Examples
///
/// ```no_run
/// # #[tokio::main]
/// # async fn main() -> Result<(), async_nats::Error> {
///
/// let client = async_nats::connect("localhost:4222").await?;
/// let jetstream = async_nats::jetstream::with_domain(client, "hub");
///
/// jetstream.publish("subject", "data".into()).await?;
/// # Ok(())
/// # }
/// ```
pub fn with_domain<T: AsRef<str>>(client: Client, domain: T) -> Context {
    context::Context::with_domain(client, domain)
}

/// Creates a new JetStream [Context] with given JetStream prefix.
/// By default it is `$JS.API`.
/// # Examples
///
/// ```no_run
/// # #[tokio::main]
/// # async fn main() -> Result<(), async_nats::Error> {
///
/// let client = async_nats::connect("localhost:4222").await?;
/// let jetstream = async_nats::jetstream::with_prefix(client, "JS.acc@hub.API");
///
/// jetstream.publish("subject", "data".into()).await?;
/// # Ok(())
/// # }
/// ```
pub fn with_prefix(client: Client, prefix: &str) -> Context {
    context::Context::with_prefix(client, prefix)
}

/// Checks if a name passed in JetStream API is valid one.
/// The restrictions are there because some fields in the JetStream configs are passed as part of the subject to apply permissions.
/// Examples are stream names, consumer names, etc.
pub(crate) fn is_valid_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|c| !c.is_ascii_whitespace() && c != b'.' && c != b'*' && c != b'>')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_valid_name() {
        assert!(is_valid_name("stream"));
        assert!(!is_valid_name("str>eam"));
        assert!(!is_valid_name("str*eam"));
        assert!(!is_valid_name("name.name"));
        assert!(!is_valid_name("name name"));
        assert!(!is_valid_name(">"));
        assert!(!is_valid_name(""));
    }
}
