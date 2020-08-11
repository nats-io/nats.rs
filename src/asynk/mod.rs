//! Asynchronous interface for the NATS client.

pub(crate) mod client;
mod connection;
pub(crate) mod connector;
mod message;
mod proto;
mod subscription;

use std::io;

pub use connection::Connection;
pub use message::Message;
pub use subscription::Subscription;

/// Connect to a NATS server at the given url.
///
/// # Example
///
/// ```
/// # fn main() -> std::io::Result<()> {
/// # smol::run(async {
/// let nc = nats::asynk::connect("demo.nats.io").await?;
/// # Ok(())
/// # })
/// # }
/// ```
pub async fn connect(nats_url: &str) -> io::Result<Connection> {
    crate::Options::new().connect_async(nats_url).await
}
