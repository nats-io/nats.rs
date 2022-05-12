use crate::Client;

pub mod context;
pub mod response;
pub use context::Context;

pub fn new(client: Client) -> Context {
    Context::new(client)
}
