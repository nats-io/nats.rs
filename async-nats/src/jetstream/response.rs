use serde::Deserialize;

/// An error description returned in a response to a jetstream request.
#[derive(Debug, Deserialize)]
pub struct Error {
    /// Code
    pub code: u64,

    /// Description
    pub description: String,
}

/// A response returned from a request to jetstream.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum Response<T> {
    Err { error: Error },
    Ok(T),
}
