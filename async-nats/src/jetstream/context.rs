use crate::jetstream::response::Response;
use crate::{Client, Error};
use bytes::Bytes;
use serde::{de::DeserializeOwned, Serialize};
use serde_json;

/// A context which can perform jetstream scoped requests.
#[derive(Debug, Clone)]
pub struct Context {
    client: Client,
    prefix: String,
}

impl Context {
    pub fn new(client: Client) -> Context {
        Context {
            client,
            prefix: "$JS.API".to_string(),
        }
    }

    /// Send a request to the jetstream JSON API.
    pub async fn request<T, V>(
        &mut self,
        subject: String,
        payload: &T,
    ) -> Result<Response<V>, Error>
    where
        T: ?Sized + Serialize,
        V: DeserializeOwned,
    {
        let request = serde_json::to_vec(&payload).map(Bytes::from)?;

        let message = self.client.request(subject, request).await?;
        let response = serde_json::from_slice(message.payload.as_ref())?;

        Ok(response)
    }
}
