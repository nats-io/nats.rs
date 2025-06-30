use futures_util::stream::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::env;

// Use the macro to generate the serialize and deserialize methods on
// struct with basic types.
#[derive(Serialize, Deserialize)]
struct Payload {
    foo: String,
    bar: u8,
}

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    // Use the NATS_URL env variable if defined, otherwise fallback
    // to the default.
    let nats_url = env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string());

    let client = async_nats::connect(nats_url).await?;

    // Create a subscription that handles one message.
    let mut subscriber = client.subscribe("foo").await?.take(1);

    // Construct a Payload value and serialize it.
    let payload = Payload {
        foo: "bar".to_string(),
        bar: 27,
    };
    let bytes = serde_json::to_vec(&json!(payload))?;

    // Publish the serialized payload.
    client.publish("foo", bytes.into()).await?;

    while let Some(message) = subscriber.next().await {
        // Deserialize the message payload into a Payload value.
        // let payload: Payload = serde_json::from_slice(message.payload.as_ref())?;
        if let Ok(payload) = serde_json::from_slice::<Payload>(&message.payload) {
            println!(
                "received payload: foo={:?} bar={:?}",
                payload.foo, payload.bar
            );
        }
    }

    Ok(())
}
