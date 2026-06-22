use async_nats::jetstream;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    // Connect to NATS and get a JetStream context.
    let client = async_nats::connect("nats://localhost:4222").await?;
    let js = jetstream::new(client);

    // NATS-DOC-START
    // Fetch the ORDERS stream and read its current info from the server.
    let mut stream = js.get_stream("ORDERS").await?;
    let info = stream.info().await?;

    // Print the key fields: name, captured subjects, and message count.
    println!("Stream name:    {}", info.config.name);
    println!("Subjects:       {:?}", info.config.subjects);
    println!("Message count:  {}", info.state.messages);
    // NATS-DOC-END

    Ok(())
}
