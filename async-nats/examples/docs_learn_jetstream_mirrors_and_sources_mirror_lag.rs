use async_nats::jetstream;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    // Connect to NATS and get a JetStream context.
    let client = async_nats::connect("nats://localhost:4222").await?;
    let js = jetstream::new(client);

    // NATS-DOC-START
    // A mirror is eventually consistent. Read its lag before trusting it to
    // hold what the upstream just received: 0 means fully caught up.
    let mut stream = js.get_stream("ORDERS-ARCHIVE").await?;
    let info = stream.info().await?;
    let mirror = info.mirror.as_ref().unwrap();

    println!("Upstream:  {}", mirror.name);
    println!("Lag:       {}", mirror.lag);
    println!("Last seen: {:?} ago", mirror.active);
    // NATS-DOC-END

    Ok(())
}
