#[tokio::main]
async fn main() {
    let nc = async_nats::connect("nats://localhost:4222").await.unwrap();

    // NATS-DOC-START
    // Publish a message to the "greetings" subject
    nc.subscribe("weather.updates").await.unwrap();
    // NATS-DOC-END
}
