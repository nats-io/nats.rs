#[tokio::main]
async fn main() {
    let nc = async_nats::connect("nats://localhost:4222").await.unwrap();

    // NATS-DOC-START
    //  Subscribe to the "weather.updates" subject
    nc.subscribe("weather.updates").await.unwrap();
    // NATS-DOC-END
}
