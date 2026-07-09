use async_nats::client::RequestErrorKind;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    let client = async_nats::connect("nats://localhost:4222").await?;

    // NATS-DOC-START
    // Ask the inventory service whether an order's item is in stock. The client
    // creates a private inbox, sends the request, and waits for one reply. A
    // missing service surfaces as NoResponders; a slow one as TimedOut.
    let order = r#"{"order_id":"ord_8w2k","customer":"acme-co","total_cents":4200,"ts":"2026-05-22T10:14:22Z"}"#;
    match client.request("orders.inventory.check", order.into()).await {
        Ok(response) => {
            println!(
                "inventory replied: {}",
                String::from_utf8_lossy(&response.payload)
            );
        }
        Err(err) => match err.kind() {
            RequestErrorKind::NoResponders => println!("no inventory service is running"),
            RequestErrorKind::TimedOut => println!("inventory service did not answer in time"),
            _ => eprintln!("request failed: {}", err),
        },
    }
    // NATS-DOC-END

    Ok(())
}
