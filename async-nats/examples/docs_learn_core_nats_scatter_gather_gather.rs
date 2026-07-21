use futures::StreamExt;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    let client = async_nats::connect("nats://localhost:4222").await?;

    // NATS-DOC-START
    // Scatter one request to every shipping-quote provider and gather the
    // replies. Subscribe to a private inbox, publish the request with that inbox
    // as the reply subject, then collect quotes until they stop arriving and
    // pick the cheapest.
    let order = r#"{"order_id":"ord_8w2k","customer":"acme-co","total_cents":4200,"ts":"2026-05-22T10:14:22Z"}"#;
    let inbox = client.new_inbox();
    let mut sub = client.subscribe(inbox.clone()).await?;
    client
        .publish_with_reply("shipping.quote", inbox, order.into())
        .await?;
    client.flush().await?;

    let mut quotes = Vec::new();
    while let Ok(Some(msg)) = tokio::time::timeout(Duration::from_millis(300), sub.next()).await {
        quotes.push(String::from_utf8_lossy(&msg.payload).to_string());
    }

    println!("gathered {} quotes: {:?}", quotes.len(), quotes);
    // NATS-DOC-END

    Ok(())
}
