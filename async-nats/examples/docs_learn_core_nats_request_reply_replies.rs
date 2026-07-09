use futures::StreamExt;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    let client = async_nats::connect("nats://localhost:4222").await?;

    // NATS-DOC-START
    // Gather more than one reply to a single request. A plain request() returns
    // only the first reply, so when several services may answer, subscribe to
    // your own inbox, publish the request with that inbox as the reply subject,
    // and collect replies until they stop arriving.
    let order = r#"{"order_id":"ord_8w2k","customer":"acme-co","total_cents":4200,"ts":"2026-05-22T10:14:22Z"}"#;
    let inbox = client.new_inbox();
    let mut sub = client.subscribe(inbox.clone()).await?;
    client
        .publish_with_reply("orders.inventory.check", inbox, order.into())
        .await?;
    client.flush().await?;

    let mut replies = Vec::new();
    // Stop once no further reply arrives within the gap deadline.
    while let Ok(Some(msg)) = tokio::time::timeout(Duration::from_millis(300), sub.next()).await {
        replies.push(String::from_utf8_lossy(&msg.payload).to_string());
    }

    println!("gathered {} replies", replies.len());
    // NATS-DOC-END

    Ok(())
}
