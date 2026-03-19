use async_nats::jetstream;
use futures_util::StreamExt;

fn main() {
    console_error_panic_hook::set_once();
    wasm_bindgen_futures::spawn_local(async {
        let client = async_nats::connect("ws://localhost:8444")
            .await
            .expect("failed to connect to NATS server");

        let mut sub = client
            .subscribe("test.*")
            .await
            .expect("failed to subscribe");
        client
            .publish("test.subject", "Hello, world from WASM!".into())
            .await
            .expect("failed to publish message");

        let jetstream = jetstream::new(client);

        let kv = jetstream
            .create_or_update_key_value(async_nats::jetstream::kv::Config {
                bucket: "test".to_string(),
                ..Default::default()
            })
            .await
            .expect("failed to create or update KV bucket");

        // wait a bit to ensure the message is sent before the program exits
        while let Some(msg) = sub.next().await {
            web_sys::console::log_1(&format!("Received message: {:?}", msg).into());
            kv.put(msg.subject, msg.payload)
                .await
                .expect("failed to put message in KV bucket");
        }
    });
}
