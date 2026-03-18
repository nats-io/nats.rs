use futures_util::StreamExt;

fn main() {
    console_error_panic_hook::set_once();
    wasm_bindgen_futures::spawn_local(async {
        let client = async_nats::connect("ws://localhost:8444")
            .await
            .expect("failed to connect to NATS server");

        let mut sub = client
            .subscribe("subject")
            .await
            .expect("failed to subscribe");
        client
            .publish("subject", "Hello, world from WASM!".into())
            .await
            .expect("failed to publish message");

        // wait a bit to ensure the message is sent before the program exits
        while let Some(msg) = sub.next().await {
            web_sys::console::log_1(&format!("Received message: {:?}", msg).into());
        }
    });
}
