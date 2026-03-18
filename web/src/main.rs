fn main() {
    console_error_panic_hook::set_once();
    wasm_bindgen_futures::spawn_local(async {
        async_nats::connect("ws://localhost:8444")
            .await
            .expect("failed to connect to NATS server");
    });
}
