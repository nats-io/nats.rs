#[test]
fn foo() {
    let cmd = r#"{"verbose":false,"pedantic":false,"tls_required":false,"name":"NATS Sample Subscriber","lang":"go","version":"1.9.2","protocol":1,"echo":true}"#;
    let info: nats::ConnectInfo = serde_json::from_str(&cmd).unwrap();
}
