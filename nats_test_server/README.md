# Test server

A test NATS server for testing the NATS rust client and applications using it. Allows injection of bugsand useful for testing a number of things.

## Example usage

```rust
#[test]
fn test_use_nats() {
  let nats = NatsTestServer::build().spawn();

  let my_component1 = component(nats.address())
  let my_component2 = component(nats.address())

  // test component behaviour

  let nats = nats.restart().spawn();

  // test behaviour after restart
} // server is stopped on drop

#[test]
fn test_use_buggy_nats() {
  let nats = NatsTestServer::build().bugginess(400).spawn(); // a 1/400 chance of restarting on any given message

  // bugginess test
}
```

## Limitations

* `hop_ports` doesn't make any sense for multiple clients
