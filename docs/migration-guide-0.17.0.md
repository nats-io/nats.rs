# Migration guide to version [0.17.0](https://github.com/nats-io/nats.rs/releases/tag/untagged-964d5dad97c03c4aa6f4)

Release 0.17.0 among many features introduced a complete rewrite of the JetStream API with a new subscription interface.

This page is meant to make migration to the new version easier.

Lot of changes were also made to keep Rust NATS Client better aligned with Reference NATS Client (Go).

# Introduction of JetStream context

To make setting `Domain` and `API Prefix` more convinient by setting it once, not for every `JetStream` related operation, `JetStream` struct was introduced.

This change resulted in having all `JetStream` API being called as `JetStream` methods. That applies to both `JetStream management` and `JetStream API`.

```rust no_run
fn main() -> std::io::Result<()> {
    let nc = nats::connect("demo.nats.io")?;
    // create Jetstream context
    let js = nats::jetstream::new(nc);

    // JetStream management
    js.add_stream("new_stream")?;
    
    // subscribing to some stream
    let subscription = js.subscribe("events")?;
    
    // publishing to stream
    js.publish("some_stream", "message")?;
    Ok(())
}
```

# JetStream Management

`JetStream management` changed mostly because of add `JetStream` context struct.

Let's take an example of adding new `stream` with pre 0.17.0:

```rust 
fn main() -> std::io::Result<()> {
    use nats_016 as nats;
    let nc = nats::connect("demo.nats.io")?;
    nc.create_stream("stream_one")?;

    // create stream with options
    nc.create_stream(nats::jetstream::StreamConfig{
        name: "stream_two".to_string(),
        max_msgs: 2000,
        discard: nats::jetstream::DiscardPolicy::Old,
        ..Default::default()
    })?;
    Ok(())
}
```

And compare it to the new API:

```rust
fn main() -> std::io::Result<()> {
    let nc = nats::connect("demo.nats.io")?;
    // Initialize JetStream struct first
    let js = nats::jetstream::new(nc);
    // use it
    js.add_stream("stream_three")?;

    // add stream with options
    js.add_stream(nats::jetstream::StreamConfig{
        name: "stream_four".to_string(),
        max_msgs: 2000,
        discard: nats::jetstream::DiscardPolicy::Old,
        ..Default::default()
    })?;

Ok(())
}
```

As you can see one more thing changed in `JetStream Management` - `create_stream` was renamed to `add_stream` to keep better aligment with reference client.

# JetStream API

JetStream interface was **entirely rewritten**.
Those changes were necessary for both cleanup and preparation for introduction of `Key-Value Store` and `Object Store`.

Except all methods being moved to `JetStream` struct, their behaviour and API also changed.

Full example with creating stream, publishing and subscribing with ephemeral subscription:


```rust
fn main() -> Result<(), Box<dyn std::error::Error>> {

    let nc = nats::connect("demo.nats.io")?;
    let js = nats::jetstream::new(nc);
    js.add_stream(&nats::jetstream::StreamConfig {
        name: "events".to_string(),
        subjects: vec![
            "events.>".to_string(),
        ],
        ..Default::default()
    })?;
    js.publish("events.test", "test message")?;
    let sub = js.subscribe("events.>")?;
    sub.with_handler(move |message| {
        println!("received {}", &message);
     Ok(())
    });
    js.delete_stream("events")?;
Ok(()) 
}
```

To see all changes, please check cargo docs and it's examples.