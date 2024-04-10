<p align="center">
  <img src="logo/logo.svg">
</p>

<p align="center">
    A <a href="https://www.rust-lang.org/">Rust</a> client for the <a href="https://nats.io">NATS messaging system</a>.
</p>

## Status

[![License Apache 2](https://img.shields.io/badge/License-Apache2-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![Crates.io](https://img.shields.io/crates/v/nats.svg)](https://crates.io/crates/nats)
[![Documentation](https://docs.rs/nats/badge.svg)](https://docs.rs/nats/)
[![Build Status](https://github.com/nats-io/nats.rs/actions/workflows/test.yml/badge.svg?branch=main)](https://github.com/nats-io/nats.rs/actions)

## :warning: Legacy Notice

This is the old legacy client. It will not get new features or updates beyond critical security fixes.

Use [async nats](https://crates.io/crates/async-nats) instead.

If you would like to use the async client in sync context, check the [async-nats examples](https://github.com/nats-io/nats.rs/tree/main/async-nats/examples).

## Motivation

Rust may be one of the most interesting new languages the NATS ecosystem has seen.
We believe this client will have a large impact on NATS, distributed systems, and
embedded and IoT environments. With Rust, we wanted to be as idiomatic as we
could be and lean into the strengths of the language. We moved many things that
would have been runtime checks and errors to the compiler, most notably options
on connections, and having subscriptions generate multiple styles of iterators
since iterators are first-class citizens in Rust. We also wanted to be aligned
with the NATS philosophy of simple, secure, and fast!

## Feedback

We encourage all folks in the NATS and Rust ecosystems to help us
improve this library. Please open issues, submit PRs, etc. We're
available in the `rust` channel on [the NATS slack](https://slack.nats.io)
as well!

## Example Usage

`> cargo run --example nats-box -- -h`

Basic connections, and those with options. The compiler will force these to be correct.

```rust
fn main() -> std::io::Result<()> {
let nc = nats::connect("demo.nats.io")?;

let nc2 = nats::Options::with_user_pass("derek", "s3cr3t!")
    .with_name("My Rust NATS App")
    .connect("127.0.0.1")?;

let nc3 = nats::Options::with_credentials("path/to/my.creds")
    .connect("connect.ngs.global")?;

let nc4 = nats::Options::new()
    .add_root_certificate("my-certs.pem")
    .connect("tls://demo.nats.io:4443")?;
Ok(())
}
```
<!--
### Publish

```rust
nc.publish("my.subject", "Hello World!")?;

nc.publish("my.subject", "my message")?;

// Publish a request manually.
let reply = nc.new_inbox();
let rsub = nc.subscribe(&reply)?;
nc.publish_request("my.subject", &reply, "Help me!")?;
```

### Subscribe

```rust
let sub = nc.subscribe("foo")?;
for msg in sub.messages() {}

// Using next.
if let Some(msg) = sub.next() {}

// Other iterators.
for msg in sub.try_iter() {}
for msg in sub.timeout_iter(Duration::from_secs(10)) {}

// Using a threaded handler.
let sub = nc.subscribe("bar")?.with_handler(move |msg| {
    println!("Received {}", &msg);
    Ok(())
});

// Queue subscription.
let qsub = nc.queue_subscribe("foo", "my_group")?;
```

### Request/Response

```rust
let resp = nc.request("foo", "Help me?")?;

// With a timeout.
let resp = nc.request_timeout("foo", "Help me?", Duration::from_secs(2))?;

// With multiple responses.
for msg in nc.request_multi("foo", "Help")?.iter() {}

// Publish a request manually.
let reply = nc.new_inbox();
let rsub = nc.subscribe(&reply)?;
nc.publish_request("foo", &reply, "Help me!")?;
let response = rsub.iter().take(1);
```

### Jetstream

Create a new stream with default options:
```rust
let js = nats::jetstream::new(nc);

// add_stream converts a str into a
// default `StreamConfig`.
js.add_stream("my_stream")?;
```

Create a new consumer:
```rust
let nc = nats::connect("demo.nats.io")?;
let js = nats::jetstream::new(nc);

js.add_stream("my_stream")?;
js.add_consumer("my_stream", "my_consumer")?;
```

Create a new subscription:
```rust
let nc = nats::connect("demo.nats.io")?;
let js = nats::jetstream::new(nc);

js.add_stream("my_stream")?;
let subscription = js.subscribe("my_stream")?;

// add stream with options
js.add_stream(nats::jetstream::StreamConfig{
    name: "my_another_stream".to_string(),
    max_msgs: 2000,
    discard: nats::jetstream::DiscardPolicy::Old,
    ..Default::default()
})?
})
``` -->
This will attempt to bind to an existing consumer if it exists, otherwise it will create a new internally managed consumer resource that gets destroyed when the subscription is dropped.


## Minimum Supported Rust Version (MSRV)

The minimum supported Rust version is 1.58.0.

## Sync vs Async

The Rust ecosystem has a diverse set of options for async programming. This client library can be used with any async runtime out of the box, such as async-std and tokio.

The async interface provided by this library is implemented as just a thin wrapper around its sync interface. Those two interface styles look very similar, and you're free to choose whichever works best for your application.

<em>*NOTE:* This crate uses a thread pool from [blocking crate](https://crates.io/crates/blocking/1.1.0). By default, it limits the number of threads to 500. It can be overridden by setting `BLOCKING_MAX_THREADS` environment variable and set between 1 and 10000. Be careful when spinning a lot of async operations, as it may drain the thread pool and block forever until it's reworked</em>
