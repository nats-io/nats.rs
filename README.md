# NATS - Rust Client (Beta)
A [Rust](https://www.rust-lang.org/) client for the [NATS messaging system](https://nats.io).

[![License Apache 2](https://img.shields.io/badge/License-Apache2-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![Build Status](https://travis-ci.org/nats-io/nats.rs.svg?branch=master)](http://travis-ci.org/nats-io/nats.rs)

## Motivation
Rust may be the most interesting new language the NATS ecosystem has seen. We believe this client will have a large impact on NATS, distributed systems, and embedded and IoT environments. With Rust we wanted to be as idiomatic as we could be and embrace the language. We moved many things that would have been runtime checks and errors to the compiler, most notably options on connections, and having subscriptions generate multiple styles of iterators, since iterators are a first class citizen in Rust. We also wanted to be aligned with the NATS philosophy of simple, secure, and fast! (The security is coming soon!)

## Feedback

This is a new client, and I and the team are new to Rust, so all feedback welcome!
We encourage all folks in the NATS and Rust ecosystems to help us improve this library. Please open issues, submit PRs, etc.

## Documentation

```bash
> cargo doc --open
```

## Example Usage

`> cargo run --example nats-box -- -h`

Basic connections, and those with options. The compiler will force these to be correct.

```rust
let nc = nats::connect("localhost")?;

let nc2 = nats::Connection::new()
    .with_name("My Rust NATS App")
    .with_user_pass("derek", "s3cr3t!")
    .connect("127.0.0.1")?;
```

### Publish

```rust
nc.publish("foo", "Hello World!")?;

// Serde serialization.
let p = Person {
    first_name: "derek",
    last_name: "collison",
    age: 22,
};

let json = serde_json::to_vec(&p)?;
nc.publish("foo", json)?;

// Publish a request manually.
let reply = nc.new_inbox();
let rsub = nc.subscribe(reply)?;
nc.publish_request("foo", reply, "Help me!")?;
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
}

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
let rsub = nc.subscribe(reply)?;
nc.publish_request("foo", reply, "Help me!")?;
let response = rsub.iter().take(1);
```

## Sync vs Async

The Rust ecosystem has a diverse set of options for async behaviors. This client library can be used somewhat effectively already with async runtimes such as async-std and tokio. Going forward we look to provide an async client. Publish today is mostly non-blocking, so largest API change would be around subscriptions being streams vs iterators by default. Also been researching sinks and whether or not they make sense. Would probably be a config feature for async wnd options for most runtimes like async-std and tokio.

## Features
The following is a list of features currently supported and planned for the near future.

* [X] Basic Publish/Subscribe
* [X] Request/Reply - Singelton and Streams
* [ ] Authentication
  * [X] Token
  * [X] User/Password
  * [ ] Nkeys
  * [ ] User JWTs (NATS 2.0)
* [ ] Reconnect logic
* [ ] TLS support
* [ ] Direct async support
* [X] Crates.io listing

### Miscellaneous TODOs
* [ ] Ping timer
* [X] msg.respond
* [ ] Drain mode
* [ ] COW for received messages
* [X] Sub w/ handler can't do iter()
* [ ] Backup servers for option
* [X] Travis integration
