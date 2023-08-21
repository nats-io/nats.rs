<p align="center">
  <img src="nats/logo/logo.svg">
</p>

<p align="center">
    A <a href="https://www.rust-lang.org/">Rust</a> client for the <a href="https://nats.io">NATS messaging system</a>.
</p>

## Motivation

Rust may be one of the most interesting new languages the NATS ecosystem has seen.
We believe this client will have a large impact on NATS, distributed systems, and
embedded and IoT environments. With Rust, we wanted to be as idiomatic as we
could be and lean into the strengths of the language. We moved many things that
would have been runtime checks and errors to the compiler, most notably options
on connections, and having subscriptions generate multiple styles of iterators
since iterators are first-class citizens in Rust. We also wanted to be aligned
with the NATS philosophy of simple, secure, and fast!

## Clients

There are two clients available in two separate crates:

### async-nats

[![License Apache 2](https://img.shields.io/badge/License-Apache2-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![Crates.io](https://img.shields.io/crates/v/async-nats.svg)](https://crates.io/crates/async-nats)
[![Documentation](https://docs.rs/async-nats/badge.svg)](https://docs.rs/async-nats/)
[![Build Status](https://github.com/nats-io/nats.rs/actions/workflows/test.yml/badge.svg?branch=main)](https://github.com/nats-io/nats.rs/actions)

New async Tokio-based NATS client.

Supports:

* Core NATS
* JetStream API
* JetStream Management API
* Key Value Store
* Object Store
* Service API

Any feedback related to this client is welcomed.

> **Note:** async client is still <1.0.0 version and will introduce breaking changes.

### nats

[![License Apache 2](https://img.shields.io/badge/License-Apache2-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![Crates.io](https://img.shields.io/crates/v/nats.svg)](https://crates.io/crates/nats)
[![Documentation](https://docs.rs/nats/badge.svg)](https://docs.rs/nats/)
[![Build Status](https://github.com/nats-io/nats.rs/actions/workflows/test.yml/badge.svg?branch=main)](https://github.com/nats-io/nats.rs/actions)

Legacy *synchronous* client that supports:

* Core NATS
* JetStream API
* JetStream Management API
* Key Value Store
* Object Store

This client will be deprecated soon, when `async-nats` reaches version 1.0, with a sync wrapper around it.

### Documentation

Please refer each crate docs for API reference and examples.

## Feedback

We encourage all folks in the NATS and Rust ecosystems to help us
improve this library. Please open issues, submit PRs, etc. We're
available in the `rust` channel on [the NATS slack](https://slack.nats.io)
as well!

