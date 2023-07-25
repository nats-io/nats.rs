# 0.31.0
This release focuses on improvements of heartbeats in JetStream Consumers.

Heartbeats are a tool that tells the user if the given consumer is healthy but does not get any messages or if the reason for no message is an actual problem.
However, if the user was not polling the `Stream` future for the next messages for a long time (because it was slowly processing messages), that could trigger idle heartbeats, as the library could not see the heartbeat messages without messages being polled.

This release fixes it by starting the idle heartbeat timer only after Stream future is polled (which usually means calling `messages.next().await`).

## What's Changed
* Fix unwrap from `HeaderName::from_str` call by @caspervonb in https://github.com/nats-io/nats.rs/pull/1032
* Use idiomatic method for writing `Option` and accessing inner `T` by @paolobarbolini in https://github.com/nats-io/nats.rs/pull/1034
* Add missing sequence number reset by @paolobarbolini in https://github.com/nats-io/nats.rs/pull/1035
* Fix header name range validation by @caspervonb in https://github.com/nats-io/nats.rs/pull/1031
* Simplify consumer checking logic by @paolobarbolini in https://github.com/nats-io/nats.rs/pull/1033
* Fix millis -> nanos typo in `BatchConfig` `expiration` by @paolobarbolini in https://github.com/nats-io/nats.rs/pull/1037
* Fix kv purge with prefix (thanks @brooksmtownsend for reporting it!) by @Jarema in https://github.com/nats-io/nats.rs/pull/1055
* Remove memcpy in object store PUT by @paolobarbolini in https://github.com/nats-io/nats.rs/pull/1039
* Drop subscription on list done by @Jarema in https://github.com/nats-io/nats.rs/pull/1041
* Improve push consumer handling when encountering slow consumers by @Jarema in https://github.com/nats-io/nats.rs/pull/1044
* Rework idle heartbeat for pull consumers by @Jarema in https://github.com/nats-io/nats.rs/pull/1046
* Rework push consumer heartbeats handling by @Jarema in https://github.com/nats-io/nats.rs/pull/1048


**Full Changelog**: https://github.com/nats-io/nats.rs/compare/async-nats/v0.30.0...async-nats/v0.30.1

# 0.30.0
## Overview
This is a big release that introduces almost all breaking changes and API refinements before 1.0.0.
The last two pending breaking items are: 
* Improved builders based on https://github.com/nats-io/nats.rs/discussions/828
* Introduce a `Subject` type (still discussed)

## Breaking Changes
* Update Service for parity with ADR by @Jarema in https://github.com/nats-io/nats.rs/pull/965
* Remove schema and api_url from Service API by @Jarema in https://github.com/nats-io/nats.rs/pull/972
* Add concrete error types to JetStream by @Jarema in https://github.com/nats-io/nats.rs/pull/874
* Add concrete errors to KV by @Jarema in https://github.com/nats-io/nats.rs/pull/1014
* Add concrete errors to Object Store by @Jarema in https://github.com/nats-io/nats.rs/pull/1015
* Add Pull Consumer concrete errors by @Jarema in https://github.com/nats-io/nats.rs/pull/1009
* Use `Bytes` for key-value payloads by @caspervonb in https://github.com/nats-io/nats.rs/pull/939
* Use u64 instead of usize for nanoseconds by @MJayat in https://github.com/nats-io/nats.rs/pull/901
* Prefer returning `&str` over String where applicable by @paolobarbolini in https://github.com/nats-io/nats.rs/pull/878
* Change `stream::Config::duplicate_window` to `Duration` by @n1ghtmare in https://github.com/nats-io/nats.rs/pull/1023

## Added
* Add ordered pull consumer by @Jarema in https://github.com/nats-io/nats.rs/pull/916
* Add `jetstream::Message` `Acker` by @Jarema in https://github.com/nats-io/nats.rs/pull/938
* Add Context::get_consumer_from_stream by @Jarema in https://github.com/nats-io/nats.rs/pull/502
* Add custom auth callback by @Jarema in https://github.com/nats-io/nats.rs/pull/997
* Add custom tls config option by @Jarema in https://github.com/nats-io/nats.rs/pull/903
* Add `watch_with_history` to KV store by @n1ghtmare in https://github.com/nats-io/nats.rs/pull/902
* Implement AsRef<str> for HeaderName by @matthiasbeyer in https://github.com/nats-io/nats.rs/pull/921
* Implement `fmt::Display` for `HeaderName` by @caspervonb in https://github.com/nats-io/nats.rs/pull/924
* Derive `Clone` for `Message` by @mgrachev in https://github.com/nats-io/nats.rs/pull/975
* Add max_bytes for pull consumer config by @piotrpio in https://github.com/nats-io/nats.rs/pull/992
* Add inactive threshold for pull consumer by @paulgb in https://github.com/nats-io/nats.rs/pull/994
* Add const representation for standard header names by @caspervonb in https://github.com/nats-io/nats.rs/pull/946
* Add static representation for custom header names by @caspervonb in https://github.com/nats-io/nats.rs/pull/967
* Add `reconnect_delay_callback` to `ConnectOptions` by @n1ghtmare in https://github.com/nats-io/nats.rs/pull/962
* Makes `Message` serialize/deserialize by @thomastaylor312 in https://github.com/nats-io/nats.rs/pull/998

## Fixed
* Disconnect if pending pings is more than max pings by @caspervonb in https://github.com/nats-io/nats.rs/pull/956
* Fix serialization of `object_store::ObjectInfo` fields by @n1ghtmare in https://github.com/nats-io/nats.rs/pull/895
* Fix ordered consumer handling for stream sequence and heartbeats by @Jarema in https://github.com/nats-io/nats.rs/pull/961
* Canonicalize `header::NATS_LAST_STREAM` by @caspervonb in https://github.com/nats-io/nats.rs/pull/948
* Fix KV update so that it works properly with a JS domain by @protochron in https://github.com/nats-io/nats.rs/pull/1000
* Fix object store compatibility issue with Go implementation by @tinou98 in https://github.com/nats-io/nats.rs/pull/984
* Drop subscription on object read done by @Jarema in https://github.com/nats-io/nats.rs/pull/1011

## Changed
* Make the current `Error` type work with anyhow by @paolobarbolini in https://github.com/nats-io/nats.rs/pull/1004
* Remove collect to Vec in try_read_op by @YaZasnyal in https://github.com/nats-io/nats.rs/pull/894
* Remove unnecessary clone for publish payloads by @YaZasnyal in https://github.com/nats-io/nats.rs/pull/893
* Remove extra whitespaces by @mgrachev in https://github.com/nats-io/nats.rs/pull/904

## What's Changed
* Optimize read buffer with capacity to reduce allocations by @YaZasnyal in https://github.com/nats-io/nats.rs/pull/888
* Make SubscribeError public by @Jarema in https://github.com/nats-io/nats.rs/pull/899
* Remove a few redundant allocations by @paolobarbolini in https://github.com/nats-io/nats.rs/pull/876
* Update `base64` crate and use it in place of `base64-url` by @paolobarbolini in https://github.com/nats-io/nats.rs/pull/871
* Make `client` module public by @caspervonb in https://github.com/nats-io/nats.rs/pull/968
* Allow multiple auth methods by @n1ghtmare in https://github.com/nats-io/nats.rs/pull/937
* Update nkeys to v0.3.0 by @vados-cosmonic in https://github.com/nats-io/nats.rs/pull/995

## Misc
* Make `ClientOp` private by @caspervonb in https://github.com/nats-io/nats.rs/pull/954
* Make `Command` private by @caspervonb in https://github.com/nats-io/nats.rs/pull/953
* Disallow non-alphanumeric in `HeaderName::from_str` by @caspervonb in https://github.com/nats-io/nats.rs/pull/944
* Improve top level crate documentation by @n1ghtmare in https://github.com/nats-io/nats.rs/pull/985
* Make `ClientOp` private by @caspervonb in https://github.com/nats-io/nats.rs/pull/954
* Make `Command` private by @caspervonb in https://github.com/nats-io/nats.rs/pull/953
* Disallow new lines in `HeaderValue::from_str` by @caspervonb in https://github.com/nats-io/nats.rs/pull/943
* Replace manual Error type declarations by @paolobarbolini in https://github.com/nats-io/nats.rs/pull/1005
* Document `HeaderMap::new` by @caspervonb in https://github.com/nats-io/nats.rs/pull/981
* Reset ping interval on incoming and outgoing messages by @caspervonb in https://github.com/nats-io/nats.rs/pull/932
* Remove redundant serde `Serialize`, `Deserialize` and `default` attributes by @n1ghtmare in https://github.com/nats-io/nats.rs/pull/929
* Improve top level `jetstream` module documentation by @n1ghtmare in https://github.com/nats-io/nats.rs/pull/990
* Improve JetStream KV documentation by @n1ghtmare in https://github.com/nats-io/nats.rs/pull/991
* Bump rustls-webpki to v0.101.1 by @paolobarbolini in https://github.com/nats-io/nats.rs/pull/1013

Thank you for all your contributions!
Those make a difference and drive the ecosystem forward!

## New Contributors
* @YaZasnyal made their first contribution in https://github.com/nats-io/nats.rs/pull/894
* @MJayat made their first contribution in https://github.com/nats-io/nats.rs/pull/901
* @matthiasbeyer made their first contribution in https://github.com/nats-io/nats.rs/pull/921
* @tinou98 made their first contribution in https://github.com/nats-io/nats.rs/pull/984
* @piotrpio made their first contribution in https://github.com/nats-io/nats.rs/pull/992
* @vados-cosmonic made their first contribution in https://github.com/nats-io/nats.rs/pull/995
* @protochron made their first contribution in https://github.com/nats-io/nats.rs/pull/1000

**Full Changelog**: https://github.com/nats-io/nats.rs/compare/async-nats/v0.29.0...async-nats/v0.30.0

# 0.29.0
## Overview

This release focuses on preparing for the 1.0.0 release.

The main highlight is Core NATS concrete errors.
There are also security improvements (@paolobarbolini thanks for your help!) and JetStream API improvements.

## Concrete Errors

New errors returned by Core NATS methods are not boxed anymore.
They themselves are not enums, but follow more flexible approach of `std::io::Error` and expose `kind()` method to get the enum.
All enums implement `PartialEq` for more straightforward assertions.

## Added
* Add support for stream subject transform by @n1ghtmare in https://github.com/nats-io/nats.rs/pull/867
* Add retaining server list order option by @Jarema in https://github.com/nats-io/nats.rs/pull/890

## Fixed
* Fix a typo in the documentation by @marcusirgens in https://github.com/nats-io/nats.rs/pull/864
* Fix reconnect burst on auth failure by @Jarema in https://github.com/nats-io/nats.rs/pull/890

## Changed
* Concrete error types for Core NATS by @Jarema in https://github.com/nats-io/nats.rs/pull/632
* Improve TLS connection resilience by @Jarema in https://github.com/nats-io/nats.rs/pull/881
* Improve `batch` and `fetch` pull consumer methods by @Jarema in https://github.com/nats-io/nats.rs/pull/862
* Make `Stream` a ref in `Purge` by @n1ghtmare in https://github.com/nats-io/nats.rs/pull/877
* Cleanup dependencies by @paolobarbolini in https://github.com/nats-io/nats.rs/pull/872
* Make `Stream` a ref in `Purge` by @n1ghtmare in https://github.com/nats-io/nats.rs/pull/877
* Use `MissedTickBehavior::Skip` for flush interval by @n1ghtmare in https://github.com/nats-io/nats.rs/pull/880
* Use `MissedTickBehavior::Delay` for ping interval by @n1ghtmare in https://github.com/nats-io/nats.rs/pull/885

## New Contributors
* @marcusirgens made their first contribution in https://github.com/nats-io/nats.rs/pull/864
* @paolobarbolini made their first contribution in https://github.com/nats-io/nats.rs/pull/872

Once again, thanks @abalmos & @NorbertBodziony for helping out with replicating the issues around fetch.
Also big thanks @paolobarbolini for the very detailed report and reproduction for TLS issue and @n1ghtmare for debugging Windows related issues.

Your contributions are invaluable to the NATS ecosystem.

**Full Changelog**: https://github.com/nats-io/nats.rs/compare/async-nats/v0.28.0...async-nats/v0.29.0

# 0.28.0
## Overview
This release prepares the client for 2.10.0 server release and adds some fixes and improvements.

To use the new features before the server 2.10.0 release, enable `server_2_10` feature.

## Breaking Changes
### To enable NAK with backoff, `AckKind::NAK` enum variant was changed

What was before: 
```rust
AckKind::Nak
``` 

now is:
```rust
AckKind::Nak(Option<std::time::Duration>)
```
Which means a standard NAK example
```rust
let message = messages.next().await?;
message.ack_kind(AckKind::Nak).await?;
```
Now should be used like this
```rust
let message = messages.next().await?;
message.ack_kind(AckKind::Nak(None)).await?;
```
or with custom NAK
```rust
message.ack_kind(AckKind::Nak(Some(std::time::Duration::from_secs(10))).await?;
```
### Consumer info cluster field is now `Option`
This field is provided only in clustered mode, so it's now properly inline with that.

### Consumer `idle heartbeat` error does not terminate iterator
This change does not require any action from the users who want to continue using client
as they do now, but those who would like to try continuing working with consumer,
even if it returned `idle heartbeats`, now they can.

## Added
* Add support for consumers with multiple filters (feature `server_2_10`) by @Jarema in https://github.com/nats-io/nats.rs/pull/814
* Add metadata support (feature `server_2_10`) by @Jarema in https://github.com/nats-io/nats.rs/pull/837
* Add NAK and backoff support by @Jarema in https://github.com/nats-io/nats.rs/pull/839

## Fixed
* Fix flapping ack test by @Jarema in https://github.com/nats-io/nats.rs/pull/842
* Fix Pull Fetch test by @Jarema in https://github.com/nats-io/nats.rs/pull/845
* Update consumer last_seen on status messages and requests by @Jarema in https://github.com/nats-io/nats.rs/pull/856
* Improve pull consumer robustness by @Jarema in https://github.com/nats-io/nats.rs/pull/858

## Changed
* Make consumer info cluster field optional by @n1ghtmare in https://github.com/nats-io/nats.rs/pull/840
* Bump dependencies by @Jarema in https://github.com/nats-io/nats.rs/pull/848
* Idle Heartbeats now does not fuse the consumer iterator by @Jarema in https://github.com/nats-io/nats.rs/pull/856

**Full Changelog**: https://github.com/nats-io/nats.rs/compare/nats/v0.24.0...async-nats/v0.28.0

# 0.27.1
## Overview
A patch release focused solely on important fixes.

## Fixed
* Fix no error when auth is not provided but required by @Jarema in https://github.com/nats-io/nats.rs/pull/822
* Fix flush after reconnecting by @Jarema in https://github.com/nats-io/nats.rs/pull/823
* Fix duplicate consumer creation by @thomastaylor312 in https://github.com/nats-io/nats.rs/pull/824


**Full Changelog**: https://github.com/nats-io/nats.rs/compare/async-nats/v0.27.0...async-nats/v0.27.1

# 0.27.0
## Overview

The main focus of this release is Service API with support for multiple endpoints.

## Added
* Add multiple endpoints service by @Jarema in https://github.com/nats-io/nats.rs/pull/791
* Add `Vec` to `ToServerAddrs` impl and improve docs examples by @Jarema in https://github.com/nats-io/nats.rs/pull/802
* Add `ignore_discovered_servers` connect option by @caspervonb in https://github.com/nats-io/nats.rs/pull/809

## Changed
* Remove unsafe usages in async-nats by @zaynetro in https://github.com/nats-io/nats.rs/pull/813
* Explicitly delete consumer after iterating over kv keys by @Jarema in https://github.com/nats-io/nats.rs/pull/818

## Fixed
* Fix key listing for async client to match Go client by @thomastaylor312 in https://github.com/nats-io/nats.rs/pull/792


## New Contributors
* @thomastaylor312 made their first contribution in https://github.com/nats-io/nats.rs/pull/792
* @zaynetro made their first contribution in https://github.com/nats-io/nats.rs/pull/813

**Full Changelog**: https://github.com/nats-io/nats.rs/compare/async-nats/v0.26.0...async-nats/v0.27.0

# 0.26.0
## Overview
This release introduces improvements around TLS handling which could cause issues with Windows systems, plus some other fixes and improvements.

## Added
* Add streams list by @Jarema in https://github.com/nats-io/nats.rs/pull/785
* Add stream names list by @Jarema in https://github.com/nats-io/nats.rs/pull/783
* Add type to service responses by @Jarema in https://github.com/nats-io/nats.rs/pull/786
* Add kv example by @Jarema in https://github.com/nats-io/nats.rs/pull/784

# Fixed
* Fix queue push consumer by @Jarema in https://github.com/nats-io/nats.rs/pull/793
* Change TLS to conditionally load native certs (reported by @ronz-sensible) by @Jarema in https://github.com/nats-io/nats.rs/pull/794
* Setup tls only if required (repored by @ronz-sensible) by @Jarema in https://github.com/nats-io/nats.rs/pull/788

## Changed
* Deprecate `Stream::purge_filter` by @caspervonb in https://github.com/nats-io/nats.rs/pull/755
* Improve defaults for connection by @Jarema in https://github.com/nats-io/nats.rs/pull/790

Thank you @ronz-sensible for helping with TLS on Windows!

**Full Changelog**: https://github.com/nats-io/nats.rs/compare/async-nats/v0.25.1...async-nats/v0.26.0

# 0.25.1
## Overview

A hotfix release, changing `consumer::Info.cluster` to not break serde when `cluster` is not present (single server mode).

## Fixed
* Fix cluster field deserialization for consumer info by @Jarema in https://github.com/nats-io/nats.rs/pull/779


**Full Changelog**: https://github.com/nats-io/nats.rs/compare/async-nats/v0.25.0...async-nats/v0.25.1

# 0.25.0
## Overview

This release focuses on `service` module, which leverages NATS primitives to provide API for creating and running horizontaly scalable microservices.

## Added
* Add Service API by @Jarema in https://github.com/nats-io/nats.rs/pull/748
* Ordered and convenient HeaderValue reported by @cortopy, implemented by @Jarema in https://github.com/nats-io/nats.rs/pull/767
## Changed
* Always reset periodic flush interval after manual flush by @caspervonb in https://github.com/nats-io/nats.rs/pull/747
* Fuse the pull consumer Stream after terminal error by @Jarema in https://github.com/nats-io/nats.rs/pull/751
* Remove auth_required comment by @Jarema in https://github.com/nats-io/nats.rs/pull/763
* Change JetStream request timeout to 5 seconds by @Jarema in https://github.com/nats-io/nats.rs/pull/772

**Full Changelog**: https://github.com/nats-io/nats.rs/compare/async-nats/v0.24.0...async-nats/v0.25.0

# 0.24.0
## Overview
This a minor release intended to release all changes before the long-awaited changes around concrete errors land.

## What's Changed
* Fix various spelling mistakes by @c0d3x42 in https://github.com/nats-io/nats.rs/pull/735
* Add spellcheck by @Jarema in https://github.com/nats-io/nats.rs/pull/736
* Reset flush interval after ping forces flush by @caspervonb in https://github.com/nats-io/nats.rs/pull/737
* Add extended purge by @Jarema in https://github.com/nats-io/nats.rs/pull/739


**Full Changelog**: https://github.com/nats-io/nats.rs/compare/async-nats/v0.23.0...async-nats/v0.24.0

# 0.23.0
## Overview

This release focuses on fixes around Object Store and customized JetStream Publish.

It also introduces a breaking change, as not all `publish()` methods did return `PublishError`, using the generic `async_nats::Error` instead. This has been fixed.

## Breaking changes
* Make publish error types consistent by @Jarema in https://github.com/nats-io/nats.rs/pull/727

## Fixed
* Fix object store watch to retrieve only new/changed values by @Jarema in https://github.com/nats-io/nats.rs/pull/720
* Fix stack overflow in object store by @Jarema in https://github.com/nats-io/nats.rs/pull/731

## Added
* Add customizable JetStream publish by @Jarema in https://github.com/nats-io/nats.rs/pull/728 request by @andrenth
* Add object store list by @Jarema in https://github.com/nats-io/nats.rs/pull/721
* Add docs lint by @Jarema in https://github.com/nats-io/nats.rs/pull/725

# Changed
* Use debug macro for logging instead of println by @c0d3x42 in https://github.com/nats-io/nats.rs/pull/716
* Merge periodic flush into connection handler loop by @caspervonb in https://github.com/nats-io/nats.rs/pull/687
* Improve docs formatting and fix links by @Jarema in https://github.com/nats-io/nats.rs/pull/723


## New Contributors
* @c0d3x42 made their first contribution in https://github.com/nats-io/nats.rs/pull/716
* @piotrpio made their first contribution in https://github.com/nats-io/nats.rs/pull/728

**Full Changelog**: https://github.com/nats-io/nats.rs/compare/async-nats/v0.22.1...async-nats/v0.23.0

# 0.22.1
## Overview
A patch release, including early feedback to 0.22.0.

## Breaking Changes

Unfortunately, last release included a typo in `retry_on_initial_release`.
This patch fixes it.
We decided to make a normal patch release without yanking as we're pre 1.0.0 verison.
To avoid similar situations in the future, spellecheck lint will be added before next release.

## Changed
* Flush on publish ack await by @Jarema in https://github.com/nats-io/nats.rs/pull/708
## Fixed
* Fix typo in retry_on_initial_connect by @Jarema in https://github.com/nats-io/nats.rs/pull/713
## Added
* Setup cargo deny to check licenses by @corbinu in https://github.com/nats-io/nats.rs/pull/703

# 0.22.0
## Overview
This release introduces a number of changes and two breaking changes:

### JetStream publish
Used to return ack:
```rust
let ack = jetstream.publish().await.unwrap();
```
But while adding publish that does not wait for acknowledgment before returning,
we realized that we can leverage [IntoFuture](https://doc.rust-lang.org/std/future/trait.IntoFuture.html), so the new API is:
```rust
// This publishes the message, but do not wait until ack is received.
let future_ack = jetstream.publish().await.unwrap();
// to receive the acknowledge, `await()` the returned value:
let ack = future_ack.await().unwrap();
```
### Event logging
After adding initial retry on connect option, `Event::Reconnect` didn't make much sense.
Hence it was renamed to `Event::Connected`, which describes the current state without implications about the previous one.
For consistency, `Event::Disconnect` was renamed to `Event::Disconnected`.

## Breaking changes
* Defer publish acknowledgments by @Jarema in https://github.com/nats-io/nats.rs/pull/644
* Add retry on initial connect by @Jarema in https://github.com/nats-io/nats.rs/pull/662
## Added
* Add support for mirrors and sources in Key Value Store by @Jarema in https://github.com/nats-io/nats.rs/pull/676
* Add sources and mirror to stream config by @Jarema in https://github.com/nats-io/nats.rs/pull/673
* Add docs to client methods by @Jarema in https://github.com/nats-io/nats.rs/pull/664
* Add license to nats-server wrapper by @Jarema in https://github.com/nats-io/nats.rs/pull/704
## Fixed
* Fix object store bucket names by @Jarema in https://github.com/nats-io/nats.rs/pull/697
* Fix event connected display by @Jarema in https://github.com/nats-io/nats.rs/pull/669
* Fix missing hearbeats future wakup by @Jarema in https://github.com/nats-io/nats.rs/pull/671
* Fix pull consumer without hearbeats by @Jarema in https://github.com/nats-io/nats.rs/pull/667
* Fix typos in async-nats docs by @jdon in https://github.com/nats-io/nats.rs/pull/663
* Fix clippy errors by @caspervonb in https://github.com/nats-io/nats.rs/pull/690
* Fix broken clippy pipeline and errors by @caspervonb in https://github.com/nats-io/nats.rs/pull/695
## Changed
* Improve event logging by @Jarema in https://github.com/nats-io/nats.rs/pull/672
* Bump rustls-pemfile version by @paulgb in https://github.com/nats-io/nats.rs/pull/666
* Use get direct in KV by @Jarema in https://github.com/nats-io/nats.rs/pull/651
* Ordered consumer recreate & use mem_storage R1 by @Jarema in https://github.com/nats-io/nats.rs/pull/654
* Change event logs to info level by @Jarema in https://github.com/nats-io/nats.rs/pull/677
* Remove unused dependency on tokio-util by @caspervonb in https://github.com/nats-io/nats.rs/pull/679
* Merge periodic ping into connection handler loop by @caspervonb in https://github.com/nats-io/nats.rs/pull/681
* Update K/V purge documentation verbiage by @corbinu in https://github.com/nats-io/nats.rs/pull/688
* Make pull consumer having exact pending numbers by @Jarema in https://github.com/nats-io/nats.rs/pull/668
* Improve object get example by @Jarema in https://github.com/nats-io/nats.rs/pull/706

## New Contributors
* @jdon made their first contribution in https://github.com/nats-io/nats.rs/pull/663
* @corbinu made their first contribution in https://github.com/nats-io/nats.rs/pull/688

**Full Changelog**: https://github.com/nats-io/nats.rs/compare/async-nats/v0.21.0...async-nats/v0.22.0

# 0.21.0
## Overview

This release's highlight is added support for Object Store.

## Breaking changes
* Add last_active to SequenceInfo and rename SequencePair to SequenceInfo by @Jarema in https://github.com/nats-io/nats.rs/pull/657

## Added
* Add Object Store by @Jarema in https://github.com/nats-io/nats.rs/pull/655
* Add discard_new_per_subject to Stream by @Jarema in https://github.com/nats-io/nats.rs/pull/656
* Add republish to KV by @Jarema in https://github.com/nats-io/nats.rs/pull/653
* Add name option by @Jarema in https://github.com/nats-io/nats.rs/pull/659

## Fixed
* Fix empty keys deadlock by @Jarema in https://github.com/nats-io/nats.rs/pull/641
* Fix lint error by @Jarema in https://github.com/nats-io/nats.rs/pull/645
* Fix benchmark in CI by @Jarema in https://github.com/nats-io/nats.rs/pull/647
* Fix potential pending pings mismatch on reconnects by @Jarema in https://github.com/nats-io/nats.rs/pull/650
* Fix typo (eror -> error) by @paulgb in https://github.com/nats-io/nats.rs/pull/652
* Remove println by @Jarema in https://github.com/nats-io/nats.rs/pull/658



**Full Changelog**: https://github.com/nats-io/nats.rs/compare/async-nats/v0.20.0...async-nats/v0.21.0

# 0.20.0
## Overview

This release focuses on KV and 2.9 nats-server features.

## Added
* Add Key Value by @Jarema and @caspervonb in https://github.com/nats-io/nats.rs/pull/586
* Add Direct get by @Jarema in https://github.com/nats-io/nats.rs/pull/636
* Add timeout to request & request builder by @Jarema in https://github.com/nats-io/nats.rs/pull/616
* Add memory storage option to consumers by @Jarema in https://github.com/nats-io/nats.rs/pull/638
* Add Consumer name by @Jarema in https://github.com/nats-io/nats.rs/pull/637

## Fixed
* Fix heartbeat typo by @Jarema in https://github.com/nats-io/nats.rs/pull/630

## What's Changed
* Headers refactor by @Jarema in https://github.com/nats-io/nats.rs/pull/629
* Use new Consumer API by @Jarema in https://github.com/nats-io/nats.rs/pull/637

**Full Changelog**: https://github.com/nats-io/nats.rs/compare/async-nats/v0.19.0...async-nats/v0.20.0
# 0.19.0
## Overview

This release is focused on resilience of the client against network issues.

It also adds some utility methods, Stream Republish and improvements for Pull Consumers.

## Added
* Add server info by @Jarema in https://github.com/nats-io/nats.rs/pull/600
* Add server compatibility check function by @Jarema in https://github.com/nats-io/nats.rs/pull/603
* Add stream info and cached_info by @Jarema in https://github.com/nats-io/nats.rs/pull/599
* Add custom request prefix option by @Jarema in https://github.com/nats-io/nats.rs/pull/604
* Add connection timeout by @thed0ct0r in https://github.com/nats-io/nats.rs/pull/607
* Add stream republish by @Jarema in https://github.com/nats-io/nats.rs/pull/613
* Add timeout to double_ack by @Jarema in https://github.com/nats-io/nats.rs/pull/617
* Add internal connection state watcher by @Jarema in https://github.com/nats-io/nats.rs/pull/606
* Add miss Pull Consumer heartbeats error by @Jarema in https://github.com/nats-io/nats.rs/pull/627
* Add purge subject by @Jarema in https://github.com/nats-io/nats.rs/pull/620

## Fixed
* Fix jetstream reconnect by @Jarema in https://github.com/nats-io/nats.rs/pull/610
* Fix typos in readme's by @insmo in https://github.com/nats-io/nats.rs/pull/618
* Fix voldemort error by @Jarema in https://github.com/nats-io/nats.rs/pull/626
* Jarema/fix pull consumer deadlock by @Jarema in https://github.com/nats-io/nats.rs/pull/619

*Thanks to all contributors for helping out, taking part in discussion and detailed issue reports!*


## New Contributors
* @thed0ct0r made their first contribution in https://github.com/nats-io/nats.rs/pull/607
* @insmo made their first contribution in https://github.com/nats-io/nats.rs/pull/618

**Full Changelog**: https://github.com/nats-io/nats.rs/compare/async-nats/v0.18.0...async-nats/v0.19.0

# 0.18.0
## Overview
This release focuses on fixes and improvements, with addition of ordered Push Consumer.

## Breaking Changes
* Refactor callbacks by @Jarema in https://github.com/nats-io/nats.rs/pull/595

## Added
* Add `get_last_raw_message_by_subject` to `Stream` by @caspervonb in https://github.com/nats-io/nats.rs/pull/584
* Add `ClusterInfo` and `PeerInfo` by @Jarema in https://github.com/nats-io/nats.rs/pull/572
* Add ordered  push consumer by @Jarema in https://github.com/nats-io/nats.rs/pull/574
* Add concurrent example by @Jarema in https://github.com/nats-io/nats.rs/pull/580
* Add delete message from stream by @Jarema in https://github.com/nats-io/nats.rs/pull/588
* Add Nkey authorization support by @neogenie  https://github.com/nats-io/nats.rs/pull/593

## Fixed
* Fix ordered consumer after discard policy hit by @Jarema in https://github.com/nats-io/nats.rs/pull/585
* Fix pull consumer stream method when batch is set to 1 by @Jarema in https://github.com/nats-io/nats.rs/pull/590
* Fix reconnect auth deadlock by @caspervonb in https://github.com/nats-io/nats.rs/pull/578

# 0.17.0
## Overview
This release focuses on two main things:
* Refactor of JetStream API
* Fix of slow connect (thanks @brooksmtownsend for reporting this!)

The changes in JetStream API make usage of builder more intuitive and seamless.
Before, you had to call
```rust
// before changes
let messages = consumer.stream().await?;
// or use a shortcut
let messages = consumer.messages().await?;

// after changes
let messages = consumer.stream().messages().await?;
// or with options
let messages = consumer.stream().max_bytes_per_bytes(1024).messages().await?;
```

## Changed
* Rename push consumer `Stream` iterator to `Messages` by @caspervonb in https://github.com/nats-io/nats.rs/pull/566
* Add pull builder for Fetch and Batch by @Jarema in https://github.com/nats-io/nats.rs/pull/565

## Fixed
* Fix slow connect in no-auth scenarios by @Jarema in https://github.com/nats-io/nats.rs/pull/568

## Other
* Fix license headers by @Jarema in https://github.com/nats-io/nats.rs/pull/564
* Add missing module docs headers by @Jarema in https://github.com/nats-io/nats.rs/pull/563
* Remove fault injection run from workflow by @caspervonb in https://github.com/nats-io/nats.rs/pull/567


**Full Changelog**: https://github.com/nats-io/nats.rs/compare/async-nats/v0.16.0...async-nats/v0.17.0

# 0.16.0

This release features a lot of improvements and additions to `JetStream` API and adds `Push Consumer`.

## Added
* Add `query_account` to `jetstream::Context` by @caspervonb in https://github.com/nats-io/nats.rs/pull/528
* Add streams to push consumers by @caspervonb in https://github.com/nats-io/nats.rs/pull/527
* Add no_echo option by @Jarema in https://github.com/nats-io/nats.rs/pull/560
* Add `jetstream::Stream::get_raw_message` by @caspervonb in https://github.com/nats-io/nats.rs/pull/484
* Add Pull Consumer builder by @Jarema in https://github.com/nats-io/nats.rs/pull/541

## Changed
* Allow unknown directives to be skipped when parsing by @caspervonb in https://github.com/nats-io/nats.rs/pull/514
* Narrow error type returned from client publishing by @caspervonb in https://github.com/nats-io/nats.rs/pull/525
* Change `create_consumer` to return `Consumer` by @Jarema in https://github.com/nats-io/nats.rs/pull/544
* Switch webpki to rustls-native-certs by @Jarema in https://github.com/nats-io/nats.rs/pull/558
* Normalize error type used in subscribe methods by @caspervonb in https://github.com/nats-io/nats.rs/pull/524
* Optimize `jetstream::consumer::pull::Consumer::stream` method. by @Jarema in https://github.com/nats-io/nats.rs/pull/529
* Make `deliver_subject` required for `push::Config` by @caspervonb in https://github.com/nats-io/nats.rs/pull/531

## Fixed
* Handle missing error cases in Stream by @Jarema in https://github.com/nats-io/nats.rs/pull/542
* Handle connecting to ipv6 addresses correctly by @jszwedko in https://github.com/nats-io/nats.rs/pull/386

## Other
* Move `Client` into its own source file by @caspervonb in https://github.com/nats-io/nats.rs/pull/523
* Extract `jetstream::Message` into its own module by @caspervonb in https://github.com/nats-io/nats.rs/pull/534
* Normalize introduction example by @caspervonb in https://github.com/nats-io/nats.rs/pull/540
* Fix documentation links by @Jarema in https://github.com/nats-io/nats.rs/pull/547
* Add more documentation to Pull Consumer by @Jarema in https://github.com/nats-io/nats.rs/pull/546
* Add Push Consumer stream docs by @Jarema in https://github.com/nats-io/nats.rs/pull/559
* Fix ack test race by @Jarema in https://github.com/nats-io/nats.rs/pull/555
* Add Message and Headers docs by @Jarema in https://github.com/nats-io/nats.rs/pull/548
* Remove trace and debug from nats-server wrapper by @Jarema in https://github.com/nats-io/nats.rs/pull/550

# 0.15.0

This release is the first `JetStream` 🍾  feature set for `async-nats`!

**It includes:**
* New simplified JetStream API approach
* JetStream Publish
* Streams management
* Consumers Management
* Pull Consumers implementation
* Ack's

Warning: JetStream support is experimental and may change

## Added
* Add JetStream types and basics by @Jarema in https://github.com/nats-io/nats.rs/pull/457
* Add get stream by @Jarema in https://github.com/nats-io/nats.rs/pull/458
* Add jetstream stream delete and stream update by @Jarema in https://github.com/nats-io/nats.rs/pull/459
* Add `async_nats::jetstream::Context::publish` by @caspervonb in https://github.com/nats-io/nats.rs/pull/460
* Add get_or_create JetStream management API by @Jarema in https://github.com/nats-io/nats.rs/pull/467
* Add domain and prefix by @Jarema in https://github.com/nats-io/nats.rs/pull/490
* Add error codes to `Response::Error` variant by @caspervonb in https://github.com/nats-io/nats.rs/pull/496
* Add JetStream ACK by @Jarema in https://github.com/nats-io/nats.rs/pull/515
* Add convinience methods to Consumer management by @Jarema in https://github.com/nats-io/nats.rs/pull/481
* Add Pull Consumer by @Jarema in https://github.com/nats-io/nats.rs/pull/479
* Add create consumer by @Jarema in https://github.com/nats-io/nats.rs/pull/471
* Add stream to pull consumers by @caspervonb in https://github.com/nats-io/nats.rs/pull/518
* Add Consumer::info and Consumer::cached_info by @Jarema in https://github.com/nats-io/nats.rs/pull/510
* Introduce a `StatusCode` type to represent statuses by @caspervonb in https://github.com/nats-io/nats.rs/pull/474
* Add example for multiple pub/subs in tasks by @Jarema in https://github.com/nats-io/nats.rs/pull/453
* Implement jetstream requests by @caspervonb in https://github.com/nats-io/nats.rs/pull/435
* Add `async_nats::jetstream::Context::publish_with_headers` by @caspervonb in https://github.com/nats-io/nats.rs/pull/462
* Implement `From<jetstream::Message>` for `Message` by @caspervonb in https://github.com/nats-io/nats.rs/pull/512
* Add get_or_create_consumer and delete_consumer by @Jarema in https://github.com/nats-io/nats.rs/pull/475
* Have No async-stream dependant implementation for Pull Consumers  by @caspervonb in https://github.com/nats-io/nats.rs/pull/499

## Changed
* Do not flush in write calls by @caspervonb in https://github.com/nats-io/nats.rs/pull/423
* Only retain non-closed subscriptions on reconnect by @caspervonb in https://github.com/nats-io/nats.rs/pull/454

## Fixed
* Fix off by one error that can occur parsing "HMSG" by @caspervonb in https://github.com/nats-io/nats.rs/pull/513
* Removed attempt to connect to server info host when TLS is enabled by @brooksmtownsend in https://github.com/nats-io/nats.rs/pull/500

# 0.14.0
## Added
* Add no responders handling by @Jarema in https://github.com/nats-io/nats.rs/pull/450
* Add client jwt authentication by @stevelr  in https://github.com/nats-io/nats.rs/pull/433
* Add lame duck mode support by @Jarema in https://github.com/nats-io/nats.rs/pull/438
* Add slow consumers support by @Jarema in https://github.com/nats-io/nats.rs/pull/444
* Add tracking maximum number of pending pings by @caspervonb  https://github.com/nats-io/nats.rs/pull/419

## Changed
* `Client` doesn't need to be mutable self by @stevelr  in https://github.com/nats-io/nats.rs/pull/434
* Make send buffer configurable by @Jarema in https://github.com/nats-io/nats.rs/pull/437

# 0.13.0
## Added
* Add Auth - username/password & token by @Jarema in https://github.com/nats-io/nats.rs/pull/408
* Support sending and receiving messages with headers by @caspervonb in https://github.com/nats-io/nats.rs/pull/402 
* Add async server errors callbacks by @Jarema in https://github.com/nats-io/nats.rs/pull/397
* Discover additional servers via INFO by @caspervonb in https://github.com/nats-io/nats.rs/pull/403
* Resolve socket addresses during connect by @caspervonb in https://github.com/nats-io/nats.rs/pull/403

## Changed
* Wait between reconnection attempts by @caspervonb in https://github.com/nats-io/nats.rs/pull/407
* Limit connection attempts by @caspervonb in https://github.com/nats-io/nats.rs/pull/400

## Other
* Remove redundant doctests by @Jarema in https://github.com/nats-io/nats.rs/pull/412
* Fix connection callback tests by @Jarema in https://github.com/nats-io/nats.rs/pull/420

# 0.12.0
## Added
* Add more examples and docs by @Jarema in https://github.com/nats-io/nats.rs/pull/372
* Add unsubscribe by @Jarema in https://github.com/nats-io/nats.rs/pull/363
* Add unsubscribe_after by @Jarema in https://github.com/nats-io/nats.rs/pull/385
* Add queue subscriber and unit test by @stevelr in https://github.com/nats-io/nats.rs/pull/388
* Implement reconnect by @caspervonb in https://github.com/nats-io/nats.rs/pull/382

## Other
* Fix test linter warnings by @caspervonb in https://github.com/nats-io/nats.rs/pull/379
* Fix tests failing with nats-server 2.8.0 by @Jarema in https://github.com/nats-io/nats.rs/pull/380
* Use local server for documentation tests by @caspervonb in https://github.com/nats-io/nats.rs/pull/377
* Improve workflow caching by @caspervonb in https://github.com/nats-io/nats.rs/pull/381
* Fix typo in README.md by @mgrachev in https://github.com/nats-io/nats.rs/pull/384
* Internal Architecture overhaul by @caspervonb and @Jarema 

# 0.11.0
Initial release of async NATS client rewrite.
The versioning starts from v0.11.0, as the Crate was used a long time ago by NATS.io org for some former work around async client.
