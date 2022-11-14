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
* Defer publish acknowledgements by @Jarema in https://github.com/nats-io/nats.rs/pull/644
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
* Add Key Value by @Jarema and @caspervonb  in https://github.com/nats-io/nats.rs/pull/586
* Add Direct get by @Jarema in https://github.com/nats-io/nats.rs/pull/636
* Add timeout to request & request builder by @Jarema  in https://github.com/nats-io/nats.rs/pull/616
* Add memory storage option to consumers by @Jarema in https://github.com/nats-io/nats.rs/pull/638
* Add Consumer name by @Jarema in https://github.com/nats-io/nats.rs/pull/637

## Fixed
* Fix heartbeat typo by @Jarema in https://github.com/nats-io/nats.rs/pull/630

## What's Changed
* Headers refactor by @Jarema in https://github.com/nats-io/nats.rs/pull/629
* Use new Consumer API  by @Jarema in https://github.com/nats-io/nats.rs/pull/637

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
