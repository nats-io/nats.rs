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
