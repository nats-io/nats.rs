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
