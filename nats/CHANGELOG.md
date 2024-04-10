# 0.25.0

## Overview

A small release that allows client compile on MIPS and PowerPC architectures.
It also adds the legacy notice and examples how to use async client in sync environments.

## What's Changed
* Add sync client legacy notice
* Updates Message Metadata docs
* Allow client to compile on MIPS and PowerPC by @protochron in https://github.com/nats-io/nats.rs/pull/1210
* Update NKEYS to v0.4.1

Thank you @protochron for your contribution!

# 0.24.1
## Overview

This is a patch release for all pending changes and backports for legacy sync nats client, especially for TLS security fix.

## What's Changed
* Fix sync TLS authentication for EC keys by @sp-angel  https://github.com/nats-io/nats.rs/pull/1128
* Update TLS dependencies for legacy client and fix vulnerability by @n1ghtmare in https://github.com/nats-io/nats.rs/pull/982
* Update nkeys to v0.3.0 by @vados-cosmonic in https://github.com/nats-io/nats.rs/pull/995

# 0.24.0
## Added
* Add timeout to JetStream requests by @j13tw in https://github.com/nats-io/nats.rs/pull/771

## Fixed
* Send PUB instead of HPUB if headers are set but empty by @Jarema in https://github.com/nats-io/nats.rs/pull/833

## Misc
*  Allow parse message data as json in nats-box example by @xoac in https://github.com/nats-io/nats.rs/pull/816

**Full Changelog**: https://github.com/nats-io/nats.rs/compare/nats/v0.23.2...nats/v0.23.1

# 0.23.1
## Overview
This is a minor sync client release with fixes to Object Store API

## Fixed
* Fix sync object store encoding by @Jarema in https://github.com/nats-io/nats.rs/pull/700
## Added
* Add digest to sync object store by @Jarema in https://github.com/nats-io/nats.rs/pull/709

# 0.23.0
## Overview
This is a minor release with one small addition.

## Added
* Add cluster to streaminfo/consumerinfo struct by @j13tw in https://github.com/nats-io/nats.rs/pull/537

# 0.22.0
## Overview
This is a minor release for the `nats` client with one fix and several documentation improvements.

## What's changed
* Handle connecting to ipv6 addresses correctly by @jszwedko in https://github.com/nats-io/nats.rs/pull/386
* Use correct flush_timeout operation in doc test by @krady21 in https://github.com/nats-io/nats.rs/pull/556
* Fix typo in jetstream/pull_subscription.rs by @bbigras in https://github.com/nats-io/nats.rs/pull/521

# 0.21.0
## Added
* Allow tokens in connection strings and add tests by @paulgb in https://github.com/nats-io/nats.rs/pull/506

# 0.20.1
## Overview
A patch release fixing stack overflow happening when there was kv watcher for a key that did not change for a long time.

## Fixed
* Remove recursive calls by @segfaultdoc in https://github.com/nats-io/nats.rs/pull/448

# 0.20.0
## Overview
A minor sync client release that adds requests with timeouts and headers.

## Added
* Add request with headers and timeouts by @Jarema in https://github.com/nats-io/nats.rs/pull/431

# 0.19.1
## Fixed
* Fix KV issues when JS domain is set by @Jarema in https://github.com/nats-io/nats.rs/pull/387

# 0.19.0
## Overview
Minor release with some fixes and improvements and repo restructuring.

## Changed
* Bump nuid dependency to `0.3.1` by @mfelsche in https://github.com/nats-io/nats.rs/pull/325
* Adapt discard policy according to server version by @Jarema in https://github.com/nats-io/nats.rs/pull/327
* Link to asynk module instead of async-nats by @aditsachde in https://github.com/nats-io/nats.rs/pull/331
* Remove `Features` section from Readme.md by @Jarema in https://github.com/nats-io/nats.rs/pull/335
* upgrade itoa, nkeys, parking_lot, regex by @attila-lin in https://github.com/nats-io/nats.rs/pull/330

## Fixed
* Fix a couple of minor things by @caspervonb in https://github.com/nats-io/nats.rs/pull/338
* Fix clippy warning by @caspervonb in https://github.com/nats-io/nats.rs/pull/361

# 0.18.1
## Overview
A patch release fixing long durations until `close()` returned. It was introduced in the graceful shutdown of client connection threads in v0.18.0.

Dropping NATS connection was not affected.

## Fixed
* Fix slow connection closing by @Jarema in https://github.com/nats-io/nats.rs/pull/319
* Fix close() hang after js.subscribe() is called by @Jarema https://github.com/nats-io/nats.rs/pull/321
* Fix close() hang after Push Consumer subsription edge case by @Jarema https://github.com/nats-io/nats.rs/pull/323

## Minor
* Replace custom Into trait with From by @Jarema in https://github.com/nats-io/nats.rs/pull/315

**Full Changelog**: https://github.com/nats-io/nats.rs/compare/v0.18.0...v0.18.1

# 0.18.0
## Overview
This release focuses mainly around fixes of large changes introduces in 0.17.0, but also improves adds slow consumers support
and improves header map interface.

## Breaking Change
* Add a public convenience interface to header maps by @caspervonb in https://github.com/nats-io/nats.rs/pull/310

## Added
* Add slow consumers support by @Jarema in https://github.com/nats-io/nats.rs/pull/299

## Changed
* Replace `chrono` with `time` due to RUSTSEC-2020-0159 by @ShellWowza in https://github.com/nats-io/nats.rs/pull/309
* Join all connect threads while closing connection by @Jarema in https://github.com/nats-io/nats.rs/pull/305

## Fixed
* Test server subject matching bug fix by @fdlg in https://github.com/nats-io/nats.rs/pull/221
* Fix Pull Subscriber visibility by @Jarema in https://github.com/nats-io/nats.rs/pull/313
* Fix missing export subscription modules by @caspervonb in https://github.com/nats-io/nats.rs/pull/314
* Check sequence mismatch only for Ordered Consumer by @Jarema in https://github.com/nats-io/nats.rs/pull/317

## New Contributors
* @ShellWowza made their first contribution in https://github.com/nats-io/nats.rs/pull/309
* @fdlg made their first contribution in https://github.com/nats-io/nats.rs/pull/221

**Full Changelog**: https://github.com/nats-io/nats.rs/compare/v0.17.0...v0.17.1

# 0.17.0
## Overview

This release brings a lot of changes and refactors among which the highlights are:
* A complete rewrite of the JetStream API with a new subscription interface
* Improvements of JetStream internals
* Key-Value Store support
* Object Store support

## Breaking Changes
* Introduce a `JetStream` type by @caspervonb in https://github.com/nats-io/nats.rs/pull/247
* Move Consumer Management to JetStream by @Jarema in https://github.com/nats-io/nats.rs/pull/250
* Re-work JetStream push consumer interface by @caspervonb in https://github.com/nats-io/nats.rs/pull/252
* Re-work JetStream pull consumer interface by @Jarema in https://github.com/nats-io/nats.rs/pull/302
* Rename create_stream to add_stream by @Jarema in https://github.com/nats-io/nats.rs/pull/251
* Change return type of add_consumer to `ConsumerInfo` by @caspervonb in https://github.com/nats-io/nats.rs/pull/252

## Added
* Add header module by @caspervonb in https://github.com/nats-io/nats.rs/pull/260
* Implement key-value store by @caspervonb in https://github.com/nats-io/nats.rs/pull/267
* Implement object store by @caspervonb in https://github.com/nats-io/nats.rs/pull/269
* Lame duck mode support by @Jarema in https://github.com/nats-io/nats.rs/pull/265
* Add domain field to `PubAck` by @caspervonb in https://github.com/nats-io/nats.rs/pull/243
* Add support for JetStream publishing by @caspervonb in https://github.com/nats-io/nats.rs/pull/248
* Add `error_callback` by @derekcollison in https://github.com/nats-io/nats.rs/pull/253
* Add `get_message` to JetStream context by @caspervonb in https://github.com/nats-io/nats.rs/pull/267
* Add `get_last_message` to JetStream context by @caspervonb in https://github.com/nats-io/nats.rs/pull/267
* Add option `retry_on_failed_connect` by @pozsgaic in https://github.com/nats-io/nats.rs/pull/223
* Add support for different .pem contents by @Jarema https://github.com/nats-io/nats.rs/pull/280
* Introduce ServerAddress https://github.com/nats-io/nats.rs/pull/276

## Changed
* Allow for inline header description with spaces by @caspervonb in https://github.com/nats-io/nats.rs/pull/241
* Allow setting a jetstream api prefix from a domain by @caspervonb in https://github.com/nats-io/nats.rs/pull/244
* Have Client in Message as Option by @Jarema in https://github.com/nats-io/nats.rs/pull/258
* Change jetstream log level to debug by @caspervonb https://github.com/nats-io/nats.rs/pull/307
* Bump MSRV to 1.53.0

## Minor
* Reduce allocations in `Headers::try_from` by @caspervonb in https://github.com/nats-io/nats.rs/pull/238
* Improve error handling by @caspervonb in https://github.com/nats-io/nats.rs/pull/249
* Some additions.. by @derekcollison in https://github.com/nats-io/nats.rs/pull/253
* Bump blocking crate by @Jarema in https://github.com/nats-io/nats.rs/pull/255
* Add `sealed` field to `jetstream::StreamConfig` by @caspervonb in https://github.com/nats-io/nats.rs/pull/256
* Fix unsubscribe method behaviour to act as in docs by @Jarema in https://github.com/nats-io/nats.rs/pull/301
* Fix license formatting so it is properly detected by Github and add missing license banners in files by @Jarema in https://github.com/nats-io/nats.rs/pull/292

**Full Changelog**: https://github.com/nats-io/nats.rs/compare/v0.16.0...v0.17.0

# 0.16.0

### Added
- Added support for header status and no responders to requests (#219)
- Added support for username and password as part of the url (#216)
- Added support for client side periodic pings

### Changed
- Bumped MSRV to 1.52.0

### Removed
- Use of IntervalTree for client side de-dupe for JetStream consumers
- Serialized used of test servers, tests can now run in parallel

### Improved
- Better handling of write errors and server disconnects as part of client side ping support
- Better handling of message headers

### Complete Changes

https://github.com/nats-io/nats.rs/compare/v0.15.2...v0.16.0

# 0.15.2

# Bug Fixes

- #217 Fix thread leak when `asynk::Subscription`
  is dropped without explicitly `unsubscribe` first.

# 0.15.1

# Improvements

- Minor debug statement removed that was present in
  previous release.

# 0.15.0

## Breaking Changes

- #207 Support ADR-15-style JetStream reply subject
  parsing, with several new fields added to the
  `JetstreamMessageInfo` struct that can be
  parsed from a message.

# 0.14.0

## Breaking Changes

- #211 `jetstream::ConsumerConfig.ack_wait` and
  `jetstream::PubOpts.ttl` have been changed from
  `isize` to `i64`.

# 0.13.1

## New Features

- #199 implemented `asynk::Subscription::try_next`.
- #199 implemented conversion traits b/w sync & async
  Message types.
- #205 `Options::tls_client_config` allows users to
  provide a manually-configured `rustls::ClientConfig`
  for communicating to the server, for cases where
  certificates are not available on the filesystem.

# 0.13.0

## Improvements

- #197 JetStream configuration objects now implement
  PartialEq and Eq.

## Breaking Changes

- #197 Some JetStream configuration objects have been
  simplified while more closely matching the Golang
  JS client semantics.

# 0.12.1

## Improvements

- #196 minor documentation fix.

# 0.12.0

## Breaking Changes

- #195 JetStream support is now available without
  any feature set, and the jetstream feature has
  been removed.

# 0.11.0

## Breaking Changes

- #192 the async_nats crate has been merged into the
  main nats crate, under the `asynk` module name.
- #192 the `jetstream::IntervalTree` type has been made
  private in anticipation of built-in FIFO (ordered)
  message processing functionality built on top of
  different internal structures.

# 0.10.1

## Improvements

- #190 exposed the last connected server's max_payload
  configurable with the new Connection::max_payload
  method.

# 0.10.0

## Improvements

- #189 ipv6 address parsing support has been added.

## Breaking Changes

- #183 the MSRV bumped to Rust 1.51, which was
  released on March 25 2021.

# 0.9.18

## Improvements

- #183 reset client writer to None upon disconnection

# 0.9.17

## Improvements

- #180 idempotent unsubscription avoids sending
  multiple UNSUB messages.

# 0.9.16

## Improvements

- #178 client state has been reorganized to allow
  reading and writing to make progress independently,
  preventing issues that were sometimes encountered
  where the act of creating a subscription would lead
  to a connection timing out after a slow consumer
  was detected by the server.

# 0.9.15

## New Features

- #177 Add `request_timeout` support for async-nats

# 0.9.14

## New Features

- #173 `Options::with_static_credentials` adds
  support for static credential files for use
  in environments where they are injected into
  the process by means other than an on-disk file.
- #174 `IntervalTree` now has the methods `min`,
  `max`, and `gaps` for getting the minimum,
  maximum, and non-contiguous gaps for the tracked
  intervals, for use in situations where users
  may want to implement their own deduplication
  strategies.

# 0.9.13

## Bug Fixes

- #172 fix an issue with a newly future-incompatible
  usage of the `log` crate's macros in a match arm.

# 0.9.12

## Bug Fixes

- #170 fix an off-by-one error preventing empty
  messages with headers from being received.

# 0.9.11

## Bug Fixes

- #168 properly handle headers whose value contains
  colon characters.

# 0.9.10

## Improvements

- #162 `Subscription::receiver` has been added to
  provide access to a `Subscription`'s underlying
  `crossbeam_challel::Receiver` for use in `select!`
  blocks etc...

# 0.9.9

## Bug Fixes

- #164 `Consumer::process_timeout` properly times out
  from all branches.
- #164 `Consumer::pull_opt` now properly checks the
  `Consumer.deliver_subject`.

# 0.9.8

## Bug Fixes

- #161 When attempting to send a message with headers
  to a server that does not support headers, an error
  is now properly returned instead of silently dropping
  the message.

# 0.9.7

## Improvements

- Improved error log output and avoid panicking
  when problems are encountered while parsing the
  JetStream reply subject.

# 0.9.6

## New Features

- JetStream consumers are now better supported
  through the `Consumer::process*` methods,
  which also perform message deduplication
  backed by an interval tree.

# 0.9.5

## New Features

- JetStream consumer and message acknowledgement is now
  supported via the `jetstream` feature.

# 0.9.4

## Bug Fixes

- #150 fixed a bug with no_echo.

# 0.9.3

## Improvements

- #141 use TCP_NODELAY on the connection to the server.
- #144 support multiple clients in the test server.

# 0.9.2

## Bug Fixes

- #143 Fix a bug preventing nkey authentication.

## Improvements

- #140 Optimize NATS protocol parsing.

# 0.9.1

## Bug Fixes

- #136 Workaround: skip native certificates that can't be loaded.

# 0.9.0

## Breaking Changes

- #130, #132 the async client has been split into its own crate, async-nats

# 0.8.6

## Bug Fixes

- #126 Fix port signedness issue on ServerInfo which
  prevented connecting to servers on ports over i16::MAX.

# 0.8.5

## Improvements

- #125 Remove Sync requirement for the handler function
  passed to Subscription::with_handler.

# 0.8.4

## Bug Fixes

- #124 Fix regex error when parsing creds due to missing
  the `unicode-perl` feature flag on the regex crate.

# 0.8.3

## New Features

- Add `Options::client_cert()`.

# 0.8.2

## Bug Fixes

- Flush outstanding messages when `Connection` is dropped.
- Call callbacks configured by `Options`.
- Shutdown only when the last `Connection` is dropped.

# 0.8.1

## Improvements

- Remove `async-dup` dependency.
- Update dependencies, notably `nkeys` to v0.0.11.

## Bug Fixes

- Fix a bug due to which TLS authentication was not
  working.
- Shutdown the client thread when `Connection` is dropped.

# 0.8.0

## New Features

- Add `asynk::Message::respond()`.
- Add `Options::with_nkey()`.

## Improvements

- Update the `smol` dependency.

## Breaking Changes

- Remove `crossbeam-channel` from the public API.

# 0.7.4

## Improvements

* Remove the `MutexGuard` held across await points
  inside `cleanup_subscriptions()` to allow futures
  returned by async methods to implement `Send`.

# 0.7.3

## New Features

* Expose the `asynk` module with the async client API.

# 0.7.2

## Bug Fixes

* Implement `Subscription::close` and
  `Subscription::unsubscribe` correctly, which would
  previously do nothing.

# 0.7.1

## Bug Fixes

* Fix a deadlock in `Subscription` when concurrently
  receiving the next message and draining.

## Misc

* Add `--auth-token` flag to the `nats-box` example.

# 0.7.0

## New Features

* Support has been added for NATS Headers
  via the `Connection::publish_with_reply_or_headers`
  method.

## Breaking Changes

* The underlying TLS implementation has been switched
  from native-tls to rustls. The previously exported
  TLS functionality has been removed, and now you can
  supply certificates with the
  `Options::add_root_certificate` method.

# 0.6.0

## New Features

* An experimental async `Connection` is now available
  to adventurous explorers by calling
  `Options::connect_async`.

## Breaking Changes

* `ConnectionOptions` has been renamed `Options`.
* The minimum supported Rust version (MSRV) is now
  1.40.0.

# 0.5.0

## Breaking Changes

* #60 `ConnectionOptions` construction has been simplified,
  and the `with_user_pass`, `with_token`, and
  `with_credentials` methods have been changed to being
  constructors for the type, rather than producing
  intermediate states.

# 0.4.0

## New Features

* #57 The `drain` method has been added to the
  `Connection` and `Subscription` structs.

## Breaking Changes

* #36 `Connection::close` is now infallible and has no
  return value
* #36 The redundant `Subscription::close` has been
  removed. The same functionality exists in
  `Subscription::unsubscribe`.
* bumped the MSRV to 1.39.0 in anticipation of possible
  async support.

# 0.3.2

## Misc

* The minimum supported Rust version (MSRV) is now
  made explicit, and lowered to version 1.37.0 from
  1.42.0. Crate version 0.3.1 has been yanked due to
  it having silently broken older Rust versions than
  1.42.0.

# 0.3.1

## New Features

* #19 TLS support has been added.

# 0.3

## New Features

* #16 Implement reconnection logic.
* #16 Buffer outbound data when in a disconnected state.
* #16 Learn about new servers using the received INFO block.
* #16 Callback functions may be provided via the new
  `Options::set_disconnect_callback` and
  `Options::set_reconnect_callback` which will be executed
  when the connection to a server has been terminated
  or when a new connection has been established afterward.

## Breaking Changes

* #11 `Connection::new` has been renamed `Options::new`.
* #13 The various iterators have been replaced with concrete
  implementations: `Iter`, `IntoIter`, `TimeoutIter` which
  ensure that the backing `Subscription` is not closed
  while they are in use.
