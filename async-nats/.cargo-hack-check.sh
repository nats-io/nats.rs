#!/bin/bash
# Optimized cargo-hack strategy for async-nats
# Due to cargo-hack limitations, we use --each-feature with strategic combinations

set -e

echo "=== Strategy 1: Test each feature individually (with ring) ==="
cargo hack check \
  --each-feature \
  --exclude-features slow_tests,compatibility_tests,experimental,fips,aws-lc-rs \
  --features ring \
  --no-dev-deps

echo ""
echo "=== Strategy 2: Test important combinations ==="
# Minimal
cargo check --no-default-features --features ring
# JetStream only
cargo check --no-default-features --features jetstream,ring
# Service only
cargo check --no-default-features --features service,ring
# JetStream + KV
cargo check --no-default-features --features kv,ring
# JetStream + Object Store
cargo check --no-default-features --features object-store,ring
# Full stack without server flags
cargo check --no-default-features --features jetstream,kv,object-store,service,nkeys,crypto,websockets,ring
# Default
cargo check

echo ""
echo "=== Strategy 3: Test with aws-lc-rs backend ==="
cargo check --no-default-features --features jetstream,kv,service,nkeys,aws-lc-rs
cargo check --no-default-features --features aws-lc-rs

echo ""
echo "=== Strategy 4: Test server version flags ==="
cargo check --features server_2_10
cargo check --features server_2_11
cargo check --features server_2_12

echo ""
echo "All checks passed!"
