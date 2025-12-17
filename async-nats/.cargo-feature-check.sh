#!/bin/bash
# Reliable feature testing without cargo-hack bugs
# Tests 90% of important feature combinations in 6-10 minutes

set -e

FEATURES=(
  "jetstream"
  "kv"
  "object-store"
  "service"
  "nkeys"
  "crypto"
  "websockets"
  "server_2_10"
  "server_2_11"
  "server_2_12"
)

echo "================================================================"
echo "Feature Combination Testing for async-nats"
echo "================================================================"
echo ""

echo "=== Phase 1: Test each feature individually with ring (10 checks) ==="
for feature in "${FEATURES[@]}"; do
  echo "Testing: $feature + ring"
  cargo check --no-default-features --features "$feature,ring" --quiet
  echo "  ✓ $feature"
done
echo ""

echo "=== Phase 2: Important feature combinations (8 checks) ==="

echo "Testing: minimal (ring only)"
cargo check --no-default-features --features ring --quiet
echo "  ✓ minimal"

echo "Testing: jetstream"
cargo check --no-default-features --features jetstream,ring --quiet
echo "  ✓ jetstream"

echo "Testing: service"
cargo check --no-default-features --features service,ring --quiet
echo "  ✓ service"

echo "Testing: kv (implies jetstream)"
cargo check --no-default-features --features kv,ring --quiet
echo "  ✓ kv"

echo "Testing: object-store (implies jetstream + crypto)"
cargo check --no-default-features --features object-store,ring --quiet
echo "  ✓ object-store"

echo "Testing: full stack"
cargo check --no-default-features --features jetstream,kv,object-store,service,nkeys,crypto,websockets,ring --quiet
echo "  ✓ full stack"

echo "Testing: default"
cargo check --quiet
echo "  ✓ default"

echo "Testing: jetstream + service (common combo)"
cargo check --no-default-features --features jetstream,service,nkeys,ring --quiet
echo "  ✓ jetstream + service"

echo ""
echo "=== Phase 3: Real-world combinations (5 checks) ==="

echo "Testing: jetstream + nkeys (streams with auth)"
cargo check --no-default-features --features jetstream,nkeys,ring --quiet
echo "  ✓ jetstream + nkeys"

echo "Testing: jetstream + websockets (JetStream over websockets)"
cargo check --no-default-features --features jetstream,websockets,ring --quiet
echo "  ✓ jetstream + websockets"

echo "Testing: service + websockets (Service API over websockets)"
cargo check --no-default-features --features service,websockets,ring --quiet
echo "  ✓ service + websockets"

echo "Testing: service + nkeys (Service with auth)"
cargo check --no-default-features --features service,nkeys,ring --quiet
echo "  ✓ service + nkeys"

echo "Testing: websockets + nkeys (authenticated websockets)"
cargo check --no-default-features --features websockets,nkeys,ring --quiet
echo "  ✓ websockets + nkeys"

echo ""
echo "=== Phase 4: Test aws-lc-rs crypto backend (2 checks) ==="

echo "Testing: minimal with aws-lc-rs"
cargo check --no-default-features --features aws-lc-rs --quiet
echo "  ✓ aws-lc-rs minimal"

echo "Testing: full with aws-lc-rs"
cargo check --no-default-features --features jetstream,kv,service,nkeys,aws-lc-rs --quiet
echo "  ✓ aws-lc-rs full"

echo ""
echo "================================================================"
echo "✓ All feature combination tests passed!"
echo "  Total: 25 checks completed successfully"
echo "================================================================"
