#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CRATE_DIR="$ROOT_DIR/async-nats"
CONFIG_PATH="$CRATE_DIR/tests/configs/ws.conf"
TARGET_DIR="$ROOT_DIR/target/wasm32-unknown-unknown/debug/deps"

NATS_SERVER_BIN="${NATS_SERVER_BIN:-nats-server}"
WASM_RUNNER_BIN="${WASM_BINDGEN_TEST_RUNNER:-wasm-bindgen-test-runner}"

if ! command -v "$NATS_SERVER_BIN" >/dev/null 2>&1; then
    echo "nats-server was not found in PATH."
    echo "Set NATS_SERVER_BIN=/path/to/nats-server or install nats-server first."
    exit 1
fi

if ! command -v "$WASM_RUNNER_BIN" >/dev/null 2>&1; then
    echo "wasm-bindgen-test-runner was not found in PATH."
    echo "Set WASM_BINDGEN_TEST_RUNNER=/path/to/wasm-bindgen-test-runner first."
    exit 1
fi

SERVER_LOG="$(mktemp -t async-nats-ws-server.XXXXXX.log)"

cleanup() {
    if [[ -n "${SERVER_PID:-}" ]] && kill -0 "$SERVER_PID" >/dev/null 2>&1; then
        kill "$SERVER_PID" >/dev/null 2>&1 || true
        wait "$SERVER_PID" >/dev/null 2>&1 || true
    fi
    rm -f "$SERVER_LOG"
}

trap cleanup EXIT

echo "Starting websocket NATS server with $CONFIG_PATH"
"$NATS_SERVER_BIN" -c "$CONFIG_PATH" >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!

sleep 2

if ! kill -0 "$SERVER_PID" >/dev/null 2>&1; then
    echo "nats-server exited unexpectedly:"
    cat "$SERVER_LOG"
    exit 1
fi

echo "Building wasm test artifact"
cargo test \
    -p async-nats \
    --target wasm32-unknown-unknown \
    --no-default-features \
    --features websockets-web,service,server_2_10 \
    --test websocketweb_test \
    --no-run \
    --manifest-path "$CRATE_DIR/Cargo.toml"

WASM_TEST="$(find "$TARGET_DIR" -name 'websocketweb_test-*.wasm' -print | sort | tail -n 1)"

if [[ -z "$WASM_TEST" ]]; then
    echo "Could not find the built websocketweb_test wasm artifact in $TARGET_DIR"
    exit 1
fi

echo "Running wasm test with $WASM_RUNNER_BIN"
"$WASM_RUNNER_BIN" "$WASM_TEST"
