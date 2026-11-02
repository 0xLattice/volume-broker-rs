#!/bin/sh
set -eu
root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
upstream=${1:-"$root/../VolumeBroker"}
[ "$(git -C "$upstream" rev-parse HEAD)" = 9173f75c7c88a60b56512c71f3917224792af686 ] || { echo 'upstream revision differs' >&2; exit 1; }
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
swiftc -O "$upstream/Sources/VolumeBroker/SQLiteConnection.swift" "$upstream/Sources/VolumeBroker/BrokerErrors.swift" "$root/tools/SQLiteInterop.swift" -o "$tmp/interop"
cid=$(python3 -c 'import hashlib,base64; print("b"+base64.b32encode(bytes([1,85,18,32])+hashlib.sha256(b"hello").digest()).decode().lower().rstrip("="))')
"$tmp/interop" "$tmp/db" create "$cid"
cargo run --manifest-path "$root/Cargo.toml" --example sqlite_interop -- "$tmp/db"
"$tmp/interop" "$tmp/db" verify
