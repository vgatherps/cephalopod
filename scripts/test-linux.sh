#!/usr/bin/env bash
# Runs cargo in a Linux container. Args go to cargo; default is `test --workspace`.
# PLATFORM=linux/amd64 selects the x86_64 image (emulated on Apple silicon).
set -euo pipefail

repo="$(cd "$(dirname "$0")/.." && pwd)"
platform="${PLATFORM:-linux/arm64}"
tag="${platform//\//-}"

if [ $# -eq 0 ]; then set -- test --workspace; fi

exec docker run --rm --platform "$platform" \
    --shm-size=1g \
    -v "$repo":/w/repo \
    -v "cephalopod-cargo-$tag":/usr/local/cargo/registry \
    -v "cephalopod-target-$tag":/w/target \
    -e CARGO_TARGET_DIR=/w/target \
    -w /w/repo \
    rust:1.93.0-bookworm cargo "$@"
