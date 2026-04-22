#!/usr/bin/env bash
# build.sh — Build fast_scanner.abi3.so compatible với glibc 2.17+ (CentOS 7, Debian 9+)
#
# Yêu cầu:
#   cargo, zig, cargo-zigbuild (cargo install cargo-zigbuild)
#   rustup target add x86_64-unknown-linux-gnu
#
# Cách dùng:
#   bash build.sh          # glibc ≥ 2.17 (tương thích rộng nhất)
#   bash build.sh 2.28     # glibc ≥ 2.28 (Debian 10 / Ubuntu 18.04)
#   bash build.sh native   # dùng glibc máy hiện tại (không tương thích cross)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT_DIR="$SCRIPT_DIR/.."
LIB_NAME="fast_scanner"
GLIBC_VER="${1:-2.17}"

export PATH="/root/.local/bin:$PATH"

echo "==> Building $LIB_NAME.abi3.so (glibc target: $GLIBC_VER)"
cd "$SCRIPT_DIR"

if [ "$GLIBC_VER" = "native" ]; then
    cargo build --release --target x86_64-unknown-linux-gnu
else
    cargo-zigbuild zigbuild --release --target "x86_64-unknown-linux-gnu.${GLIBC_VER}"
fi

RELEASE_SO="target/x86_64-unknown-linux-gnu/release/lib${LIB_NAME}.so"

strip "$RELEASE_SO"
cp "$RELEASE_SO" "$OUT_DIR/${LIB_NAME}.abi3.so"
rm -rf target build

echo ""
echo "==> Done. glibc requirements:"
objdump -p "$OUT_DIR/${LIB_NAME}.abi3.so" | grep GLIBC | sort -V
