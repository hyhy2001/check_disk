#!/usr/bin/env bash
# build.sh — Build native CDX1 query artifacts with broad glibc compatibility.
#
# Usage:
#   bash build.sh          # glibc >= 2.17 (zig cross-compat, recommended)
#   bash build.sh 2.28     # glibc >= 2.28
#   bash build.sh native   # host compiler/glibc

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GLIBC_VER="${1:-2.17}"

export PATH="/root/.local/bin:$PATH"
cd "$SCRIPT_DIR"

echo "==> Building native_index (glibc target: ${GLIBC_VER})"

if [ "$GLIBC_VER" = "native" ]; then
  make clean
  make
else
  if ! command -v zig >/dev/null 2>&1; then
    echo "zig not found. Fallback native build."
    make clean
    make
  else
    export CC="zig cc -target x86_64-linux-gnu.${GLIBC_VER}"
    make clean
    make
  fi
fi

echo ""
echo "==> Done. glibc requirements (libcdx1.so):"
objdump -p "$SCRIPT_DIR/libcdx1.so" | grep GLIBC | sort -V || true
