#!/usr/bin/env bash
# build.sh — Build the Rust scanner extension and place it next to the
# Python sources so `import fast_scanner` works without installation.
#
# Compatibility: bash 4+ on Linux/macOS. Cross-arch (x86_64 / aarch64) and
# Python 3.8+ via PyO3 abi3-py38. No virtualenv required to build, since
# the resulting .so loads on any compatible Python.
#
# Usage:
#     ./build.sh                # release build, install to src/
#     ./build.sh debug          # debug build (faster compile, slower runtime)
#     ./build.sh check          # cargo check only (compile-test, no .so install)
#     ./build.sh --help         # this help
#
# Exit codes:
#     0 ok      1 missing toolchain     2 build failed     3 install failed

set -euo pipefail

# ── Paths ────────────────────────────────────────────────────────────────
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
RUST_DIR="${SCRIPT_DIR}/src/rust_scanner"
PY_DEST_DIR="${SCRIPT_DIR}/src"
EXT_NAME="fast_scanner"

# ── Args ─────────────────────────────────────────────────────────────────
MODE="release"
case "${1:-}" in
    ""|release) MODE="release" ;;
    debug)      MODE="debug" ;;
    check)      MODE="check" ;;
    -h|--help)
        sed -n '2,15p' "${BASH_SOURCE[0]}" | sed 's/^# \?//'
        exit 0
        ;;
    *)
        echo "error: unknown mode '${1}' (use: release | debug | check | --help)" >&2
        exit 1
        ;;
esac

# ── Color output (TTY only) ──────────────────────────────────────────────
if [ -t 1 ]; then
    BOLD='\033[1m'; DIM='\033[2m'; GREEN='\033[32m'; YELLOW='\033[33m'
    RED='\033[31m'; CYAN='\033[36m'; RESET='\033[0m'
else
    BOLD=''; DIM=''; GREEN=''; YELLOW=''; RED=''; CYAN=''; RESET=''
fi
say()  { printf "${BOLD}%s${RESET}\n" "$1"; }
info() { printf "${DIM}  %s${RESET}\n" "$1"; }
ok()   { printf "${GREEN}✓${RESET} %s\n" "$1"; }
warn() { printf "${YELLOW}⚠${RESET}  %s\n" "$1"; }
err()  { printf "${RED}✗ %s${RESET}\n" "$1" >&2; }

# ── Toolchain check ──────────────────────────────────────────────────────
say "[1/4] Checking Rust toolchain..."
if ! command -v cargo >/dev/null 2>&1; then
    err "cargo not found. Install Rust:"
    err "  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh"
    exit 1
fi
RUST_VERSION="$(rustc --version 2>/dev/null || echo unknown)"
ok "Rust: ${RUST_VERSION}"

# ── Detect host platform → expected library extension ───────────────────
UNAME_S="$(uname -s)"
UNAME_M="$(uname -m)"
case "${UNAME_S}" in
    Linux*)
        LIB_PREFIX="lib"
        LIB_EXT="so"
        # Python expects "<name>.abi3.so" (or "<name>.cpython-<ver>-<arch>.so")
        # PyO3 abi3 build produces a generic name; we rename on copy.
        PY_EXT_SUFFIX="abi3.so"
        ;;
    Darwin*)
        LIB_PREFIX="lib"
        LIB_EXT="dylib"
        PY_EXT_SUFFIX="abi3.so"   # macOS Python loads .so even from a .dylib symlink
        ;;
    MINGW*|MSYS*|CYGWIN*)
        LIB_PREFIX=""
        LIB_EXT="dll"
        PY_EXT_SUFFIX="pyd"
        ;;
    *)
        warn "Untested platform '${UNAME_S}'; assuming Linux ELF layout."
        LIB_PREFIX="lib"
        LIB_EXT="so"
        PY_EXT_SUFFIX="abi3.so"
        ;;
esac
ok "Platform: ${UNAME_S} (${UNAME_M}) → ${LIB_PREFIX}${EXT_NAME}.${LIB_EXT} → ${EXT_NAME}.${PY_EXT_SUFFIX}"

# ── Build ────────────────────────────────────────────────────────────────
say "[2/4] Building Rust extension (${MODE})..."
cd "${RUST_DIR}"

case "${MODE}" in
    check)
        if cargo check 2>&1 | sed 's/^/    /'; then
            ok "cargo check passed"
            exit 0
        else
            err "cargo check failed"
            exit 2
        fi
        ;;
    debug)
        BUILD_ARGS=()
        TARGET_DIR="target/debug"
        ;;
    release)
        BUILD_ARGS=(--release)
        TARGET_DIR="target/release"
        ;;
esac

if ! cargo build "${BUILD_ARGS[@]}"; then
    err "cargo build failed"
    exit 2
fi
ok "Build succeeded"

# ── Locate output ────────────────────────────────────────────────────────
say "[3/4] Locating compiled artifact..."
SRC_LIB="${RUST_DIR}/${TARGET_DIR}/${LIB_PREFIX}${EXT_NAME}.${LIB_EXT}"

if [ ! -f "${SRC_LIB}" ]; then
    err "Expected library not found: ${SRC_LIB}"
    err "Build produced these files in ${TARGET_DIR}:"
    ls -la "${RUST_DIR}/${TARGET_DIR}" 2>&1 | grep -E "\.(so|dylib|dll)$" | sed 's/^/    /' >&2 || true
    exit 3
fi
ok "Found: ${SRC_LIB}"

# ── Install (rename to PyO3-loadable name) ───────────────────────────────
say "[4/4] Installing to ${PY_DEST_DIR}/..."
DEST="${PY_DEST_DIR}/${EXT_NAME}.${PY_EXT_SUFFIX}"
mkdir -p "${PY_DEST_DIR}"
cp "${SRC_LIB}" "${DEST}"
chmod 755 "${DEST}"
SIZE_BYTES="$(stat -c%s "${DEST}" 2>/dev/null || stat -f%z "${DEST}" 2>/dev/null || echo "?")"
ok "Installed: ${DEST} (${SIZE_BYTES} bytes)"

# ── Smoke test (best effort — needs Python on PATH) ──────────────────────
if command -v python3 >/dev/null 2>&1; then
    info "Verifying import..."
    if (cd "${SCRIPT_DIR}" && python3 -c "
import sys
sys.path.insert(0, 'src')
import ${EXT_NAME}
api = [x for x in dir(${EXT_NAME}) if not x.startswith('_')]
print('  module path:', ${EXT_NAME}.__file__)
print('  exported symbols:', ', '.join(api[:5]) + ('…' if len(api) > 5 else ''))
" 2>&1 | sed 's/^/    /'); then
        ok "Module loads correctly"
    else
        warn "Import smoke test failed — .so may be incompatible with this Python."
    fi
else
    info "python3 not on PATH; skipping smoke test."
fi

printf "\n${GREEN}${BOLD}Build complete.${RESET}\n"
printf "${DIM}Run a scan with:${RESET}\n"
printf "    ${CYAN}python3 disk_checker.py --directory <path> --output-dir <out>${RESET}\n\n"
