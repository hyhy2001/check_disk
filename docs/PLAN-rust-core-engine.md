# 🚀 Project Plan: Rust Core Engine Integration

## 1. Overview
The goal is to rewrite the core disk scanning engine (`disk_scanner.py`) in Rust to bypass Python's GIL and maximize true multi-threading performance on large directories. The Rust extension will be exposed to Python natively via PyO3. An intelligent auto-fallback mechanism will ensure the existing pure-Python scanner runs seamlessly if the Rust module is unavailable or fails to load.

## 2. Project Type
**BACKEND** (High-Performance Systems Programming)

## 3. Success Criteria
- [ ] A Rust scanning core (`fast_scanner.so`) is successfully compiled and imported in Python.
- [ ] Safe Auto-Fallback: If `import fast_scanner` fails, the system logs a warning and uses the legacy `DiskScanner` class seamlessly.
- [ ] The Rust core strictly matches the output signature of the Python engine (returning equivalent dictionaries of UID sizes, dir sizes, and performing its own TSV chunk flushing with K-way merge compliance).
- [ ] Provide an automated setup script or instructions for installing/compiling the Rust core on the VPS.

## 4. Tech Stack
- **Rust**: Using the `jwalk` crate for lighting-fast parallel directory traversal.
- **PyO3 / Maturin**: For generating the Python bindings directly into a `.so` extension.
- **Python**: Existing architecture with a dynamic module loading (try-except) wrapper block.

## 5. File Structure
Target file additions:
```text
src/
 ├── rust_scanner/
 |    ├── Cargo.toml
 |    ├── pyproject.toml
 |    └── src/
 |         └── lib.rs       <-- The Rust core logic
 ├── disk_scanner.py        <-- Updated with auto-fallback proxy wrapper
```

## 6. Task Breakdown

### Task 1: Environment Setup & Bootstrapping
- **Agent**: `backend-specialist` / `rust-pro`
- **Skill**: `bash-linux`, `rust-pro`
- **Priority**: High (P0)
- **Description**: Verify the VPS Rust installation automatically. Scaffold the PyO3/Maturin project inside `src/rust_scanner/`.
- **INPUT**: Empty directory.
- **OUTPUT**: `Cargo.toml` and basic Python binding `lib.rs` ready to compile.
- **VERIFY**: Run `cargo check` and `maturin develop` to confirm successful environment initialization.

### Task 2: Implement Rust High-Performance Core
- **Agent**: `rust-pro`
- **Skill**: `rust-pro`
- **Priority**: High (P1)
- **Description**: Recreate the memory-efficient directory traversal using Rust multithreading. Replicate the skip logic (`critical_skip_dirs`, snapshot detection), the streaming TSV chunk flushing (with unique `_cZ.tsv` filenames and `w` mode), and implement PyO3 wrappers to return Python-typed dictionaries of the aggregated stats back to the main app.
- **INPUT**: Python threading logic requirements.
- **OUTPUT**: Fully functional Rust scanning function pushing data to TSVs and returning stats.
- **VERIFY**: Test Rust module isolated against a small directory tree.

### Task 3: Python Auto-Fallback Proxy
- **Agent**: `backend-specialist`
- **Skill**: `python-patterns`
- **Priority**: High (P1)
- **Description**: Modify `disk_checker.py` and `disk_scanner.py` to intercept the scanning run. Add a `try...except ImportError` block that tries to load the Rust engine. If successful, route the parameters to Rust. If it fails, visibly fallback to the pure-Python `DiskScanner`.
- **INPUT**: Current `DiskScanner` invocation.
- **OUTPUT**: Refactored entry points supporting dual modes.
- **VERIFY**: Force an `ImportError` on a clean machine to simulate a missing `.so` and ensure graceful pure-Python fallback.

## 7. Phase X: Verification
- [ ] Check if `rustc` and `cargo` are available. (Installing if necessary via `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`).
- [ ] Compile the `fast_scanner` module natively on the VPS.
- [ ] Execute `python disk_checker.py --run` and observe the staggering speed improvement and identical formatting.
- [ ] Execution script passed and User Approval acknowledged.
