# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

geoarrow-rs is a Rust implementation of the GeoArrow specification with Python and JavaScript/WebAssembly bindings. It provides efficient spatial operations on GeoArrow memory layout, integrating with GeoRust algorithms for geospatial computation.

## Repository Structure

The repository is organized as a multi-language monorepo:

- `rust/` - Core Rust crates organized as a workspace:
  - `geoarrow-*` crates for core functionality (array, cast, schema, etc.)
  - `geoparquet/` - GeoParquet format support
  - `pyo3-geoarrow/` - Python FFI bindings
- `python/` - Python bindings with separate modules:
  - `geoarrow-core/` - Core data structures
  - `geoarrow-compute/` - Compute operations
  - `geoarrow-io/` - File format I/O
- `js/` - JavaScript/WebAssembly bindings
- `fixtures/` - Test data and examples
- `docs/` - Documentation source

## Development Commands

### Rust Development
- **Test**: `cargo test --all-features` (requires external dependencies via pixi)
- **Lint**: `cargo clippy --all-features --tests -- -D warnings`
- **Format check**: `cargo +nightly-2025-05-14 fmt -- --check --unstable-features --config imports_granularity=Module,group_imports=StdExternalCrate`
- **Check**: `cargo check --all-features`
- **Docs**: `cargo doc --all-features --document-private-items`

### Python Development
- Individual modules are built using maturin
- Test with pytest: `pytest` in relevant module directories
- Python modules use namespace packaging (`geoarrow-rust-*`)

### JavaScript Development
- **Build**: `npm run build` (requires various FEATURES env var configurations)
- **Test**: `npm run test` (using vitest)
- **Build variants**:
  - `npm run build:geoparquet` - GeoParquet-only build
  - `npm run build:flatgeobuf` - FlatGeobuf-only build

## External Dependencies

The project uses pixi for managing external dependencies (GEOS, GDAL, PROJ):
- Configuration: `build/pixi.toml`
- Set up environment variables for CI/local development:
  ```bash
  PKG_CONFIG_PATH=$(pwd)/build/.pixi/envs/default/lib/pkgconfig
  LD_LIBRARY_PATH=$(pwd)/build/.pixi/envs/default/lib
  ```

## Architecture Notes

- **GeoArrow Format**: Implements columnar geospatial data format built on Apache Arrow
- **Multi-target**: Single Rust codebase compiles to native Rust, Python extensions via PyO3, and WebAssembly
- **Workspace Structure**: Uses Cargo workspace for managing multiple related crates
- **Zero-copy**: Focus on efficient memory management and zero-copy operations between language boundaries
- **Modular Design**: Clear separation between core arrays, compute operations, and I/O functionality

## Common Issues

- CMAKE version errors: Set `export CMAKE_POLICY_VERSION_MINIMUM=3.5`
- External dependencies are managed through pixi environment, not system packages

## Conventional Commits

All PRs must follow conventional commits format. Squash merge is used, so only PR titles need to conform. Allowed types are defined in `.github/workflows/conventional-commits.yml`.
