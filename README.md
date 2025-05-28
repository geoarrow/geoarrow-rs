# geoarrow-rs

A Rust implementation of the [GeoArrow](https://github.com/geoarrow/geoarrow) specification and bindings to [GeoRust algorithms](https://github.com/georust/geo) for efficient spatial operations on GeoArrow memory.

This repository also includes [Python bindings](https://github.com/geoarrow/geoarrow-rs/blob/main/python/README.md) and [JavaScript (WebAssembly) bindings](https://github.com/geoarrow/geoarrow-rs/blob/main/js/README.md), wrapping the GeoArrow memory layout and offering vectorized geometry operations.

## Documentation

[**Documentation Website**](https://geoarrow.org/geoarrow-rs/)

<!-- - [Use from Rust](https://geoarrow.org/geoarrow-rs/rust) -->
<!-- - [Use from Python](https://geoarrow.org/geoarrow-rs/python) -->
<!-- - [Use from JavaScript](https://geoarrow.org/geoarrow-rs/js)
- [Create your own Rust-JavaScript library with `wasm-bindgen`](https://docs.rs/geoarrow-wasm/latest/geoarrow_wasm/) -->
<!-- - [Create your own Rust-Python library with `pyo3-geoarrow`](https://docs.rs/geoarrow-wasm/latest/geoarrow_wasm/) -->

## Project Status

May 7, 2025

The `geoarrow-rs` project is about 3 years old. Early prototyping started inside the [`geopolars`](https://github.com/geopolars/geopolars) repo before deciding I needed something more general (not tied to Polars) and creating first [`geopolars/geoarrow`](https://github.com/geopolars/geoarrow) and then lastly [`geoarrow/geoarrow-rs`](https://github.com/geoarrow/geoarrow-rs).

However despite its age, the `geoarrow` crate suffers from a couple issues. For one, it took a while to figure out the right abstractions. And learning Rust at the same time didn't help. The `geoarrow` crate is also too monolithic. Some parts of `geoarrow` are production ready, but there's decidedly a _lot_ of code in `geoarrow` that is _not_ production ready. And it's very much not clear which parts of `geoarrow` are production ready or not.

But I think `geoarrow-rs` has potential to form part of the core geospatial data engineering stack in Rust. And as part of that, we need a better delineation of which parts of the code are stable or not. As such, the `geoarrow-rs` repo is **currently ongoing a large refactor from a single crate to a monorepo of smaller crates, each with a more well-defined scope**.

As of May 2025, avoid using the `geoarrow` crate and instead use the newer crates with a smaller scope, like `geoarrow-array`.

These smaller crates are designed to mimic the upstream `arrow` crates as much as possible.

### Stability

- `geoarrow-schema`: Pretty stable
- `geoarrow-array`: Pretty stable
- `geoarrow-cast`: Pretty stable
- `geoarrow-flatgeobuf`: Somewhat stable.
- `geoparquet`: Somewhat stable. A decently large refactor is expected in <https://github.com/geoarrow/geoarrow-rs/pull/1089>.
- Other Rust crates: unstable
- Python bindings: unstable
- JS bindings: unstable

## References

- [Prototyping GeoRust + GeoArrow in WebAssembly](https://observablehq.com/@kylebarron/prototyping-georust-geoarrow-in-webassembly) gives an early preview of the JavaScript API.
- [GeoArrow and GeoParquet in deck.gl](https://observablehq.com/@kylebarron/geoarrow-and-geoparquet-in-deck-gl) gives an overview of what GeoArrow's memory layout looks like under the hood, even though it's focused on how to render the data on a map.
- [Thoughts on GEOS in WebAssembly](https://kylebarron.dev/blog/geos-wasm) introduces why I think GeoRust + GeoArrow on the web has significant potential.
- [Zero-copy Apache Arrow with WebAssembly](https://observablehq.com/@kylebarron/zero-copy-apache-arrow-with-webassembly) explains how the JavaScript bindings are able to move memory between JavaScript and WebAssembly so efficiently.
