# `geoarrow-rs`

A Rust implementation of the [GeoArrow](https://github.com/geoarrow/geoarrow) specification and bindings to [GeoRust algorithms](https://github.com/georust/geo) for efficient spatial operations on GeoArrow memory.

This repository also includes [JavaScript (WebAssembly) bindings](https://github.com/kylebarron/geoarrow-rs/blob/main/js/README.md), wrapping the GeoArrow memory layout and offering vectorized geometry operations.

## Documentation

- Rust library <https://docs.rs/geoarrow2/latest/geoarrow2/>
- JavaScript library: <https://geoarrow.github.io/geoarrow-rs/js/>
- Rust wasm-bindgen library: <https://docs.rs/geoarrow-wasm/latest/geoarrow_wasm/>

## Examples

- [Rust examples](examples/README.md)

## Install

Add this to your `Cargo.toml`:

```toml
geoarrow = { git = "https://github.com/geoarrow/geoarrow-rs" }
```

## References

- [Prototyping GeoRust + GeoArrow in WebAssembly](https://observablehq.com/@kylebarron/prototyping-georust-geoarrow-in-webassembly) gives an early preview of the JavaScript API.
- [GeoArrow and GeoParquet in deck.gl](https://observablehq.com/@kylebarron/geoarrow-and-geoparquet-in-deck-gl) gives an overview of what GeoArrow's memory layout looks like under the hood, even though it's focused on how to render the data on a map.
- [Thoughts on GEOS in WebAssembly](https://kylebarron.dev/blog/geos-wasm) introduces why I think GeoRust + GeoArrow on the web has significant potential.
- [Zero-copy Apache Arrow with WebAssembly](https://observablehq.com/@kylebarron/zero-copy-apache-arrow-with-webassembly) explains how the JavaScript bindings are able to move memory between JavaScript and WebAssembly so efficiently.
