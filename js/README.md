# `geoarrow-wasm`

Efficient, vectorized geospatial operations in WebAssembly.

This library defines efficient data structures for arrays of geometries (by wrapping the Rust implementation of GeoArrow, [`geoarrow-rs`](https://github.com/geoarrow/geoarrow-rs)) and connects to [GeoRust](https://github.com/georust/geo), a suite of geospatial algorithms implemented in Rust.

Note that this is an _opinionated_ library. Today, it chooses performance over ease of use. Over time it will get easier to use.

## Documentation

- JavaScript library: <https://geoarrow.github.io/geoarrow-rs/js>
- Rust wasm-bindgen library: <https://docs.rs/geoarrow-wasm/latest/geoarrow_wasm/>

## Why?

I wrote a [blog post](https://kylebarron.dev/blog/geos-wasm) about this that goes into more detail.

## Install

### From JavaScript

Most users will use this by installing the prebuilt JavaScript package. This is published to NPM as [`geoarrow-wasm`](https://npmjs.com/package/geoarrow-wasm).


### From Rust

Advanced users can also depend on these Rust-Wasm bindings directly, enabling you to add custom operations on top of these bindings and generating your own WebAssembly bundles. This means you can reuse all the binding between JavaScript and WebAssembly and focus on implementing your algorithms. This package is published to crates.io as [`geoarrow-wasm`](https://crates.io/crates/geoarrow-wasm).

## Examples

- [Prototyping GeoRust + GeoArrow in WebAssembly](https://observablehq.com/@kylebarron/prototyping-georust-geoarrow-in-webassembly)

## How it Works


