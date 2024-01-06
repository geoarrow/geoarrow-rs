# `geoarrow.rust.core`

Python bindings to `geoarrow-rs`

## Overview

This library contains Python bindings to the [GeoArrow Rust implementation](https://github.com/geoarrow/geoarrow-rs).

- **Fast**: Connects to algorithms implemented in [GeoRust](https://georust.org/), which compile to native code.
- **Parallel**: Multi-threading is enabled out-of-the-box for all operations on chunked data structures.
- **Self-contained**: Zero Python dependencies.
- **Easy to install**: Distributed as static binary wheels with zero C dependencies.
- **Strong, static typing**: geometry arrays have a known type
- **Interoperable ecosystem**: Data is shared at zero cost with other Python libraries in the burgeoning [GeoArrow ecosystem](https://geoarrow.org/), such as [geoarrow-c](https://github.com/geoarrow/geoarrow-c/tree/main/python) or [lightning-fast map rendering](https://github.com/developmentseed/lonboard).

More specifically, it contains:

- Classes to represent GeoArrow arrays: `PointArray`, `LineStringArray`, etc.
- Classes to represent _chunked_ GeoArrow arrays: `ChunkedPointArray`, `ChunkedLineStringArray`, etc.
- A spatial table representation, `GeoTable`, where one column is a geospatial type, to enable future support for geospatial joins.
- Rust-based algorithms for computations on GeoArrow memory.
- Rust-based parsers for various geospatial file formats.

## Documentation

Refer to the documentation at [geoarrow.github.io/geoarrow-rs/python](https://geoarrow.github.io/geoarrow-rs/python).

## Future work:

- [ ] 3D coordinates. Only 2D geometries are supported at this time.
- [ ] More algorithms, including spatial indexes and spatial joins.
- [ ] CRS management. This currently loses the CRS information in the [GeoArrow metadata](https://geoarrow.org/extension-types#extension-metadata).

## Background reading

Refer to the [GeoArrow Python module proposal](https://github.com/geoarrow/geoarrow-python/issues/38) for more background information.
