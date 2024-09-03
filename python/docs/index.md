# `geoarrow-rust`

A Python library implementing the [GeoArrow](https://geoarrow.org/) specification with efficient spatial operations. This library has "rust" in the name because it is implemented based on the [GeoArrow Rust implementation](https://github.com/geoarrow/geoarrow-rs).

## Project goals

- **Fast**: Connects to algorithms implemented in [GeoRust](https://georust.org/), which compile to native code.
- **Parallel**: Multi-threading is enabled out-of-the-box for all operations on chunked data structures.
- **Self-contained**: Zero Python dependencies.
- **Easy to install**: Distributed as static binary wheels with zero C dependencies.
- **Strong, static typing**: geometry arrays have a known type
- **Interoperable ecosystem**: Data is shared at zero cost with other Python libraries in the burgeoning [GeoArrow ecosystem](https://geoarrow.org/), such as [geoarrow-c](https://github.com/geoarrow/geoarrow-c/tree/main/python) or [lightning-fast map rendering](https://github.com/developmentseed/lonboard).

More specifically, it contains:

- Classes to represent GeoArrow arrays: `PointArray`, `LineStringArray`, etc.
- Classes to represent _chunked_ GeoArrow arrays: `ChunkedPointArray`, `ChunkedLineStringArray`, etc.
- A spatial table representation, `GeoTable`, where one column is a geospatial type and [Apache Arrow](https://arrow.apache.org/) is used to represent attribute columns. This enables future support for table-based operations like geospatial joins.
- Rust-based algorithms for computations on GeoArrow memory.
- Rust-based parsers for various geospatial file formats.

## Documentation

Refer to the documentation at [geoarrow.github.io/geoarrow-rs/python](https://geoarrow.github.io/geoarrow-rs/python).

## Installation

```
pip install geoarrow-rust-core
```

`geoarrow-rust` is distributed with [namespace packaging](https://packaging.python.org/en/latest/guides/packaging-namespace-packages/), meaning that each python package `geoarrow-rust-[submodule-name]` (imported as `geoarrow.rust.[submodule-name]`) can be published to PyPI independently. The benefit of this approach is that _core library_ — which contains only pure-Rust code — can be precompiled for many platforms very easily. Then other submodules with C dependencies, like a future `geoarrow-rust-geos`, which will bind to GEOS for spatial operations, can be built and packaged independently.

## Future work:

- [ ] 3D coordinates. Only 2D geometries are supported at this time.
- [ ] More algorithms, including spatial indexes and spatial joins.
- [ ] CRS management. This currently loses the CRS information in the [GeoArrow metadata](https://geoarrow.org/extension-types#extension-metadata).

## Background reading

Refer to the [GeoArrow Python module proposal](https://github.com/geoarrow/geoarrow-python/issues/38) for more background information.
