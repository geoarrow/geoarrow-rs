# geoarrow.rust

[![GitHub Workflow Status (Python)](https://img.shields.io/github/actions/workflow/status/geoarrow/geoarrow-rs/python.yml?branch=main)](https://github.com/geoarrow/geoarrow-rs/actions/workflows/python.yml)

Python bindings to the [GeoArrow Rust implementation](https://github.com/geoarrow/geoarrow-rs).

## Overview

This library contains Python bindings to the [GeoArrow Rust implementation](https://github.com/geoarrow/geoarrow-rs).

- **Fast**: Connects to algorithms implemented in [GeoRust](https://georust.org/), which compile to native code.
- **Parallel**: Multi-threading is enabled out-of-the-box for all operations on chunked data structures.
- **Self-contained**: `pyproj` is the only Python dependency.
- **Easy to install**: Distributed as static binary wheels with zero C dependencies.
- **Static typing**: type hints for all operations.
- **Interoperable ecosystem**: Efficient data exchange with other libraries in the Python Arrow and GeoArrow ecosystems. , such as [geoarrow-c](https://github.com/geoarrow/geoarrow-c/tree/main/python) or [lightning-fast map rendering](https://github.com/developmentseed/lonboard).

## Documentation

Refer to the [documentation website](https://geoarrow.org/geoarrow-rs/python).

## Modules

`geoarrow.rust` is distributed with [namespace packaging](https://packaging.python.org/en/latest/guides/packaging-namespace-packages/), meaning that each Python submodule is published to PyPI independently. This allows for separation of concerns and smaller environments when only some portion of functionality is necessary.

Existing modules:

- [`geoarrow-rust-core`](./geoarrow-core/README.md): Data structures to store and manage geometry data in GeoArrow format.
- [`geoarrow-rust-compute`](./geoarrow-compute/README.md): Compute operations on GeoArrow data.
- [`geoarrow-rust-io`](./geoarrow-io/README.md): Pure-rust readers and writers for geospatial file formats.

In order to obtain relevant modules, you should install them from PyPI directly, e.g.:

```
pip install geoarrow-rust-core geoarrow-rust-compute geoarrow-rust-io
```

Future potential modules:

- `geoarrow-rust-geos`: [GEOS](https://libgeos.org/)-based algorithms on GeoArrow memory.
- `geoarrow-rust-proj`: [PROJ](https://proj.org/en/9.3/)-based coordinate reprojection on GeoArrow memory.

See [DEVELOP.md](DEVELOP.md) for more information on developing and building the Python bindings.
