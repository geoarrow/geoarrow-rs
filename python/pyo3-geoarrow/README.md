# pyo3-geoarrow

<!-- [![crates.io version][crates.io_badge]][crates.io_link]
[![docs.rs docs][docs.rs_badge]][docs.rs_link]

[crates.io_badge]: https://img.shields.io/crates/v/pyo3-geoarrow.svg
[crates.io_link]: https://crates.io/crates/pyo3-geoarrow
[docs.rs_badge]: https://docs.rs/pyo3-geoarrow/badge.svg
[docs.rs_link]: https://docs.rs/pyo3-geoarrow -->

Lightweight [GeoArrow](https://geoarrow.org/) integration for [pyo3](https://pyo3.rs/). Designed to make it easier for Rust libraries to add interoperable, zero-copy geospatial Python bindings.

Specifically, pyo3-geoarrow implements zero-copy FFI conversions between Python objects and Rust representations using the `geoarrow` crate. This relies heavily on the [Arrow PyCapsule Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html) for seamless interoperability across the Python Arrow ecosystem.

