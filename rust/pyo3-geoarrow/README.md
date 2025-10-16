# pyo3-geoarrow

[PyO3](https://pyo3.rs/) bindings for GeoArrow types, enabling seamless integration between Rust's GeoArrow implementation and Python.

This crate provides Python-compatible wrappers around GeoArrow data structures, allowing Python code to efficiently work with and share geospatial data in the GeoArrow format through the [Arrow C Data Interface][arrow-c-data-interface] (using the [Arrow PyCapsule Interface][arrow-pycapsule-interface]) without data copies.

[arrow-c-data-interface]: https://arrow.apache.org/docs/format/CDataInterface.html
[arrow-pycapsule-interface]: https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html

## Features

- **Zero-copy data exchange**: Use Arrow's [C Data Interface][arrow-c-data-interface] for efficient memory sharing between Rust and Python.
- **GeoArrow types**: Python bindings for GeoArrow geometry arrays, scalars, and metadata.
- **Type safety**: Strongly-typed wrappers that preserve GeoArrow's type system in Python.
- **FFI support**: Import and export GeoArrow data to/from Python using the [Arrow PyCapsule Interface][arrow-pycapsule-interface].

## Core Types

- [`PyGeoArray`]: Python wrapper for GeoArrow geometry arrays
- [`PyGeoChunkedArray`]: Python wrapper for chunked GeoArrow geometry arrays
- [`PyGeoArrayReader`]: Python wrapper for streaming array readers
- [`PyGeoScalar`]: Python wrapper for GeoArrow scalar geometries
- [`PyGeoType`]: Python wrapper for GeoArrow data types
- [`PyCrs`]: Python wrapper for coordinate reference system representation

## Usage

This crate is primarily intended for use by Python binding developers who need to interoperate with GeoArrow data in Python. It is also used internally by the `geoarrow-rust-*` Python packages.

```rust
use pyo3::prelude::*;
use pyo3_geoarrow::PyGeoArray;
use geoarrow_array::GeoArrowArray;

#[pyfunction]
fn process_geometry(py: Python, array: PyGeoArray) -> PyResult<PyGeoArray> {
    // Access the underlying GeoArrow array
    let inner: &dyn GeoArrowArray = array.inner();

    // Perform operations...

    Ok(PyGeoArray::new(result))
}
```

## Integration with Arrow

This crate implements the [Arrow PyCapsule Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html), allowing GeoArrow objects to be exchanged with any Python library that supports Arrow, including:

- PyArrow
- Polars (once they support extension types)
- GeoPandas (via PyArrow)
- DuckDB

## Dependencies

This crate builds on:

- [`pyo3`](https://docs.rs/pyo3): Python bindings for Rust
- [`pyo3-arrow`](https://docs.rs/pyo3-arrow): Arrow integration for PyO3
- [`geoarrow-array`](https://docs.rs/geoarrow-array): Core GeoArrow array types
- [`geoarrow-schema`](https://docs.rs/geoarrow-schema): GeoArrow type system and metadata
