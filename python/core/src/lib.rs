use pyo3::prelude::*;
pub mod algorithm;
pub mod array;
pub mod broadcasting;
pub mod ffi;
pub mod io;

/// A Python module implemented in Rust.
#[pymodule]
fn rust(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<array::PointArray>()?;
    m.add_class::<array::LineStringArray>()?;
    m.add_class::<array::PolygonArray>()?;
    m.add_class::<array::MultiPointArray>()?;
    m.add_class::<array::MultiLineStringArray>()?;
    m.add_class::<array::MultiPolygonArray>()?;
    m.add_class::<array::WKBArray>()?;

    // Primitive arrays
    m.add_class::<array::BooleanArray>()?;
    m.add_class::<array::Float16Array>()?;
    m.add_class::<array::Float32Array>()?;
    m.add_class::<array::Float64Array>()?;
    m.add_class::<array::Int16Array>()?;
    m.add_class::<array::Int32Array>()?;
    m.add_class::<array::Int64Array>()?;
    m.add_class::<array::Int8Array>()?;
    m.add_class::<array::LargeStringArray>()?;
    m.add_class::<array::StringArray>()?;
    m.add_class::<array::UInt16Array>()?;
    m.add_class::<array::UInt32Array>()?;
    m.add_class::<array::UInt64Array>()?;
    m.add_class::<array::UInt8Array>()?;

    // Top-level functions
    m.add_function(wrap_pyfunction!(crate::algorithm::geo::area::area, m)?)?;

    // IO
    m.add_function(wrap_pyfunction!(crate::io::wkb::to_wkb, m)?)?;
    m.add_function(wrap_pyfunction!(crate::io::wkb::from_wkb, m)?)?;

    Ok(())
}
