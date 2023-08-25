use pyo3::prelude::*;
// pub mod algorithm;
// pub mod array;
// pub mod broadcasting;
// pub mod ffi;

/// Formats the sum of two numbers as string.
#[pyfunction]
fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
    Ok((a + b).to_string())
}

/// A Python module implemented in Rust.
#[pymodule]
fn rust(_py: Python, m: &PyModule) -> PyResult<()> {
    // m.add_class::<array::PointArray>()?;
    // m.add_class::<array::LineStringArray>()?;
    // m.add_class::<array::PolygonArray>()?;
    // m.add_class::<array::MultiPointArray>()?;
    // m.add_class::<array::MultiLineStringArray>()?;
    // m.add_class::<array::MultiPolygonArray>()?;

    m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;
    Ok(())
}
