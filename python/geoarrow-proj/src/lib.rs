pub mod ffi;

use geoarrow::algorithm::proj::Reproject;
use geoarrow::array::PointArray;
use proj::Proj;
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

// This is a hacked function for now just to ensure PROJ is actually linked into the wheels, so
// that I can test wheel building correctly.
#[pyfunction]
fn reproject() {
    // let arrow2_arr = from_py_array(arr).unwrap();
    // let point_arr: PointArray = arrow2_arr.as_ref().try_into().unwrap();
    let point_arr: PointArray = vec![geo::point! { x: 2.0, y: 1.0 }].into();
    let from = "EPSG:2230";
    let to = "EPSG:26946";
    let proj = Proj::new_known_crs(from, to, None).unwrap();
    let new_arr = point_arr.reproject(&proj).unwrap();
    dbg!(new_arr);
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
    m.add_function(wrap_pyfunction!(reproject, m)?)?;
    Ok(())
}
