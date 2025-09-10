#![cfg_attr(not(test), warn(unused_crate_dependencies))]

pub(crate) mod constants;
mod udf;
mod utils;

use pyo3::exceptions::PyRuntimeWarning;
use pyo3::prelude::*;
use pyo3::types::PyTuple;
use pyo3::{intern, wrap_pymodule};

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[pyfunction]
fn ___version() -> &'static str {
    VERSION
}

/// Raise RuntimeWarning for debug builds
#[pyfunction]
fn check_debug_build(py: Python) -> PyResult<()> {
    #[cfg(debug_assertions)]
    {
        let warnings_mod = py.import(intern!(py, "warnings"))?;
        let warning = PyRuntimeWarning::new_err(
            "geodatafusion has not been compiled in release mode. Performance will be degraded.",
        );
        let args = PyTuple::new(py, vec![warning])?;
        warnings_mod.call_method1(intern!(py, "warn"), args)?;
    }
    Ok(())
}

/// A Python module implemented in Rust.
#[pymodule]
fn _rust(py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    check_debug_build(py)?;
    m.add_wrapped(wrap_pyfunction!(___version))?;

    let native_mod = wrap_pymodule!(udf::native::native)(py);
    m.add_submodule(native_mod.bind(py))?;
    py.import(intern!(py, "sys"))?
        .getattr(intern!(py, "modules"))?
        .set_item("geodatafusion.native", native_mod)?;

    let geohash_mod = wrap_pymodule!(udf::geohash::geohash)(py);
    m.add_submodule(geohash_mod.bind(py))?;
    py.import(intern!(py, "sys"))?
        .getattr(intern!(py, "modules"))?
        .set_item("geodatafusion.geohash", geohash_mod)?;

    let geo_mod = wrap_pymodule!(udf::geo::geo)(py);
    m.add_submodule(geo_mod.bind(py))?;
    py.import(intern!(py, "sys"))?
        .getattr(intern!(py, "modules"))?
        .set_item("geodatafusion.geo", geo_mod)?;

    Ok(())
}
