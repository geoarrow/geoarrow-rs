use proj::Proj;
use pyo3::prelude::*;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[pyfunction]
fn ___version() -> &'static str {
    VERSION
}

#[pyfunction]
fn tmp() {
    let from = "EPSG:2230";
    let to = "EPSG:26946";
    let nad_ft_to_m = Proj::new_known_crs(from, to, None).unwrap();
    println!("{:?}", nad_ft_to_m);
}

/// A Python module implemented in Rust.
#[pymodule]
fn _rust_proj(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_wrapped(wrap_pyfunction!(___version))?;
    m.add_wrapped(wrap_pyfunction!(tmp))?;

    Ok(())
}
