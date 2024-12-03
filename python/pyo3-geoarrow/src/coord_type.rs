use geoarrow::array::CoordType;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

#[derive(Debug, Clone, Copy)]
pub enum PyCoordType {
    Interleaved,
    Separated,
}

impl<'a> FromPyObject<'a> for PyCoordType {
    fn extract_bound(ob: &Bound<'a, PyAny>) -> PyResult<Self> {
        let s: String = ob.extract()?;
        match s.to_lowercase().as_str() {
            "interleaved" => Ok(Self::Interleaved),
            "separated" => Ok(Self::Separated),
            _ => Err(PyValueError::new_err("Unexpected coord type")),
        }
    }
}

impl From<PyCoordType> for CoordType {
    fn from(value: PyCoordType) -> Self {
        match value {
            PyCoordType::Interleaved => Self::Interleaved,
            PyCoordType::Separated => Self::Separated,
        }
    }
}
