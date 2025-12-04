use geoarrow_schema::CoordType;
use pyo3::exceptions::PyValueError;
use pyo3::intern;
use pyo3::prelude::*;

/// Python wrapper for GeoArrow coordinate type.
///
/// Specifies whether coordinates are stored in an interleaved (XYZXYZ...) or
/// separated (XXX..., YYY..., ZZZ...) layout in memory.
#[derive(Debug, Default, Clone, Copy)]
pub enum PyCoordType {
    /// Interleaved coordinate layout (XYZXYZ...).
    Interleaved,
    /// Separated coordinate layout (XXX..., YYY..., ZZZ...).
    #[default]
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

impl From<CoordType> for PyCoordType {
    fn from(value: CoordType) -> Self {
        match value {
            CoordType::Interleaved => Self::Interleaved,
            CoordType::Separated => Self::Separated,
        }
    }
}

impl<'py> IntoPyObject<'py> for PyCoordType {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let enums_mod = py.import(intern!(py, "geoarrow.rust.core.enums"))?;
        let enum_cls = enums_mod.getattr(intern!(py, "CoordType"))?;
        match self {
            Self::Interleaved => enum_cls.getattr(intern!(py, "INTERLEAVED")),
            Self::Separated => enum_cls.getattr(intern!(py, "SEPARATED")),
        }
    }
}
