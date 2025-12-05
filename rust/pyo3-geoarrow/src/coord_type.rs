use geoarrow_schema::CoordType;
use pyo3::exceptions::PyValueError;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedStr;

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

impl<'py> FromPyObject<'_, 'py> for PyCoordType {
    type Error = PyErr;

    fn extract(ob: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
        let s = ob.extract::<PyBackedStr>()?.to_lowercase();
        match s.as_str() {
            "interleaved" => Ok(Self::Interleaved),
            "separated" => Ok(Self::Separated),
            _ => Err(PyValueError::new_err(format!(
                "Unexpected coord type, should be 'interleaved' or 'separated', got {}",
                ob.repr()?
            ))),
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
