use geoarrow_schema::Dimension;
use pyo3::exceptions::PyValueError;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedStr;

/// Python wrapper for GeoArrow coordinate dimension.
///
/// Specifies the dimensionality of coordinates:
/// - `XY`: 2D coordinates (x, y)
/// - `XYZ`: 3D coordinates (x, y, z)
/// - `XYM`: 2D coordinates with measure (x, y, m)
/// - `XYZM`: 3D coordinates with measure (x, y, z, m)
#[derive(Debug, Clone, Copy)]
#[allow(clippy::upper_case_acronyms)]
#[allow(missing_docs)]
pub enum PyDimension {
    XY,
    XYZ,
    XYM,
    XYZM,
}

impl<'py> FromPyObject<'_, 'py> for PyDimension {
    type Error = PyErr;

    fn extract(ob: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
        let s = ob.extract::<PyBackedStr>()?.to_lowercase();
        match s.as_str() {
            "xy" => Ok(Self::XY),
            "xyz" => Ok(Self::XYZ),
            "xym" => Ok(Self::XYM),
            "xyzm" => Ok(Self::XYZM),
            _ => Err(PyValueError::new_err(format!(
                "Unexpected dimension, got {}",
                ob.repr()?
            ))),
        }
    }
}

impl From<PyDimension> for Dimension {
    fn from(value: PyDimension) -> Self {
        match value {
            PyDimension::XY => Self::XY,
            PyDimension::XYZ => Self::XYZ,
            PyDimension::XYM => Self::XYM,
            PyDimension::XYZM => Self::XYZM,
        }
    }
}

impl From<Dimension> for PyDimension {
    fn from(value: Dimension) -> Self {
        match value {
            Dimension::XY => Self::XY,
            Dimension::XYZ => Self::XYZ,
            Dimension::XYM => Self::XYM,
            Dimension::XYZM => Self::XYZM,
        }
    }
}

impl<'py> IntoPyObject<'py> for PyDimension {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let enums_mod = py.import(intern!(py, "geoarrow.rust.core.enums"))?;
        let enum_cls = enums_mod.getattr(intern!(py, "Dimension"))?;
        match self {
            Self::XY => enum_cls.getattr(intern!(py, "XY")),
            Self::XYZ => enum_cls.getattr(intern!(py, "XYZ")),
            Self::XYM => enum_cls.getattr(intern!(py, "XYM")),
            Self::XYZM => enum_cls.getattr(intern!(py, "XYZM")),
        }
    }
}
