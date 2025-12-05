use geoarrow_schema::Edges;
use pyo3::exceptions::PyValueError;
use pyo3::intern;
use pyo3::prelude::*;

/// Python wrapper for GeoArrow edge interpolation type.
///
/// Specifies how edges between coordinates should be interpreted when working with
/// geodesic (spherical/ellipsoidal) geometries.
#[derive(Debug, Clone, Copy)]
pub struct PyEdges(Edges);

impl<'a, 'py> FromPyObject<'a, 'py> for PyEdges {
    type Error = PyErr;
    fn extract(ob: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        let s: String = ob.extract()?;
        match s.to_lowercase().as_str() {
            "andoyer" => Ok(Self(Edges::Andoyer)),
            "karney" => Ok(Self(Edges::Karney)),
            "spherical" => Ok(Self(Edges::Spherical)),
            "thomas" => Ok(Self(Edges::Thomas)),
            "vincenty" => Ok(Self(Edges::Vincenty)),
            _ => Err(PyValueError::new_err("Unexpected edge type")),
        }
    }
}

impl From<PyEdges> for Edges {
    fn from(value: PyEdges) -> Self {
        value.0
    }
}

impl From<Edges> for PyEdges {
    fn from(value: Edges) -> Self {
        Self(value)
    }
}

impl<'py> IntoPyObject<'py> for PyEdges {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let enums_mod = py.import(intern!(py, "geoarrow.rust.core.enums"))?;
        let enum_cls = enums_mod.getattr(intern!(py, "Edges"))?;
        match self.0 {
            Edges::Andoyer => enum_cls.getattr(intern!(py, "ANDOYER")),
            Edges::Karney => enum_cls.getattr(intern!(py, "KARNEY")),
            Edges::Spherical => enum_cls.getattr(intern!(py, "SPHERICAL")),
            Edges::Thomas => enum_cls.getattr(intern!(py, "THOMAS")),
            Edges::Vincenty => enum_cls.getattr(intern!(py, "VINCENTY")),
        }
    }
}
