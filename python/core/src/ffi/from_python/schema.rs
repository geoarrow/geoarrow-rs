use crate::ffi::from_python::utils::import_arrow_c_schema;
use crate::schema::Schema;
use pyo3::prelude::*;
use pyo3::{PyAny, PyResult};

impl<'a> FromPyObject<'a> for Schema {
    fn extract_bound(ob: &Bound<'a, PyAny>) -> PyResult<Self> {
        let schema = import_arrow_c_schema(ob)?;
        Ok(Self(schema))
    }
}
