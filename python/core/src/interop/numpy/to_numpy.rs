use crate::array::WKBArray;
use crate::chunked_array::ChunkedWKBArray;
use geoarrow::trait_::GeometryArrayAccessor;
use geoarrow::GeometryArrayTrait;
use pyo3::exceptions::PyValueError;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};

#[pymethods]
impl WKBArray {
    /// An implementation of the Array interface, for interoperability with numpy and other
    /// array libraries.
    pub fn __array__(&self, py: Python) -> PyResult<PyObject> {
        if self.0.null_count() > 0 {
            return Err(PyValueError::new_err(
                "Cannot create numpy array from pyarrow array with nulls.",
            ));
        }

        let numpy_mod = py.import_bound(intern!(py, "numpy"))?;

        let args = (self.0.len(),);
        let kwargs = PyDict::new_bound(py);
        kwargs.set_item("dtype", numpy_mod.getattr(intern!(py, "object_"))?)?;
        let np_arr = numpy_mod.call_method(intern!(py, "empty"), args, Some(&kwargs))?;

        for (i, wkb) in self.0.iter_values().enumerate() {
            np_arr.set_item(i, PyBytes::new_bound(py, wkb.as_ref()))?;
        }

        Ok(np_arr.to_object(py))
    }
}

#[pymethods]
impl ChunkedWKBArray {
    /// An implementation of the Array interface, for interoperability with numpy and other
    /// array libraries.
    pub fn __array__(&self, py: Python) -> PyResult<PyObject> {
        let numpy_mod = py.import_bound(intern!(py, "numpy"))?;
        let shapely_chunks = self
            .0
            .chunks()
            .iter()
            .map(|chunk| Ok(WKBArray(chunk.clone()).__array__(py)?.to_object(py)))
            .collect::<PyResult<Vec<_>>>()?;
        let np_arr = numpy_mod.call_method1(intern!(py, "concatenate"), (shapely_chunks,))?;
        Ok(np_arr.to_object(py))
    }
}
