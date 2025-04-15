use geoarrow::ArrayBase;
use geoarrow::trait_::ArrayAccessor;
use pyo3::exceptions::PyValueError;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};

pub fn wkb_array_to_numpy(py: Python, arr: &geoarrow::array::WKBArray<i32>) -> PyResult<PyObject> {
    if arr.null_count() > 0 {
        return Err(PyValueError::new_err(
            "Cannot create numpy array from pyarrow array with nulls.",
        ));
    }

    let numpy_mod = py.import(intern!(py, "numpy"))?;

    let args = (arr.len(),);
    let kwargs = PyDict::new(py);
    kwargs.set_item("dtype", numpy_mod.getattr(intern!(py, "object_"))?)?;
    let np_arr = numpy_mod.call_method(intern!(py, "empty"), args, Some(&kwargs))?;

    for (i, wkb) in arr.iter_values().enumerate() {
        np_arr.set_item(i, PyBytes::new(py, wkb.as_ref()))?;
    }

    Ok(np_arr.into_pyobject(py)?.into_any().unbind())
}

pub fn chunked_wkb_array_to_numpy(
    py: Python,
    arr: geoarrow::chunked_array::ChunkedWKBArray<i32>,
) -> PyResult<PyObject> {
    let numpy_mod = py.import(intern!(py, "numpy"))?;
    let shapely_chunks = arr
        .chunks()
        .iter()
        .map(|chunk| wkb_array_to_numpy(py, chunk))
        .collect::<PyResult<Vec<_>>>()?;
    let np_arr = numpy_mod.call_method1(intern!(py, "concatenate"), (shapely_chunks,))?;
    Ok(np_arr.into_pyobject(py)?.into_any().unbind())
}
