use crate::array::primitive::*;
use crate::array::WKBArray;
use crate::chunked_array::primitive::*;
use crate::chunked_array::ChunkedWKBArray;
use arrow_array::Array;
use geoarrow::trait_::GeometryArrayAccessor;
use geoarrow::GeometryArrayTrait;
use numpy::ToPyArray;
use pyo3::exceptions::PyValueError;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};

macro_rules! impl_array {
    ($struct_name:ty) => {
        #[pymethods]
        impl $struct_name {
            /// An implementation of the Array interface, for interoperability with numpy and other
            /// array libraries.
            pub fn __array__(&self, py: Python) -> PyResult<PyObject> {
                if self.0.null_count() > 0 {
                    return Err(PyValueError::new_err(
                        "Cannot create numpy array from pyarrow array with nulls.",
                    ));
                }

                Ok(self.0.values().to_pyarray(py).to_object(py))
            }

            /// Copy this array to a `numpy` NDArray
            pub fn to_numpy(&self, py: Python) -> PyResult<PyObject> {
                self.__array__(py)
            }
        }
    };
}

// Needs `half` feature (but not sure if we'll ever need to use it)
// impl_array!(Float16Array);
impl_array!(Float32Array);
impl_array!(Float64Array);
impl_array!(UInt8Array);
impl_array!(UInt16Array);
impl_array!(UInt32Array);
impl_array!(UInt64Array);
impl_array!(Int8Array);
impl_array!(Int16Array);
impl_array!(Int32Array);
impl_array!(Int64Array);

#[pymethods]
impl BooleanArray {
    /// An implementation of the Array interface, for interoperability with numpy and other
    /// array libraries.
    pub fn __array__<'py>(&'py self, py: Python<'py>) -> PyResult<&'py PyAny> {
        if self.0.null_count() > 0 {
            return Err(PyValueError::new_err(
                "Cannot create numpy array from pyarrow array with nulls.",
            ));
        }

        let bools = self.0.values().iter().collect::<Vec<_>>();
        Ok(bools.to_pyarray(py))
    }

    /// Copy this array to a `numpy` NDArray
    pub fn to_numpy<'py>(&'py self, py: Python<'py>) -> PyResult<&'py PyAny> {
        self.__array__(py)
    }
}

macro_rules! impl_chunked {
    ($struct_name:ty) => {
        #[pymethods]
        impl $struct_name {
            /// An implementation of the Array interface, for interoperability with numpy and other
            /// array libraries.
            pub fn __array__<'py>(&'py self, py: Python<'py>) -> PyResult<&'py PyAny> {
                // Copy individual arrays to numpy objects, then concatenate
                let py_arrays = self
                    .0
                    .chunks()
                    .iter()
                    .map(|chunk| chunk.values().to_pyarray(py).to_object(py))
                    .collect::<Vec<_>>()
                    .to_object(py);

                let numpy_mod = py.import(intern!(py, "numpy"))?;
                numpy_mod.call_method1(intern!(py, "concatenate"), (py_arrays,))
            }

            /// Copy this array to a `numpy` NDArray
            pub fn to_numpy<'py>(&'py self, py: Python<'py>) -> PyResult<&'py PyAny> {
                self.__array__(py)
            }
        }
    };
}

// Needs `half` feature (but not sure if we'll ever need to use it)
// impl_chunked!(ChunkedFloat16Array);
impl_chunked!(ChunkedFloat32Array);
impl_chunked!(ChunkedFloat64Array);
impl_chunked!(ChunkedUInt8Array);
impl_chunked!(ChunkedUInt16Array);
impl_chunked!(ChunkedUInt32Array);
impl_chunked!(ChunkedUInt64Array);
impl_chunked!(ChunkedInt8Array);
impl_chunked!(ChunkedInt16Array);
impl_chunked!(ChunkedInt32Array);
impl_chunked!(ChunkedInt64Array);

#[pymethods]
impl ChunkedBooleanArray {
    /// An implementation of the Array interface, for interoperability with numpy and other
    /// array libraries.
    pub fn __array__<'py>(&'py self, py: Python<'py>) -> PyResult<&'py PyAny> {
        if self.0.null_count() > 0 {
            return Err(PyValueError::new_err(
                "Cannot create numpy array from pyarrow array with nulls.",
            ));
        }

        let np_chunks = self
            .0
            .chunks()
            .iter()
            .map(|chunk| Ok(BooleanArray(chunk.clone()).__array__(py)?.to_object(py)))
            .collect::<PyResult<Vec<_>>>()?;

        let numpy_mod = py.import(intern!(py, "numpy"))?;
        numpy_mod.call_method1(intern!(py, "concatenate"), (np_chunks,))
    }

    /// Copy this array to a `numpy` NDArray
    pub fn to_numpy<'py>(&'py self, py: Python<'py>) -> PyResult<&'py PyAny> {
        self.__array__(py)
    }
}
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

        let numpy_mod = py.import(intern!(py, "numpy"))?;

        let args = (self.0.len(),);
        let kwargs = PyDict::new(py);
        kwargs.set_item("dtype", numpy_mod.getattr(intern!(py, "object_"))?)?;
        let np_arr = numpy_mod.call_method(intern!(py, "empty"), args, Some(kwargs))?;

        for (i, wkb) in self.0.iter_values().enumerate() {
            np_arr.set_item(i, PyBytes::new(py, wkb.as_ref()))?;
        }

        Ok(np_arr.to_object(py))
    }
}

#[pymethods]
impl ChunkedWKBArray {
    /// An implementation of the Array interface, for interoperability with numpy and other
    /// array libraries.
    pub fn __array__(&self, py: Python) -> PyResult<PyObject> {
        let numpy_mod = py.import(intern!(py, "numpy"))?;
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
