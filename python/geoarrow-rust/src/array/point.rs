use std::ffi::{c_void, CString};

use crate::ffi::{from_py_array, to_py_array};
use arrow::ffi::{self, FFI_ArrowArray, FFI_ArrowSchema};
use geoarrow::GeometryArrayTrait;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyTuple, PyType};

#[pyclass]
pub struct PointArray(pub(crate) geoarrow::array::PointArray);

fn pycapsule_array_destructor(ffi_array: FFI_ArrowArray, _capsule_context: *mut c_void) {
    drop(ffi_array)
}

#[pymethods]
impl PointArray {
    #[classmethod]
    fn from_arrow(_cls: &PyType, ob: &PyAny) -> PyResult<Self> {
        ob.extract()
    }

    fn __arrow_c_array__(&self) -> PyResult<PyObject> {
        let field = self.0.extension_field();
        let ffi_schema = FFI_ArrowSchema::try_from(&*field).unwrap();
        let ffi_array = FFI_ArrowArray::new(&self.0.clone().into_array_ref().to_data());
        // ffi_schema.

        let schema_capsule_name = CString::new("arrow_schema").unwrap();
        let array_capsule_name = CString::new("arrow_array").unwrap();
        Python::with_gil(|py| {
            // let schema_capsule = PyCapsule::new(py, ffi_array, Some(schema_capsule_name));
            let capsule = PyCapsule::new(
                py,
                ffi_schema,
                Some(schema_capsule_name),
            )
            .unwrap();
        });
                // pycapsule_schema_destructor,

        // let schema_capsule = PyCapsule::new(py)

        // PyCapsule::n
        // Python::with_gil(|py| {
        //     let name = CString::new("foo").unwrap();
        //     let capsule = PyCapsule::new(py, 123_u32, Some(name)).unwrap();
        //     let val = unsafe { capsule.reference::<u32>() };
        //     assert_eq!(*val, 123);
        // });

        // self.0.
        // ffi::to_ffi(data)
        todo!()
    }
}

impl From<geoarrow::array::PointArray> for PointArray {
    fn from(value: geoarrow::array::PointArray) -> Self {
        Self(value)
    }
}

impl From<PointArray> for geoarrow::array::PointArray {
    fn from(value: PointArray) -> Self {
        value.0
    }
}

impl<'a> FromPyObject<'a> for PointArray {
    fn extract(ob: &'a PyAny) -> PyResult<Self> {
        let arrow2_arr = from_py_array(ob)?;
        Ok(PointArray(arrow2_arr.as_ref().try_into().unwrap()))
    }
}

impl IntoPy<PyResult<PyObject>> for PointArray {
    fn into_py(self, py: Python<'_>) -> PyResult<PyObject> {
        to_py_array(py, self.0.into_array_ref())
    }
}
