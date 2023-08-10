use std::ffi::CString;

use crate::ffi::{from_py_array, to_py_array};
use arrow2::datatypes::Field;
use arrow2::ffi;
use geoarrow::GeometryArrayTrait;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyType};

#[repr(C)]
struct CDataCapsule {
    pub schema: *const ffi::ArrowSchema,
    pub array: *const ffi::ArrowArray,
}

unsafe impl Send for CDataCapsule {}

#[pyclass]
pub struct PointArray(pub(crate) geoarrow::array::PointArray);

#[pymethods]
impl PointArray {
    #[classmethod]
    fn from_arrow(cls: &PyType, ob: &PyAny) -> PyResult<Self> {
        ob.extract()
    }

    pub fn from_capsule(ob: &PyAny) -> PyResult<Self> {
        let cap: PyCapsule = ob.extract()?;
        cap.
        let cap = unsafe { PyCapsule::import(py, name)}

    }

    pub fn to_capsule(&self) -> PyResult<PyObject> {
        let array = self.0.clone().into_arrow();

        Python::with_gil(|py| {
            let name = CString::new("geoarrow.point").unwrap();
            let schema = Box::new(ffi::export_field_to_c(&Field::new(
                "",
                array.data_type().clone(),
                true,
            )));
            let array = Box::new(ffi::export_array_to_c(array));

            let schema_ptr: *const arrow2::ffi::ArrowSchema = &*schema;
            let array_ptr: *const arrow2::ffi::ArrowArray = &*array;

            let rust_capsule = CDataCapsule {
                schema: schema_ptr,
                array: array_ptr,
            };
            let capsule = PyCapsule::new(py, rust_capsule, Some(name)).unwrap();
            Ok(capsule.into())
        })
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
        to_py_array(py, self.0.into_boxed_arrow())
    }
}
