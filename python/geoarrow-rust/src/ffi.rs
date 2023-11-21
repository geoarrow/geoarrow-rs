use crate::array::*;
use arrow::datatypes::Field;
use arrow::error::ArrowError;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::{make_array, ArrayRef};
use geoarrow::GeometryArrayTrait;
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyTuple};
use pyo3::{PyAny, PyResult};
use std::ffi::CString;

macro_rules! impl_arrow_c_array {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// An implementation of the Arrow PyCapsule Interface
            fn __arrow_c_array__(&self) -> PyResult<PyObject> {
                let field = self.0.extension_field();
                let ffi_schema = FFI_ArrowSchema::try_from(&*field).unwrap();
                let ffi_array = FFI_ArrowArray::new(&self.0.clone().into_array_ref().to_data());

                let schema_capsule_name = CString::new("arrow_schema").unwrap();
                let array_capsule_name = CString::new("arrow_array").unwrap();

                Python::with_gil(|py| {
                    let schema_capsule = PyCapsule::new(py, ffi_schema, Some(schema_capsule_name))?;
                    let array_capsule = PyCapsule::new(py, ffi_array, Some(array_capsule_name))?;
                    let tuple = PyTuple::new(py, vec![schema_capsule, array_capsule]);
                    Ok(tuple.to_object(py))
                })
            }
        }
    };
}

impl_arrow_c_array!(PointArray);
impl_arrow_c_array!(LineStringArray);
impl_arrow_c_array!(PolygonArray);
impl_arrow_c_array!(MultiPointArray);
impl_arrow_c_array!(MultiLineStringArray);
impl_arrow_c_array!(MultiPolygonArray);

macro_rules! impl_from_py_object {
    ($struct_name:ident) => {
        impl<'a> FromPyObject<'a> for $struct_name {
            fn extract(ob: &'a PyAny) -> PyResult<Self> {
                let (array, _field) = import_arrow_c_array(ob)?;
                Ok(Self(array.as_ref().try_into().unwrap()))
            }
        }
    };
}

impl_from_py_object!(WKBArray);
impl_from_py_object!(PointArray);
impl_from_py_object!(LineStringArray);
impl_from_py_object!(PolygonArray);
impl_from_py_object!(MultiPointArray);
impl_from_py_object!(MultiLineStringArray);
impl_from_py_object!(MultiPolygonArray);

fn to_py_err(err: ArrowError) -> PyErr {
    PyValueError::new_err(err.to_string())
}

fn validate_pycapsule(capsule: &PyCapsule, expected_name: &str) -> PyResult<()> {
    let capsule_name = capsule.name()?;
    if capsule_name.is_none() {
        return Err(PyValueError::new_err(
            "Expected schema PyCapsule to have name set.",
        ));
    }

    let capsule_name = capsule_name.unwrap().to_str()?;
    if capsule_name != expected_name {
        return Err(PyValueError::new_err(format!(
            "Expected name '{}' in PyCapsule, instead got '{}'",
            expected_name, capsule_name
        )));
    }

    Ok(())
}

/// Import __arrow_c_array__
pub(crate) fn import_arrow_c_array(ob: &PyAny) -> PyResult<(ArrayRef, Field)> {
    if !ob.hasattr("__arrow_c_array__")? {
        return Err(PyValueError::new_err(
            "Expected an object with dunder __arrow_c_array__",
        ));
    }

    let tuple = ob.getattr("__arrow_c_array__")?.call0()?;
    if !tuple.is_instance_of::<PyTuple>() {
        return Err(PyTypeError::new_err(
            "Expected __arrow_c_array__ to return a tuple.",
        ));
    }

    let schema_capsule: &PyCapsule = PyTryInto::try_into(tuple.get_item(0)?)?;
    let array_capsule: &PyCapsule = PyTryInto::try_into(tuple.get_item(1)?)?;

    validate_pycapsule(schema_capsule, "arrow_schema")?;
    validate_pycapsule(array_capsule, "arrow_array")?;

    let schema_ptr = unsafe { schema_capsule.reference::<FFI_ArrowSchema>() };
    let array = unsafe { FFI_ArrowArray::from_raw(array_capsule.pointer() as _) };

    let array_data = unsafe { arrow::ffi::from_ffi(array, schema_ptr) }.map_err(to_py_err)?;
    let field = Field::try_from(schema_ptr).map_err(to_py_err)?;
    Ok((make_array(array_data), field))
}
