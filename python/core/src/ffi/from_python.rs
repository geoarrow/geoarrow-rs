use crate::array::*;
use arrow::datatypes::Field;
use arrow::error::ArrowError;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::{make_array, ArrayRef};
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyTuple, PyType};
use pyo3::{PyAny, PyResult};

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
impl_from_py_object!(MixedGeometryArray);
// impl_from_py_object!(RectArray);
impl_from_py_object!(GeometryCollectionArray);

macro_rules! impl_from_arrow {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            #[classmethod]
            fn from_arrow(_cls: &PyType, ob: &PyAny) -> PyResult<Self> {
                ob.extract()
            }
        }
    };
}

impl_from_arrow!(WKBArray);
impl_from_arrow!(PointArray);
impl_from_arrow!(LineStringArray);
impl_from_arrow!(PolygonArray);
impl_from_arrow!(MultiPointArray);
impl_from_arrow!(MultiLineStringArray);
impl_from_arrow!(MultiPolygonArray);
impl_from_arrow!(MixedGeometryArray);
// impl_from_arrow!(RectArray);
impl_from_arrow!(GeometryCollectionArray);

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
