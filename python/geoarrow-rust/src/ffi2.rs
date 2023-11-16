use arrow::array::ArrayData;
use arrow::ffi;
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyTuple};
use pyo3::{PyAny, PyResult};

#[pyfunction]
pub fn read_array(ob: &'_ PyAny) {
    let arr = pyobj_to_array(ob);
}

pub fn pyobj_to_array(ob: &'_ PyAny) -> PyResult<ArrayData> {
    if ob.hasattr("__arrow_c_array__")? {
        let tuple = ob.getattr("__arrow_c_array__")?.call0()?;

        if !tuple.is_instance_of::<PyTuple>() {
            return Err(PyTypeError::new_err(
                "Expected __arrow_c_array__ to return a tuple.",
            ));
        }

        let schema_capsule = tuple.get_item(0)?;
        if !schema_capsule.is_instance_of::<PyCapsule>() {
            return Err(PyTypeError::new_err(
                "Expected __arrow_c_array__ first element to be PyCapsule.",
            ));
        }
        let schema_capsule: &PyCapsule = PyTryInto::try_into(schema_capsule)?;
        let schema_capsule_name = schema_capsule.name()?;
        if schema_capsule_name.is_none() {
            return Err(PyValueError::new_err(
                "Expected PyCapsule to have name set.",
            ));
        }
        let schema_capsule_name = schema_capsule_name.unwrap().to_str()?;
        if schema_capsule_name != "arrow_schema" {
            return Err(PyValueError::new_err(
                "Expected name 'arrow_schema' in PyCapsule.",
            ));
        }

        let array_capsule = tuple.get_item(1)?;
        if !array_capsule.is_instance_of::<PyCapsule>() {
            return Err(PyTypeError::new_err(
                "Expected __arrow_c_array__ second element to be PyCapsule.",
            ));
        }
        let array_capsule: &PyCapsule = PyTryInto::try_into(array_capsule)?;
        let array_capsule_name = array_capsule.name()?;
        if array_capsule_name.is_none() {
            return Err(PyValueError::new_err(
                "Expected PyCapsule to have name set.",
            ));
        }
        let array_capsule_name = array_capsule_name.unwrap().to_str()?;
        if array_capsule_name != "arrow_array" {
            return Err(PyValueError::new_err(
                "Expected name 'arrow_array' in PyCapsule.",
            ));
        }

        let arr = unsafe {
            ffi::from_ffi(
                *array_capsule.reference::<ffi::FFI_ArrowArray>(),
                schema_capsule.reference::<ffi::FFI_ArrowSchema>(),
            )
            .unwrap()
        };
        return Ok(arr);
    }

    Err(PyValueError::new_err(
        "Expected an object with dunder __arrow_c_array__",
    ))
}

use std::ffi::CString;

#[repr(C)]
struct Foo {
    pub val: u32,
}

fn tmp() {
    let r = Python::with_gil(|py| -> PyResult<()> {
        let foo = Foo { val: 123 };
        let name = CString::new("builtins.capsule").unwrap();

        let capsule = PyCapsule::new(py, foo, Some(name.clone()))?;

        let module = PyModule::import(py, "builtins")?;
        module.add("capsule", capsule)?;

        let cap: &Foo = unsafe { PyCapsule::import(py, name.as_ref())? };
        assert_eq!(cap.val, 123);
        Ok(())
    });
    assert!(r.is_ok());
}
