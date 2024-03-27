use std::sync::Arc;

use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow::ffi_stream::FFI_ArrowArrayStream;
use arrow_array::{make_array, ArrayRef};
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyTuple};
use pyo3::{PyAny, PyResult};

/// Validate PyCapsule has provided name
pub fn validate_pycapsule_name(capsule: &PyCapsule, expected_name: &str) -> PyResult<()> {
    let capsule_name = capsule.name()?;
    if let Some(capsule_name) = capsule_name {
        let capsule_name = capsule_name.to_str()?;
        if capsule_name != expected_name {
            return Err(PyValueError::new_err(format!(
                "Expected name '{}' in PyCapsule, instead got '{}'",
                expected_name, capsule_name
            )));
        }
    } else {
        return Err(PyValueError::new_err(
            "Expected schema PyCapsule to have name set.",
        ));
    }

    Ok(())
}

/// Import `__arrow_c_schema__` across Python boundary
pub(crate) fn import_arrow_c_schema(ob: &PyAny) -> PyResult<SchemaRef> {
    if !ob.hasattr("__arrow_c_schema__")? {
        return Err(PyValueError::new_err(
            "Expected an object with dunder __arrow_c_schema__",
        ));
    }

    let capsule: &PyCapsule = PyTryInto::try_into(ob.getattr("__arrow_c_schema__")?.call0()?)?;
    validate_pycapsule_name(capsule, "arrow_schema")?;

    let schema_ptr = unsafe { capsule.reference::<FFI_ArrowSchema>() };
    let schema =
        Schema::try_from(schema_ptr).map_err(|err| PyTypeError::new_err(err.to_string()))?;
    Ok(Arc::new(schema))
}

/// Import `__arrow_c_array__` across Python boundary
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

    validate_pycapsule_name(schema_capsule, "arrow_schema")?;
    validate_pycapsule_name(array_capsule, "arrow_array")?;

    let schema_ptr = unsafe { schema_capsule.reference::<FFI_ArrowSchema>() };
    let array = unsafe { FFI_ArrowArray::from_raw(array_capsule.pointer() as _) };

    let array_data = unsafe { arrow::ffi::from_ffi(array, schema_ptr) }
        .map_err(|err| PyTypeError::new_err(err.to_string()))?;
    let field = Field::try_from(schema_ptr).map_err(|err| PyTypeError::new_err(err.to_string()))?;
    Ok((make_array(array_data), field))
}

/// Import `__arrow_c_stream__` across Python boundary.
pub(crate) fn import_arrow_c_stream(ob: &PyAny) -> PyResult<FFI_ArrowArrayStream> {
    if !ob.hasattr("__arrow_c_stream__")? {
        return Err(PyValueError::new_err(
            "Expected an object with dunder __arrow_c_stream__",
        ));
    }

    let capsule: &PyCapsule = PyTryInto::try_into(ob.getattr("__arrow_c_stream__")?.call0()?)?;
    validate_pycapsule_name(capsule, "arrow_array_stream")?;

    let stream = unsafe { FFI_ArrowArrayStream::from_raw(capsule.pointer() as _) };
    Ok(stream)
}
