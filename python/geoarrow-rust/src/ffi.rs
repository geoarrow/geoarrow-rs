use arrow::array::ArrayData;
use arrow::ffi::{self, copy_ffi_array};
use arrow_array::ArrayRef;
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyTuple};
use pyo3::{ffi::Py_uintptr_t, PyAny, PyObject, PyResult};

// #[derive(FromPyObject)]
struct PyCapsuleArray {
    schema: PyCapsule,
    array: PyCapsule,
}

#[pyfunction]
pub fn read_array(ob: &'_ PyAny) -> PyResult<()> {
    let arr = pyobj_to_array(ob)?;
    println!("{:?}", arr);
    Ok(())
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
        let array_ptr = array_capsule.pointer();

        let array_ptr = array_ptr as *mut ffi::FFI_ArrowArray;
        let owned_array_ptr = unsafe { array_ptr.as_mut().unwrap().copy() };
        owned_array_ptr.

        let schema_ptr = unsafe { schema_capsule.reference::<ffi::FFI_ArrowSchema>() };
        let meta = schema_ptr.metadata().unwrap();
        println!("Metadata: {:?}", meta);

        unsafe {
            println!(
                "is original released: {}",
                array_ptr.as_mut().unwrap().is_released()
            );
        };

        let arr = ffi::from_ffi(owned_array_ptr, schema_ptr).unwrap();
        return Ok(arr);
    }

    Err(PyValueError::new_err(
        "Expected an object with dunder __arrow_c_array__",
    ))
}

impl FromPyObject<'_> for PyCapsuleArray {
    fn extract(ob: &'_ PyAny) -> PyResult<Self> {
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

            let cap: Py<PyCapsule> = array_capsule.into();

            // array_capsule.

            // return Ok(Self {
            //     schema: schema_capsule,
            //     array: array_capsule,
            // });

            // let schema_ptr = schema_capsule.pointer();
            // let array_ptr = array_capsule.pointer();

            // let schema_ptr = schema_ptr as *const ffi::FFI_ArrowSchema;
            // let array_ptr = array_ptr as *const ffi::FFI_ArrowArray;

            // // PyCapsule::import(py, name)

            // // let x = unsafe { &*schema_ptr
            // // };
            // // let x = unsafe {
            // //     *array_ptr
            // // };
            // let test = unsafe {
            //     ffi::from_ffi(*array_ptr, &*schema_ptr)
            // };

            // todo!()

            // // let (array_ptr, schema_ptr) = unsafe {
            // //     (*array_ptr, *schema_ptr)
            // // };
            // // let x = *schema_ptr;

            // // ffi::from_ffi(array_ptr, &schema_ptr);

            // // let
        }

        return Err(PyValueError::new_err(
            "Expected an object with dunder __arrow_c_array__",
        ));
    }
}

impl ToPyObject for PyCapsuleArray {
    fn to_object(&self, py: Python<'_>) -> PyObject {
        let tuple: &PyTuple = PyTuple::new(
            py,
            vec![self.schema.to_object(py), self.array.to_object(py)],
        );
        tuple.to_object(py)
    }
}

// #[derive(FromPyObject)]
struct PyCapsuleSchema(PyCapsule);

/// Take an arrow array from python and convert it to a rust arrow array.
/// This operation does not copy data.
pub fn from_py_array(pyobj: &PyAny) -> PyResult<ArrayRef> {
    // let pyarray_capsule: PyCapsuleArray = pyobj.getattr("__arrow_c_array__")?.call0()?.into();
    todo!()
    // py_arrow_array.

    // // prepare a pointer to receive the Array struct
    // let array = Box::new(ffi::ArrowArray::empty());
    // let schema = Box::new(ffi::ArrowSchema::empty());

    // let array_ptr = &*array as *const ffi::ArrowArray;
    // let schema_ptr = &*schema as *const ffi::ArrowSchema;

    // // make the conversion through PyArrow's private API
    // // this changes the pointer's memory and is thus unsafe. In particular, `_export_to_c` can go out of bounds
    // arrow_array.call_method1(
    //     "_export_to_c",
    //     (array_ptr as Py_uintptr_t, schema_ptr as Py_uintptr_t),
    // )?;

    // unsafe {
    //     let field = ffi::import_field_from_c(schema.as_ref()).unwrap();
    //     let array = ffi::import_array_from_c(*array, field.data_type).unwrap();
    //     Ok(array)
    // }
}

/// Arrow array to Python.
pub fn to_py_array(py: Python, array: ArrayRef) -> PyResult<PyObject> {
    todo!()
    // let schema = Box::new(ffi::export_field_to_c(&Field::new(
    //     "",
    //     array.data_type().clone(),
    //     true,
    // )));
    // let array = Box::new(ffi::export_array_to_c(array));

    // let schema_ptr: *const arrow2::ffi::ArrowSchema = &*schema;
    // let array_ptr: *const arrow2::ffi::ArrowArray = &*array;

    // let pa = py.import("pyarrow")?;

    // let array = pa.getattr("Array")?.call_method1(
    //     "_import_from_c",
    //     (array_ptr as Py_uintptr_t, schema_ptr as Py_uintptr_t),
    // )?;

    // Ok(array.to_object(py))
}
