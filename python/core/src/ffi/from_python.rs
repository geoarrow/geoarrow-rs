use std::sync::Arc;

use crate::array::*;
use arrow::datatypes::{DataType, Field};
use arrow::error::ArrowError;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::{make_array, ArrayRef};
use geoarrow::error::GeoArrowError;
use geoarrow::GeometryArrayTrait;
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyTuple};
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

pub fn convert_to_geometry_array(
    array: &ArrayRef,
    field: &Field,
) -> Result<Arc<dyn GeometryArrayTrait>, GeoArrowError> {
    if let Some(extension_name) = field.metadata().get("ARROW:extension:name") {
        let geom_arr: Arc<dyn GeometryArrayTrait> = match extension_name.as_str() {
            "geoarrow.point" => {
                Arc::new(geoarrow::array::PointArray::try_from(array.as_ref()).unwrap())
            }
            "geoarrow.linestring" => match field.data_type() {
                DataType::List(_) => Arc::new(
                    geoarrow::array::LineStringArray::<i32>::try_from(array.as_ref()).unwrap(),
                ),
                DataType::LargeList(_) => Arc::new(
                    geoarrow::array::LineStringArray::<i64>::try_from(array.as_ref()).unwrap(),
                ),
                _ => panic!("Unexpected data type"),
            },
            "geoarrow.polygon" => match field.data_type() {
                DataType::List(_) => Arc::new(
                    geoarrow::array::PolygonArray::<i32>::try_from(array.as_ref()).unwrap(),
                ),
                DataType::LargeList(_) => Arc::new(
                    geoarrow::array::PolygonArray::<i64>::try_from(array.as_ref()).unwrap(),
                ),
                _ => panic!("Unexpected data type"),
            },
            "geoarrow.multipoint" => match field.data_type() {
                DataType::List(_) => Arc::new(
                    geoarrow::array::MultiPointArray::<i32>::try_from(array.as_ref()).unwrap(),
                ),
                DataType::LargeList(_) => Arc::new(
                    geoarrow::array::MultiPointArray::<i64>::try_from(array.as_ref()).unwrap(),
                ),
                _ => panic!("Unexpected data type"),
            },
            "geoarrow.multilinestring" => match field.data_type() {
                DataType::List(_) => Arc::new(
                    geoarrow::array::MultiLineStringArray::<i32>::try_from(array.as_ref()).unwrap(),
                ),
                DataType::LargeList(_) => Arc::new(
                    geoarrow::array::MultiLineStringArray::<i64>::try_from(array.as_ref()).unwrap(),
                ),
                _ => panic!("Unexpected data type"),
            },
            "geoarrow.multipolygon" => match field.data_type() {
                DataType::List(_) => Arc::new(
                    geoarrow::array::MultiPolygonArray::<i32>::try_from(array.as_ref()).unwrap(),
                ),
                DataType::LargeList(_) => Arc::new(
                    geoarrow::array::MultiPolygonArray::<i64>::try_from(array.as_ref()).unwrap(),
                ),
                _ => panic!("Unexpected data type"),
            },
            "geoarrow.wkb" => match field.data_type() {
                DataType::Binary => {
                    Arc::new(geoarrow::array::WKBArray::<i32>::try_from(array.as_ref()).unwrap())
                }
                DataType::LargeBinary => {
                    Arc::new(geoarrow::array::WKBArray::<i64>::try_from(array.as_ref()).unwrap())
                }
                _ => panic!("Unexpected data type"),
            },
            _ => {
                return Err(GeoArrowError::General(format!(
                    "Unknown geoarrow type {}",
                    extension_name
                )))
            }
        };
        Ok(geom_arr)
    } else {
        match field.data_type() {
            DataType::Binary => {
                Ok(Arc::new(geoarrow::array::WKBArray::<i32>::try_from(array.as_ref()).unwrap()))
            }
            DataType::LargeBinary => {
                Ok(Arc::new(geoarrow::array::WKBArray::<i64>::try_from(array.as_ref()).unwrap()))
            }
            _ => Err(GeoArrowError::General("Only Binary, LargeBinary, FixedSizeList, and Struct arrays are unambigously typed and can be used without extension metadata.".to_string()))
        }
    }
}
