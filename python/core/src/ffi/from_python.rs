use crate::array::*;
use crate::chunked_array::*;
use crate::table::GeoTable;
use arrow::datatypes::Field;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use arrow_array::{make_array, ArrayRef, RecordBatchReader};
use geoarrow::datatypes::GeoDataType;
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyTuple, PyType};
use pyo3::{PyAny, PyResult};

macro_rules! impl_from_py_object {
    ($struct_name:ident, $geoarrow_arr:ty) => {
        impl<'a> FromPyObject<'a> for $struct_name {
            fn extract(ob: &'a PyAny) -> PyResult<Self> {
                let (array, _field) = import_arrow_c_array(ob)?;
                let geo_array = <$geoarrow_arr>::try_from(array.as_ref())
                    .map_err(|err| PyTypeError::new_err(err.to_string()))?;
                Ok(geo_array.into())
            }
        }
    };
}

impl_from_py_object!(WKBArray, geoarrow::array::WKBArray<i32>);
impl_from_py_object!(PointArray, geoarrow::array::PointArray);
impl_from_py_object!(LineStringArray, geoarrow::array::LineStringArray<i32>);
impl_from_py_object!(PolygonArray, geoarrow::array::PolygonArray<i32>);
impl_from_py_object!(MultiPointArray, geoarrow::array::MultiPointArray<i32>);
impl_from_py_object!(
    MultiLineStringArray,
    geoarrow::array::MultiLineStringArray<i32>
);
impl_from_py_object!(MultiPolygonArray, geoarrow::array::MultiPolygonArray<i32>);
impl_from_py_object!(MixedGeometryArray, geoarrow::array::MixedGeometryArray<i32>);
// impl_from_py_object!(RectArray);
impl_from_py_object!(
    GeometryCollectionArray,
    geoarrow::array::GeometryCollectionArray<i32>
);

macro_rules! impl_from_arrow {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// Construct this object from existing Arrow data
            ///
            /// Args:
            ///     input: Arrow array to use for constructing this object
            ///
            /// Returns:
            ///     Self
            #[classmethod]
            pub fn from_arrow(_cls: &PyType, input: &PyAny) -> PyResult<Self> {
                input.extract()
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
impl_from_arrow!(GeoTable);

macro_rules! impl_from_arrow_chunks {
    ($py_chunked_array:ty, $py_array:ty, $rs_chunked_array:ty) => {
        #[pymethods]
        impl $py_chunked_array {
            /// Construct this chunked array from existing Arrow data
            ///
            /// This is a temporary workaround for [this pyarrow
            /// issue](https://github.com/apache/arrow/issues/38717), where it's currently impossible to
            /// read a pyarrow [`ChunkedArray`][pyarrow.ChunkedArray] directly without adding a direct
            /// dependency on pyarrow.
            ///
            /// Args:
            ///     input: Arrow arrays to use for constructing this object
            ///
            /// Returns:
            ///     Self
            #[classmethod]
            fn from_arrow_arrays(_cls: &PyType, input: Vec<&PyAny>) -> PyResult<Self> {
                let py_arrays = input
                    .into_iter()
                    .map(|x| x.extract())
                    .collect::<PyResult<Vec<$py_array>>>()?;
                Ok(<$rs_chunked_array>::new(
                    py_arrays.into_iter().map(|py_array| py_array.0).collect(),
                )
                .into())
            }
        }
    };
}

impl_from_arrow_chunks!(
    ChunkedPointArray,
    PointArray,
    geoarrow::chunked_array::ChunkedPointArray
);
impl_from_arrow_chunks!(
    ChunkedLineStringArray,
    LineStringArray,
    geoarrow::chunked_array::ChunkedLineStringArray<i32>
);
impl_from_arrow_chunks!(
    ChunkedPolygonArray,
    PolygonArray,
    geoarrow::chunked_array::ChunkedPolygonArray<i32>
);
impl_from_arrow_chunks!(
    ChunkedMultiPointArray,
    MultiPointArray,
    geoarrow::chunked_array::ChunkedMultiPointArray<i32>
);
impl_from_arrow_chunks!(
    ChunkedMultiLineStringArray,
    MultiLineStringArray,
    geoarrow::chunked_array::ChunkedMultiLineStringArray<i32>
);
impl_from_arrow_chunks!(
    ChunkedMultiPolygonArray,
    MultiPolygonArray,
    geoarrow::chunked_array::ChunkedMultiPolygonArray<i32>
);
impl_from_arrow_chunks!(
    ChunkedMixedGeometryArray,
    MixedGeometryArray,
    geoarrow::chunked_array::ChunkedMixedGeometryArray<i32>
);
// impl_from_arrow_chunks!(
//     ChunkedRectArray,
//     RectArray,
//     geoarrow::chunked_array::ChunkedRectArray
// );
impl_from_arrow_chunks!(
    ChunkedGeometryCollectionArray,
    GeometryCollectionArray,
    geoarrow::chunked_array::ChunkedGeometryCollectionArray<i32>
);
impl_from_arrow_chunks!(
    ChunkedWKBArray,
    WKBArray,
    geoarrow::chunked_array::ChunkedWKBArray<i32>
);

impl<'a> FromPyObject<'a> for GeoTable {
    fn extract(ob: &'a PyAny) -> PyResult<Self> {
        let stream = import_arrow_c_stream(ob)?;
        let stream_reader = ArrowArrayStreamReader::try_new(stream)
            .map_err(|err| PyValueError::new_err(err.to_string()))?;
        let schema = stream_reader.schema();

        let mut batches = vec![];
        for batch in stream_reader {
            let batch = batch.map_err(|err| PyTypeError::new_err(err.to_string()))?;
            batches.push(batch);
        }

        let table = geoarrow::table::GeoTable::from_arrow(batches, schema, None, None)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;

        if let Ok(data_type) = table.geometry_data_type() {
            match data_type {
                GeoDataType::LargeLineString(_)
                | GeoDataType::LargePolygon(_)
                | GeoDataType::LargeMultiPoint(_)
                | GeoDataType::LargeMultiLineString(_)
                | GeoDataType::LargeMultiPolygon(_)
                | GeoDataType::LargeMixed(_)
                | GeoDataType::LargeWKB
                | GeoDataType::LargeGeometryCollection(_) => return Err(PyValueError::new_err(
                    "Unable to downcast from large to small offsets. Are your offsets 2^31 long?",
                )),
                _ => (),
            }
        }

        Ok(table.into())
    }
}

fn validate_pycapsule(capsule: &PyCapsule, expected_name: &str) -> PyResult<()> {
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

    let array_data = unsafe { arrow::ffi::from_ffi(array, schema_ptr) }
        .map_err(|err| PyTypeError::new_err(err.to_string()))?;
    let field = Field::try_from(schema_ptr).map_err(|err| PyTypeError::new_err(err.to_string()))?;
    Ok((make_array(array_data), field))
}

pub(crate) fn import_arrow_c_stream(ob: &PyAny) -> PyResult<FFI_ArrowArrayStream> {
    if !ob.hasattr("__arrow_c_stream__")? {
        return Err(PyValueError::new_err(
            "Expected an object with dunder __arrow_c_stream__",
        ));
    }

    let capsule: &PyCapsule = PyTryInto::try_into(ob.getattr("__arrow_c_stream__")?.call0()?)?;
    validate_pycapsule(capsule, "arrow_array_stream")?;

    let stream = unsafe { FFI_ArrowArrayStream::from_raw(capsule.pointer() as _) };
    Ok(stream)
}

// pub(crate) fn import_arrow_c_stream_(ob: &PyAny) -> PyResult<geoarrow::table::GeoTable> {
// }

// pub(crate) fn import_arrow_c_stream_table(ob: &PyAny) -> PyResult<geoarrow::table::GeoTable> {
// }
