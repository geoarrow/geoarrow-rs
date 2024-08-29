use std::sync::Arc;

use crate::array::*;
use crate::error::PyGeoArrowResult;
use crate::interop::shapely::utils::import_shapely;
use arrow_array::OffsetSizeTrait;
use arrow_buffer::NullBuffer;
use geoarrow::array::{from_arrow_array, AsGeometryArray, CoordBuffer};
use geoarrow::datatypes::{Dimension, GeoDataType};
use geoarrow::io::wkb::to_wkb;
use geoarrow::GeometryArrayTrait;
use numpy::PyArrayMethods;
use numpy::ToPyArray;
use pyo3::exceptions::PyValueError;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::PyTuple;
use pyo3::PyAny;
use pyo3_arrow::input::AnyArray;
use pyo3_arrow::PyArray;

const NULL_VALUES_ERR_MSG: &str = "Cannot convert GeoArrow array with null values to Shapely";

/// Check that the array has no null values
fn check_nulls(nulls: Option<&NullBuffer>) -> PyGeoArrowResult<()> {
    if nulls.is_some_and(|x| x.null_count() > 0) {
        Err(PyValueError::new_err(NULL_VALUES_ERR_MSG).into())
    } else {
        Ok(())
    }
}

/// Copy a CoordBuffer to a numpy array of shape `(length, D)`
fn coords_to_numpy<const D: usize>(
    py: Python,
    coords: &CoordBuffer<D>,
) -> PyGeoArrowResult<PyObject> {
    match coords {
        CoordBuffer::Interleaved(cb) => {
            let scalar_buffer = cb.coords();
            let numpy_coords = scalar_buffer
                .to_pyarray_bound(py)
                .reshape([scalar_buffer.len() / D, D])?;

            Ok(numpy_coords.to_object(py))
        }
        CoordBuffer::Separated(cb) => {
            let buffers = cb.coords();
            let numpy_buffers = buffers
                .iter()
                .map(|buf| buf.to_pyarray_bound(py).to_object(py))
                .collect::<Vec<_>>();

            let numpy_mod = py.import_bound(intern!(py, "numpy"))?;
            Ok(numpy_mod
                .call_method1(
                    intern!(py, "column_stack"),
                    PyTuple::new_bound(py, numpy_buffers),
                )?
                .into_py(py))
        }
    }
}

/// Convert a GeoArrow array to a numpy array of Shapely objects
///
/// Args:
///     input: input geometry array
///
/// Returns:
///     numpy array with Shapely objects
#[pyfunction]
pub fn to_shapely(py: Python, input: AnyArray) -> PyGeoArrowResult<Bound<PyAny>> {
    match input {
        AnyArray::Array(arr) => pyarray_to_shapely(py, arr),
        AnyArray::Stream(stream) => {
            let field = stream.field_ref()?;
            let mut shapely_chunks = vec![];
            for chunk in stream.into_reader()? {
                let py_array = PyArray::new(chunk?, field.clone());
                shapely_chunks.push(pyarray_to_shapely(py, py_array)?);
            }

            let numpy_mod = py.import_bound(intern!(py, "numpy"))?;
            Ok(numpy_mod.call_method1(intern!(py, "concatenate"), (shapely_chunks,))?)
        }
    }
}

fn pyarray_to_shapely(py: Python, input: PyArray) -> PyGeoArrowResult<Bound<PyAny>> {
    let (array, field) = input.into_inner();
    check_nulls(array.nulls())?;

    let array = from_arrow_array(&array, &field)?;

    use Dimension::*;
    use GeoDataType::*;
    match array.data_type() {
        Point(_, XY) => point_arr(py, array.as_ref().as_point_2d().clone()),
        LineString(_, XY) => linestring_arr(py, array.as_ref().as_line_string_2d().clone()),
        Polygon(_, XY) => polygon_arr(py, array.as_ref().as_polygon_2d().clone()),
        MultiPoint(_, XY) => multipoint_arr(py, array.as_ref().as_multi_point_2d().clone()),
        MultiLineString(_, XY) => {
            multilinestring_arr(py, array.as_ref().as_multi_line_string_2d().clone())
        }
        MultiPolygon(_, XY) => multipolygon_arr(py, array.as_ref().as_multi_polygon_2d().clone()),
        Mixed(_, XY) => via_wkb(py, array),
        GeometryCollection(_, XY) => via_wkb(py, array),
        WKB => wkb_arr(py, array.as_ref().as_wkb().clone()),
        t => Err(PyValueError::new_err(format!("unexpected type {:?}", t)).into()),
    }
}

fn point_arr<const D: usize>(
    py: Python,
    arr: geoarrow::array::PointArray<D>,
) -> PyGeoArrowResult<Bound<PyAny>> {
    let shapely_mod = import_shapely(py)?;
    let shapely_geom_type_enum = shapely_mod.getattr(intern!(py, "GeometryType"))?;

    let coords = coords_to_numpy(py, arr.coords())?;
    let args = (
        shapely_geom_type_enum.getattr(intern!(py, "POINT"))?,
        coords,
    );
    Ok(shapely_mod.call_method1(intern!(py, "from_ragged_array"), args)?)
}

fn linestring_arr<O: OffsetSizeTrait + numpy::Element, const D: usize>(
    py: Python,
    arr: geoarrow::array::LineStringArray<O, D>,
) -> PyGeoArrowResult<Bound<PyAny>> {
    let shapely_mod = import_shapely(py)?;
    let shapely_geom_type_enum = shapely_mod.getattr(intern!(py, "GeometryType"))?;

    let coords = coords_to_numpy(py, arr.coords())?;
    let offsets = (arr.geom_offsets().to_pyarray_bound(py),);

    let args = (
        shapely_geom_type_enum.getattr(intern!(py, "LINESTRING"))?,
        coords,
        offsets,
    );
    Ok(shapely_mod.call_method1(intern!(py, "from_ragged_array"), args)?)
}

fn polygon_arr<O: OffsetSizeTrait + numpy::Element, const D: usize>(
    py: Python,
    arr: geoarrow::array::PolygonArray<O, D>,
) -> PyGeoArrowResult<Bound<PyAny>> {
    let shapely_mod = import_shapely(py)?;
    let shapely_geom_type_enum = shapely_mod.getattr(intern!(py, "GeometryType"))?;

    let coords = coords_to_numpy(py, arr.coords())?;
    let offsets = (
        arr.ring_offsets().to_pyarray_bound(py),
        arr.geom_offsets().to_pyarray_bound(py),
    );

    let args = (
        shapely_geom_type_enum.getattr(intern!(py, "POLYGON"))?,
        coords,
        offsets,
    );
    Ok(shapely_mod.call_method1(intern!(py, "from_ragged_array"), args)?)
}

fn multipoint_arr<O: OffsetSizeTrait + numpy::Element, const D: usize>(
    py: Python,
    arr: geoarrow::array::MultiPointArray<O, D>,
) -> PyGeoArrowResult<Bound<PyAny>> {
    let shapely_mod = import_shapely(py)?;
    let shapely_geom_type_enum = shapely_mod.getattr(intern!(py, "GeometryType"))?;

    let coords = coords_to_numpy(py, arr.coords())?;
    let offsets = (arr.geom_offsets().to_pyarray_bound(py),);

    let args = (
        shapely_geom_type_enum.getattr(intern!(py, "MULTIPOINT"))?,
        coords,
        offsets,
    );
    Ok(shapely_mod.call_method1(intern!(py, "from_ragged_array"), args)?)
}

fn multilinestring_arr<O: OffsetSizeTrait + numpy::Element, const D: usize>(
    py: Python,
    arr: geoarrow::array::MultiLineStringArray<O, D>,
) -> PyGeoArrowResult<Bound<PyAny>> {
    let shapely_mod = import_shapely(py)?;
    let shapely_geom_type_enum = shapely_mod.getattr(intern!(py, "GeometryType"))?;

    let coords = coords_to_numpy(py, arr.coords())?;
    let offsets = (
        arr.ring_offsets().to_pyarray_bound(py),
        arr.geom_offsets().to_pyarray_bound(py),
    );

    let args = (
        shapely_geom_type_enum.getattr(intern!(py, "MULTILINESTRING"))?,
        coords,
        offsets,
    );
    Ok(shapely_mod.call_method1(intern!(py, "from_ragged_array"), args)?)
}

fn multipolygon_arr<O: OffsetSizeTrait + numpy::Element, const D: usize>(
    py: Python,
    arr: geoarrow::array::MultiPolygonArray<O, D>,
) -> PyGeoArrowResult<Bound<PyAny>> {
    let shapely_mod = import_shapely(py)?;
    let shapely_geom_type_enum = shapely_mod.getattr(intern!(py, "GeometryType"))?;

    let coords = coords_to_numpy(py, arr.coords())?;
    let offsets = (
        arr.ring_offsets().to_pyarray_bound(py),
        arr.polygon_offsets().to_pyarray_bound(py),
        arr.geom_offsets().to_pyarray_bound(py),
    );

    let args = (
        shapely_geom_type_enum.getattr(intern!(py, "MULTIPOLYGON"))?,
        coords,
        offsets,
    );
    Ok(shapely_mod.call_method1(intern!(py, "from_ragged_array"), args)?)
}

fn via_wkb(py: Python, arr: Arc<dyn GeometryArrayTrait>) -> PyGeoArrowResult<Bound<PyAny>> {
    wkb_arr(py, to_wkb(arr.as_ref()))
}

fn wkb_arr(py: Python, arr: geoarrow::array::WKBArray<i32>) -> PyGeoArrowResult<Bound<PyAny>> {
    let shapely_mod = import_shapely(py)?;
    let wkb_array = WKBArray(arr);
    Ok(shapely_mod.call_method1(intern!(py, "from_wkb"), (wkb_array.__array__(py)?,))?)
}

#[pymethods]
impl WKBArray {
    /// Convert this array to a shapely array
    ///
    /// Returns:
    ///
    ///     A shapely array.
    fn to_shapely<'a>(&'a self, py: Python<'a>) -> PyGeoArrowResult<PyObject> {
        check_nulls(self.0.nulls())?;
        let shapely_mod = import_shapely(py)?;
        let shapely_arr =
            shapely_mod.call_method1(intern!(py, "from_wkb"), (self.__array__(py)?,))?;
        Ok(shapely_arr.to_object(py))
    }
}

// macro_rules! impl_chunked_to_shapely {
//     ($py_chunked_struct:ty, $py_array_struct:ident) => {
//         #[pymethods]
//         impl $py_chunked_struct {
//             /// Convert this array to a shapely array
//             ///
//             /// Returns:
//             ///
//             ///     A shapely array.
//             pub fn to_shapely<'a>(&'a self, py: Python<'a>) -> PyGeoArrowResult<Bound<PyAny>> {
//                 let numpy_mod = py.import_bound(intern!(py, "numpy"))?;
//                 let shapely_chunks = self
//                     .0
//                     .chunks()
//                     .iter()
//                     .map(|chunk| {
//                         Ok($py_array_struct(chunk.clone())
//                             .to_shapely(py)?
//                             .to_object(py))
//                     })
//                     .collect::<PyGeoArrowResult<Vec<_>>>()?;
//                 Ok(numpy_mod.call_method1(intern!(py, "concatenate"), (shapely_chunks,))?)
//             }
//         }
//     };
// }

// impl_chunked_to_shapely!(ChunkedPointArray, PointArray);
// impl_chunked_to_shapely!(ChunkedLineStringArray, LineStringArray);
// impl_chunked_to_shapely!(ChunkedPolygonArray, PolygonArray);
// impl_chunked_to_shapely!(ChunkedMultiPointArray, MultiPointArray);
// impl_chunked_to_shapely!(ChunkedMultiLineStringArray, MultiLineStringArray);
// impl_chunked_to_shapely!(ChunkedMultiPolygonArray, MultiPolygonArray);
// impl_chunked_to_shapely!(ChunkedMixedGeometryArray, MixedGeometryArray);
// impl_chunked_to_shapely!(ChunkedGeometryCollectionArray, GeometryCollectionArray);
// impl_chunked_to_shapely!(ChunkedWKBArray, WKBArray);
