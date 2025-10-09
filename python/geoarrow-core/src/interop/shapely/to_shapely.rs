use std::sync::Arc;

// use crate::interop::numpy::to_numpy::wkb_array_to_numpy;
use crate::interop::shapely::utils::import_shapely;
use geoarrow_array::GeoArrowArray;
use geoarrow_array::array::{
    CoordBuffer, LineStringArray, MultiLineStringArray, MultiPointArray, MultiPolygonArray,
    PointArray, PolygonArray, RectArray,
};
use geoarrow_array::cast::{AsGeoArrowArray, to_wkb};
use geoarrow_schema::GeoArrowType;
use numpy::PyArrayMethods;
use numpy::ToPyArray;
use pyo3::PyAny;
use pyo3::exceptions::PyValueError;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::PyTuple;
use pyo3_arrow::PyArray;
use pyo3_geoarrow::input::AnyGeoArray;
use pyo3_geoarrow::{PyGeoArray, PyGeoArrowResult};

/// Check that the array has no null values
fn check_nulls(geo_arr: &dyn GeoArrowArray) -> PyGeoArrowResult<()> {
    if geo_arr.logical_null_count() > 0 {
        Err(
            PyValueError::new_err("Cannot convert GeoArrow array with null values to Shapely")
                .into(),
        )
    } else {
        Ok(())
    }
}

/// Copy a CoordBuffer to a numpy array of shape `(length, D)`
fn coords_to_numpy(py: Python, coords: &CoordBuffer) -> PyGeoArrowResult<PyObject> {
    match coords {
        CoordBuffer::Interleaved(cb) => {
            let size = cb.dim().size();
            let scalar_buffer = cb.coords();
            let numpy_coords = scalar_buffer
                .to_pyarray(py)
                .reshape([scalar_buffer.len() / size, size])?;

            Ok(numpy_coords.into_pyobject(py).unwrap().into_any().unbind())
        }
        CoordBuffer::Separated(cb) => {
            let buffers = cb.buffers();
            let numpy_buffers = buffers
                .iter()
                .map(|buf| buf.to_pyarray(py))
                .collect::<Vec<_>>();

            let numpy_mod = py.import(intern!(py, "numpy"))?;
            Ok(numpy_mod
                .call_method1(
                    intern!(py, "column_stack"),
                    PyTuple::new(py, numpy_buffers)?,
                )?
                .into_pyobject(py)
                .unwrap()
                .into_any()
                .unbind())
        }
    }
}

#[pyfunction]
pub fn to_shapely(py: Python, input: AnyGeoArray) -> PyGeoArrowResult<Bound<PyAny>> {
    match input {
        AnyGeoArray::Array(arr) => pyarray_to_shapely(py, arr),
        AnyGeoArray::Stream(stream) => {
            let field = stream.field_ref()?;
            let mut shapely_chunks = vec![];
            for chunk in stream.into_reader()? {
                let py_array = PyArray::new(chunk?, field.clone());
                shapely_chunks.push(pyarray_to_shapely(py, py_array)?);
            }

            let numpy_mod = py.import(intern!(py, "numpy"))?;
            Ok(numpy_mod.call_method1(intern!(py, "concatenate"), (shapely_chunks,))?)
        }
    }
}

fn pyarray_to_shapely(py: Python, input: PyGeoArray) -> PyGeoArrowResult<Bound<PyAny>> {
    let geo_arr = input.into_inner();
    check_nulls(geo_arr)?;

    match geo_arr.data_type() {
        GeoArrowType::Point(_) => point_arr(py, geo_arr.as_point()),
        GeoArrowType::LineString(_) => linestring_arr(py, geo_arr.as_line_string()),
        GeoArrowType::Polygon(_) => polygon_arr(py, geo_arr.as_polygon()),
        GeoArrowType::MultiPoint(_) => multipoint_arr(py, geo_arr.as_multi_point()),
        GeoArrowType::MultiLineString(_) => multilinestring_arr(py, geo_arr.as_multi_line_string()),
        GeoArrowType::MultiPolygon(_) => multipolygon_arr(py, geo_arr.as_multi_polygon()),
        GeoArrowType::Rect(_) => rect_arr(py, geo_arr.as_rect()),
        GeoArrowType::GeometryCollection(_) => via_wkb(py, geo_arr.as_geometry_collection()),
        GeoArrowType::Geometry(_) => via_wkb(py, geo_arr.as_geometry()),
    }
}

fn point_arr<'py>(py: Python<'py>, arr: &'py PointArray) -> PyGeoArrowResult<Bound<'py, PyAny>> {
    let shapely_mod = import_shapely(py)?;
    let shapely_geom_type_enum = shapely_mod.getattr(intern!(py, "GeometryType"))?;

    let coords = coords_to_numpy(py, arr.coords())?;
    let args = (
        shapely_geom_type_enum.getattr(intern!(py, "POINT"))?,
        coords,
    );
    Ok(shapely_mod.call_method1(intern!(py, "from_ragged_array"), args)?)
}

fn linestring_arr(py: Python, arr: LineStringArray) -> PyGeoArrowResult<Bound<PyAny>> {
    let shapely_mod = import_shapely(py)?;
    let shapely_geom_type_enum = shapely_mod.getattr(intern!(py, "GeometryType"))?;

    let coords = coords_to_numpy(py, arr.coords())?;
    let offsets = (arr.geom_offsets().to_pyarray(py),);

    let args = (
        shapely_geom_type_enum.getattr(intern!(py, "LINESTRING"))?,
        coords,
        offsets,
    );
    Ok(shapely_mod.call_method1(intern!(py, "from_ragged_array"), args)?)
}

fn polygon_arr(py: Python, arr: PolygonArray) -> PyGeoArrowResult<Bound<PyAny>> {
    let shapely_mod = import_shapely(py)?;
    let shapely_geom_type_enum = shapely_mod.getattr(intern!(py, "GeometryType"))?;

    let coords = coords_to_numpy(py, arr.coords())?;
    let offsets = (
        arr.ring_offsets().to_pyarray(py),
        arr.geom_offsets().to_pyarray(py),
    );

    let args = (
        shapely_geom_type_enum.getattr(intern!(py, "POLYGON"))?,
        coords,
        offsets,
    );
    Ok(shapely_mod.call_method1(intern!(py, "from_ragged_array"), args)?)
}

fn multipoint_arr(py: Python, arr: MultiPointArray) -> PyGeoArrowResult<Bound<PyAny>> {
    let shapely_mod = import_shapely(py)?;
    let shapely_geom_type_enum = shapely_mod.getattr(intern!(py, "GeometryType"))?;

    let coords = coords_to_numpy(py, arr.coords())?;
    let offsets = (arr.geom_offsets().to_pyarray(py),);

    let args = (
        shapely_geom_type_enum.getattr(intern!(py, "MULTIPOINT"))?,
        coords,
        offsets,
    );
    Ok(shapely_mod.call_method1(intern!(py, "from_ragged_array"), args)?)
}

fn multilinestring_arr(py: Python, arr: MultiLineStringArray) -> PyGeoArrowResult<Bound<PyAny>> {
    let shapely_mod = import_shapely(py)?;
    let shapely_geom_type_enum = shapely_mod.getattr(intern!(py, "GeometryType"))?;

    let coords = coords_to_numpy(py, arr.coords())?;
    let offsets = (
        arr.ring_offsets().to_pyarray(py),
        arr.geom_offsets().to_pyarray(py),
    );

    let args = (
        shapely_geom_type_enum.getattr(intern!(py, "MULTILINESTRING"))?,
        coords,
        offsets,
    );
    Ok(shapely_mod.call_method1(intern!(py, "from_ragged_array"), args)?)
}

fn multipolygon_arr(py: Python, arr: MultiPolygonArray) -> PyGeoArrowResult<Bound<PyAny>> {
    let shapely_mod = import_shapely(py)?;
    let shapely_geom_type_enum = shapely_mod.getattr(intern!(py, "GeometryType"))?;

    let coords = coords_to_numpy(py, arr.coords())?;
    let offsets = (
        arr.ring_offsets().to_pyarray(py),
        arr.polygon_offsets().to_pyarray(py),
        arr.geom_offsets().to_pyarray(py),
    );

    let args = (
        shapely_geom_type_enum.getattr(intern!(py, "MULTIPOLYGON"))?,
        coords,
        offsets,
    );
    Ok(shapely_mod.call_method1(intern!(py, "from_ragged_array"), args)?)
}

fn rect_arr(py: Python, arr: RectArray) -> PyGeoArrowResult<Bound<PyAny>> {
    let shapely_mod = import_shapely(py)?;

    let lower = arr.lower();
    let upper = arr.upper();

    let xmin = &lower.buffers()[0].to_pyarray(py);
    let ymin = &lower.buffers()[1].to_pyarray(py);
    let xmax = &upper.buffers()[0].to_pyarray(py);
    let ymax = &upper.buffers()[1].to_pyarray(py);

    let args = (xmin, ymin, xmax, ymax);
    Ok(shapely_mod.call_method1(intern!(py, "box"), args)?)
}

fn via_wkb(py: Python, arr: Arc<dyn NativeArray>) -> PyGeoArrowResult<Bound<PyAny>> {
    wkb_arr(py, to_wkb(arr.as_ref()))
}

fn wkb_arr(py: Python, arr: geoarrow_array::array::WkbArray) -> PyGeoArrowResult<Bound<PyAny>> {
    let shapely_mod = import_shapely(py)?;
    let args = (wkb_array_to_numpy(py, &arr)?,);
    Ok(shapely_mod.call_method1(intern!(py, "from_wkb"), args)?)
}
