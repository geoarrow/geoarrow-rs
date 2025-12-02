use std::sync::Arc;

use crate::constructors::array::{
    linestrings, multilinestrings, multipoints, multipolygons, points, polygons,
};
use crate::interop::shapely::utils::import_shapely;
use crate::interop::utils::new_metadata;
use arrow_array::builder::BinaryBuilder;
use geoarrow_array::array::WkbArray;
use geoarrow_schema::{GeometryType, Metadata};
use pyo3::exceptions::PyValueError;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedBytes;
use pyo3::types::{PyDict, PyTuple};
use pyo3::{IntoPyObjectExt, PyAny};
use pyo3_geoarrow::{PyCoordType, PyCrs, PyEdges, PyGeoArray, PyGeoArrowError, PyGeoArrowResult};

#[derive(Debug, Default, Clone, Copy)]
pub(crate) enum ShapelyConversionMethod {
    Ragged,
    #[default]
    Wkb,
}

impl<'a, 'py> FromPyObject<'a, 'py> for ShapelyConversionMethod {
    type Error = PyErr;
    fn extract(ob: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        let ob = ob.as_ref().bind(ob.py());
        let s: String = ob.extract()?;
        match s.to_lowercase().as_str() {
            "wkb" => Ok(Self::Wkb),
            "ragged" | "native" => Ok(Self::Ragged),
            _ => Err(PyValueError::new_err("Unexpected conversion method")),
        }
    }
}

// TODO: support chunk_size parameter to create chunked arrays
#[pyfunction]
#[pyo3(
    signature = (input, *, crs = None, edges = None, method = ShapelyConversionMethod::Wkb, coord_type = None),
    text_signature = "(input, *, crs = None, edges = None, method = 'wkb', coord_type = None)")
]
pub(crate) fn from_shapely(
    py: Python,
    input: &Bound<PyAny>,
    crs: Option<PyCrs>,
    edges: Option<PyEdges>,
    method: ShapelyConversionMethod,
    coord_type: Option<PyCoordType>,
) -> PyGeoArrowResult<PyGeoArray> {
    match method {
        ShapelyConversionMethod::Wkb => from_shapely_via_wkb(py, input, crs, edges, coord_type),
        ShapelyConversionMethod::Ragged => from_shapely_via_ragged(py, input, crs, edges),
    }
}

fn from_shapely_via_wkb(
    py: Python,
    input: &Bound<PyAny>,
    crs: Option<PyCrs>,
    edges: Option<PyEdges>,
    coord_type: Option<PyCoordType>,
) -> PyGeoArrowResult<PyGeoArray> {
    let metadata = new_metadata(crs, edges);

    let wkb_arr = make_wkb_arr(py, input, metadata.clone())?;
    let to_type = GeometryType::new(metadata)
        .with_coord_type(coord_type.map(|c| c.into()).unwrap_or_default());
    let geom_arr = geoarrow_array::cast::from_wkb(&wkb_arr, to_type.into())?;
    let py_geo_arr = PyGeoArray::new(geom_arr).into_bound_py_any(py)?;

    // Use the Python-exposed downcast method
    let kwargs = PyDict::new(py);
    if let Some(ctype) = coord_type {
        kwargs.set_item("coord_type", ctype)?;
    }

    Ok(py_geo_arr
        .call_method(intern!(py, "downcast"), (), Some(&kwargs))?
        .extract()?)
}

fn make_wkb_arr(
    py: Python,
    input: &Bound<PyAny>,
    metadata: Arc<Metadata>,
) -> PyGeoArrowResult<WkbArray> {
    let shapely_mod = import_shapely(py)?;
    let wkb_result = call_to_wkb(py, &shapely_mod, input)?;

    // TODO: use new WKB/WKT direct builder APIs
    // https://github.com/geoarrow/geoarrow-rs/issues/1349
    let mut builder = BinaryBuilder::with_capacity(wkb_result.len()?, 0);

    for item in wkb_result.try_iter()? {
        let buf = item?
            .extract::<PyBackedBytes>()
            .map_err(PyErr::from)
            .map_err(PyGeoArrowError::from)?;
        builder.append_value(buf.as_ref());
    }

    Ok(WkbArray::new(builder.finish(), metadata))
}

/// Call shapely.to_wkb
fn call_to_wkb<'a>(
    py: Python<'a>,
    shapely_mod: &'a Bound<PyModule>,
    input: &'a Bound<PyAny>,
) -> PyGeoArrowResult<Bound<'a, PyAny>> {
    let args = (input,);

    let kwargs = PyDict::new(py);
    kwargs.set_item("output_dimension", 2)?;
    kwargs.set_item("include_srid", false)?;
    kwargs.set_item("flavor", "iso")?;

    Ok(shapely_mod.call_method(intern!(py, "to_wkb"), args, Some(&kwargs))?)
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum ShapelyGeometryType {
    Point,
    LineString,
    Polygon,
    MultiPoint,
    MultiLineString,
    MultiPolygon,
}

impl<'a, 'py> FromPyObject<'a, 'py> for ShapelyGeometryType {
    type Error = PyErr;
    fn extract(ob: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        let ob = ob.as_ref().bind(ob.py());
        match ob.extract::<isize>()? {
            0 => Ok(Self::Point),
            1 => Ok(Self::LineString),
            3 => Ok(Self::Polygon),
            4 => Ok(Self::MultiPoint),
            5 => Ok(Self::MultiLineString),
            6 => Ok(Self::MultiPolygon),
            _ => Err(PyValueError::new_err(format!(
                "Unexpected or unsupported geometry type: {}",
                ob
            ))),
        }
    }
}

fn from_shapely_via_ragged(
    py: Python,
    input: &Bound<PyAny>,
    crs: Option<PyCrs>,
    edges: Option<PyEdges>,
) -> PyGeoArrowResult<PyGeoArray> {
    let numpy_mod = py.import(intern!(py, "numpy"))?;
    let shapely_mod = import_shapely(py)?;
    let kwargs = PyDict::new(py);

    let (geom_type, coords, offsets) = shapely_mod
        .call_method(intern!(py, "to_ragged_array"), (input,), Some(&kwargs))?
        .extract::<(ShapelyGeometryType, Bound<PyAny>, Py<PyAny>)>()?;

    let coords = numpy_mod.call_method1(
        intern!(py, "ascontiguousarray"),
        PyTuple::new(py, vec![coords])?,
    )?;

    match geom_type {
        ShapelyGeometryType::Point => points(coords.extract()?, crs, edges),
        ShapelyGeometryType::LineString => {
            let (geom_offsets,) = offsets.extract::<(Bound<PyAny>,)>(py)?;
            linestrings(coords.extract()?, geom_offsets.extract()?, crs, edges)
        }
        ShapelyGeometryType::Polygon => {
            let (ring_offsets, geom_offsets) =
                offsets.extract::<(Bound<PyAny>, Bound<PyAny>)>(py)?;

            polygons(
                coords.extract()?,
                geom_offsets.extract()?,
                ring_offsets.extract()?,
                crs,
                edges,
            )
        }
        ShapelyGeometryType::MultiPoint => {
            let (geom_offsets,) = offsets.extract::<(Bound<PyAny>,)>(py)?;
            multipoints(coords.extract()?, geom_offsets.extract()?, crs, edges)
        }
        ShapelyGeometryType::MultiLineString => {
            let (ring_offsets, geom_offsets) =
                offsets.extract::<(Bound<PyAny>, Bound<PyAny>)>(py)?;
            multilinestrings(
                coords.extract()?,
                geom_offsets.extract()?,
                ring_offsets.extract()?,
                crs,
                edges,
            )
        }
        ShapelyGeometryType::MultiPolygon => {
            let (ring_offsets, polygon_offsets, geom_offsets) =
                offsets.extract::<(Bound<PyAny>, Bound<PyAny>, Bound<PyAny>)>(py)?;

            multipolygons(
                coords.extract()?,
                geom_offsets.extract()?,
                polygon_offsets.extract()?,
                ring_offsets.extract()?,
                crs,
                edges,
            )
        }
    }
}
