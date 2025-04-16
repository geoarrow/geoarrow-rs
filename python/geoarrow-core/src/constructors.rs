use std::sync::Arc;

use geoarrow_array::array::{
    LineStringArray, MultiLineStringArray, MultiPointArray, MultiPolygonArray, PointArray,
    PolygonArray,
};
use geoarrow_schema::{Edges, Metadata};
use pyo3::prelude::*;
use pyo3_geoarrow::{PyCoordBuffer, PyCrs, PyGeoArrowArray, PyGeoArrowResult, PyOffsetBuffer};

fn create_array_metadata(crs: Option<PyCrs>, edges: Option<Edges>) -> Arc<Metadata> {
    Arc::new(crs.map(|inner| inner.into_inner()).unwrap_or_default())
}

#[pyfunction]
#[pyo3(signature = (coords, *, crs = None, edges = None))]
pub fn points(
    coords: PyCoordBuffer,
    crs: Option<PyCrs>,
    edges: Option<Edges>,
) -> PyGeoArrowResult<PyGeoArrowArray> {
    let metadata = create_array_metadata(crs, edges);
    let array = PointArray::new(coords.into_inner(), None, metadata);
    Ok(PyGeoArrowArray::new(Arc::new(array)))
}

#[pyfunction]
#[pyo3(signature = (coords, geom_offsets, *, crs = None, edges = None))]
pub fn linestrings(
    coords: PyCoordBuffer,
    geom_offsets: PyOffsetBuffer,
    crs: Option<PyCrs>,
    edges: Option<Edges>,
) -> PyGeoArrowResult<PyGeoArrowArray> {
    let metadata = create_array_metadata(crs, edges);
    let array = LineStringArray::new(
        coords.into_inner(),
        geom_offsets.into_inner(),
        None,
        metadata,
    );
    Ok(PyGeoArrowArray::new(Arc::new(array)))
}

#[pyfunction]
#[pyo3(signature = (coords, geom_offsets, ring_offsets, *, crs = None, edges = None))]
pub fn polygons(
    coords: PyCoordBuffer,
    geom_offsets: PyOffsetBuffer,
    ring_offsets: PyOffsetBuffer,
    crs: Option<PyCrs>,
    edges: Option<Edges>,
) -> PyGeoArrowResult<PyGeoArrowArray> {
    let metadata = create_array_metadata(crs, edges);
    let array = PolygonArray::new(
        coords.into_inner(),
        geom_offsets.into_inner(),
        ring_offsets.into_inner(),
        None,
        metadata,
    );
    Ok(PyGeoArrowArray::new(Arc::new(array)))
}

#[pyfunction]
#[pyo3(signature = (coords, geom_offsets, *, crs = None, edges = None))]
pub fn multipoints(
    coords: PyCoordBuffer,
    geom_offsets: PyOffsetBuffer,
    crs: Option<PyCrs>,
    edges: Option<Edges>,
) -> PyGeoArrowResult<PyGeoArrowArray> {
    let metadata = create_array_metadata(crs, edges);
    let array = MultiPointArray::new(
        coords.into_inner(),
        geom_offsets.into_inner(),
        None,
        metadata,
    );
    Ok(PyGeoArrowArray::new(Arc::new(array)))
}

#[pyfunction]
#[pyo3(signature = (coords, geom_offsets, ring_offsets, *, crs = None, edges = None))]
pub fn multilinestrings(
    coords: PyCoordBuffer,
    geom_offsets: PyOffsetBuffer,
    ring_offsets: PyOffsetBuffer,
    crs: Option<PyCrs>,
    edges: Option<Edges>,
) -> PyGeoArrowResult<PyGeoArrowArray> {
    let metadata = create_array_metadata(crs, edges);
    let array = MultiLineStringArray::new(
        coords.into_inner(),
        geom_offsets.into_inner(),
        ring_offsets.into_inner(),
        None,
        metadata,
    );
    Ok(PyGeoArrowArray::new(Arc::new(array)))
}

#[pyfunction]
#[pyo3(signature = (coords, geom_offsets, polygon_offsets, ring_offsets, *, crs = None, edges = None))]
pub fn multipolygons(
    coords: PyCoordBuffer,
    geom_offsets: PyOffsetBuffer,
    polygon_offsets: PyOffsetBuffer,
    ring_offsets: PyOffsetBuffer,
    crs: Option<PyCrs>,
    edges: Option<Edges>,
) -> PyGeoArrowResult<PyGeoArrowArray> {
    let metadata = create_array_metadata(crs, edges);
    let array = MultiPolygonArray::new(
        coords.into_inner(),
        geom_offsets.into_inner(),
        polygon_offsets.into_inner(),
        ring_offsets.into_inner(),
        None,
        metadata,
    );
    Ok(PyGeoArrowArray::new(Arc::new(array)))
}
