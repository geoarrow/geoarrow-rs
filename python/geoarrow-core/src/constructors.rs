use std::sync::Arc;

use geoarrow_array::array::{
    LineStringArray, MultiLineStringArray, MultiPointArray, MultiPolygonArray, PointArray,
    PolygonArray,
};
use geoarrow_schema::Metadata;
use pyo3::prelude::*;
use pyo3_geoarrow::{PyCoordBuffer, PyCrs, PyEdges, PyGeoArray, PyGeoArrowResult, PyOffsetBuffer};

fn create_array_metadata(crs: Option<PyCrs>, edges: Option<PyEdges>) -> Arc<Metadata> {
    let edges = edges.map(|e| e.into());
    Arc::new(Metadata::new(crs.unwrap_or_default().into(), edges))
}

// TODO: convert these constructors to take in `type` as an argument?
#[pyfunction]
#[pyo3(signature = (coords, *, crs = None, edges = None))]
pub fn points(
    coords: PyCoordBuffer,
    crs: Option<PyCrs>,
    edges: Option<PyEdges>,
) -> PyGeoArrowResult<PyGeoArray> {
    let metadata = create_array_metadata(crs, edges);
    let array = PointArray::new(coords.into_inner(), None, metadata);
    Ok(PyGeoArray::new(Arc::new(array)))
}

#[pyfunction]
#[pyo3(signature = (coords, geom_offsets, *, crs = None, edges = None))]
pub fn linestrings(
    coords: PyCoordBuffer,
    geom_offsets: PyOffsetBuffer,
    crs: Option<PyCrs>,
    edges: Option<PyEdges>,
) -> PyGeoArrowResult<PyGeoArray> {
    let metadata = create_array_metadata(crs, edges);
    let array = LineStringArray::new(
        coords.into_inner(),
        geom_offsets.into_inner(),
        None,
        metadata,
    );
    Ok(PyGeoArray::new(Arc::new(array)))
}

#[pyfunction]
#[pyo3(signature = (coords, geom_offsets, ring_offsets, *, crs = None, edges = None))]
pub fn polygons(
    coords: PyCoordBuffer,
    geom_offsets: PyOffsetBuffer,
    ring_offsets: PyOffsetBuffer,
    crs: Option<PyCrs>,
    edges: Option<PyEdges>,
) -> PyGeoArrowResult<PyGeoArray> {
    let metadata = create_array_metadata(crs, edges);
    let array = PolygonArray::new(
        coords.into_inner(),
        geom_offsets.into_inner(),
        ring_offsets.into_inner(),
        None,
        metadata,
    );
    Ok(PyGeoArray::new(Arc::new(array)))
}

#[pyfunction]
#[pyo3(signature = (coords, geom_offsets, *, crs = None, edges = None))]
pub fn multipoints(
    coords: PyCoordBuffer,
    geom_offsets: PyOffsetBuffer,
    crs: Option<PyCrs>,
    edges: Option<PyEdges>,
) -> PyGeoArrowResult<PyGeoArray> {
    let metadata = create_array_metadata(crs, edges);
    let array = MultiPointArray::new(
        coords.into_inner(),
        geom_offsets.into_inner(),
        None,
        metadata,
    );
    Ok(PyGeoArray::new(Arc::new(array)))
}

#[pyfunction]
#[pyo3(signature = (coords, geom_offsets, ring_offsets, *, crs = None, edges = None))]
pub fn multilinestrings(
    coords: PyCoordBuffer,
    geom_offsets: PyOffsetBuffer,
    ring_offsets: PyOffsetBuffer,
    crs: Option<PyCrs>,
    edges: Option<PyEdges>,
) -> PyGeoArrowResult<PyGeoArray> {
    let metadata = create_array_metadata(crs, edges);
    let array = MultiLineStringArray::new(
        coords.into_inner(),
        geom_offsets.into_inner(),
        ring_offsets.into_inner(),
        None,
        metadata,
    );
    Ok(PyGeoArray::new(Arc::new(array)))
}

#[pyfunction]
#[pyo3(signature = (coords, geom_offsets, polygon_offsets, ring_offsets, *, crs = None, edges = None))]
pub fn multipolygons(
    coords: PyCoordBuffer,
    geom_offsets: PyOffsetBuffer,
    polygon_offsets: PyOffsetBuffer,
    ring_offsets: PyOffsetBuffer,
    crs: Option<PyCrs>,
    edges: Option<PyEdges>,
) -> PyGeoArrowResult<PyGeoArray> {
    let metadata = create_array_metadata(crs, edges);
    let array = MultiPolygonArray::new(
        coords.into_inner(),
        geom_offsets.into_inner(),
        polygon_offsets.into_inner(),
        ring_offsets.into_inner(),
        None,
        metadata,
    );
    Ok(PyGeoArray::new(Arc::new(array)))
}
