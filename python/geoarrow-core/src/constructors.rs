use std::sync::Arc;

use geoarrow::array::{
    LineStringArray, MultiLineStringArray, MultiPointArray, MultiPolygonArray, NativeArrayDyn,
    PointArray, PolygonArray,
};
use pyo3::prelude::*;
use pyo3_geoarrow::{PyCoordBuffer, PyGeoArrowResult, PyNativeArray, PyOffsetBuffer};

#[pyfunction]
pub fn points(coords: PyCoordBuffer) -> PyGeoArrowResult<PyNativeArray> {
    match coords {
        PyCoordBuffer::TwoD(coords) => {
            let array = PointArray::new(coords.into(), None, Default::default());
            Ok(PyNativeArray::new(NativeArrayDyn::new(Arc::new(array))))
        }
        PyCoordBuffer::ThreeD(coords) => {
            let array = PointArray::new(coords.into(), None, Default::default());
            Ok(PyNativeArray::new(NativeArrayDyn::new(Arc::new(array))))
        }
    }
}

#[pyfunction]
pub fn linestrings(
    coords: PyCoordBuffer,
    geom_offsets: PyOffsetBuffer,
) -> PyGeoArrowResult<PyNativeArray> {
    match coords {
        PyCoordBuffer::TwoD(coords) => {
            let array = LineStringArray::new(
                coords.into(),
                geom_offsets.into_inner(),
                None,
                Default::default(),
            );
            Ok(PyNativeArray::new(NativeArrayDyn::new(Arc::new(array))))
        }
        PyCoordBuffer::ThreeD(coords) => {
            let array = LineStringArray::new(
                coords.into(),
                geom_offsets.into_inner(),
                None,
                Default::default(),
            );
            Ok(PyNativeArray::new(NativeArrayDyn::new(Arc::new(array))))
        }
    }
}

#[pyfunction]
pub fn polygons(
    coords: PyCoordBuffer,
    geom_offsets: PyOffsetBuffer,
    ring_offsets: PyOffsetBuffer,
) -> PyGeoArrowResult<PyNativeArray> {
    match coords {
        PyCoordBuffer::TwoD(coords) => {
            let array = PolygonArray::new(
                coords.into(),
                geom_offsets.into_inner(),
                ring_offsets.into_inner(),
                None,
                Default::default(),
            );
            Ok(PyNativeArray::new(NativeArrayDyn::new(Arc::new(array))))
        }
        PyCoordBuffer::ThreeD(coords) => {
            let array = PolygonArray::new(
                coords.into(),
                geom_offsets.into_inner(),
                ring_offsets.into_inner(),
                None,
                Default::default(),
            );
            Ok(PyNativeArray::new(NativeArrayDyn::new(Arc::new(array))))
        }
    }
}

#[pyfunction]
pub fn multipoints(
    coords: PyCoordBuffer,
    geom_offsets: PyOffsetBuffer,
) -> PyGeoArrowResult<PyNativeArray> {
    match coords {
        PyCoordBuffer::TwoD(coords) => {
            let array = MultiPointArray::new(
                coords.into(),
                geom_offsets.into_inner(),
                None,
                Default::default(),
            );
            Ok(PyNativeArray::new(NativeArrayDyn::new(Arc::new(array))))
        }
        PyCoordBuffer::ThreeD(coords) => {
            let array = MultiPointArray::new(
                coords.into(),
                geom_offsets.into_inner(),
                None,
                Default::default(),
            );
            Ok(PyNativeArray::new(NativeArrayDyn::new(Arc::new(array))))
        }
    }
}

#[pyfunction]
pub fn multilinestrings(
    coords: PyCoordBuffer,
    geom_offsets: PyOffsetBuffer,
    ring_offsets: PyOffsetBuffer,
) -> PyGeoArrowResult<PyNativeArray> {
    match coords {
        PyCoordBuffer::TwoD(coords) => {
            let array = MultiLineStringArray::new(
                coords.into(),
                geom_offsets.into_inner(),
                ring_offsets.into_inner(),
                None,
                Default::default(),
            );
            Ok(PyNativeArray::new(NativeArrayDyn::new(Arc::new(array))))
        }
        PyCoordBuffer::ThreeD(coords) => {
            let array = MultiLineStringArray::new(
                coords.into(),
                geom_offsets.into_inner(),
                ring_offsets.into_inner(),
                None,
                Default::default(),
            );
            Ok(PyNativeArray::new(NativeArrayDyn::new(Arc::new(array))))
        }
    }
}

#[pyfunction]
pub fn multipolygons(
    coords: PyCoordBuffer,
    geom_offsets: PyOffsetBuffer,
    polygon_offsets: PyOffsetBuffer,
    ring_offsets: PyOffsetBuffer,
) -> PyGeoArrowResult<PyNativeArray> {
    match coords {
        PyCoordBuffer::TwoD(coords) => {
            let array = MultiPolygonArray::new(
                coords.into(),
                geom_offsets.into_inner(),
                polygon_offsets.into_inner(),
                ring_offsets.into_inner(),
                None,
                Default::default(),
            );
            Ok(PyNativeArray::new(NativeArrayDyn::new(Arc::new(array))))
        }
        PyCoordBuffer::ThreeD(coords) => {
            let array = MultiPolygonArray::new(
                coords.into(),
                geom_offsets.into_inner(),
                polygon_offsets.into_inner(),
                ring_offsets.into_inner(),
                None,
                Default::default(),
            );
            Ok(PyNativeArray::new(NativeArrayDyn::new(Arc::new(array))))
        }
    }
}
