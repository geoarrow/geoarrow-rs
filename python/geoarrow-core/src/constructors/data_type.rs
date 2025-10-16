use std::sync::Arc;

use geoarrow_schema::{
    BoxType, GeoArrowType, GeometryCollectionType, GeometryType, LineStringType, Metadata,
    MultiLineStringType, MultiPointType, MultiPolygonType, PointType, PolygonType, WkbType,
    WktType,
};
use pyo3::prelude::*;

use pyo3_geoarrow::{PyCoordType, PyCrs, PyDimension, PyEdges, PyGeoType};

macro_rules! impl_native_type_constructor {
    ($fn_name:ident, $geoarrow_type:ty) => {
        #[allow(missing_docs)]
        #[pyfunction]
        #[pyo3(
            signature = (dimension, *, coord_type = PyCoordType::Separated, crs=None, edges=None),
            text_signature = "(dimension, *, coord_type='separated', crs=None, edges=None)"
        )]
        pub fn $fn_name(
            dimension: PyDimension,
            coord_type: PyCoordType,
            crs: Option<PyCrs>,
            edges: Option<PyEdges>,
        ) -> PyGeoType {
            let edges = edges.map(|e| e.into());
            let metadata = Arc::new(Metadata::new(crs.unwrap_or_default().into(), edges));
            <$geoarrow_type>::new(dimension.into(), metadata)
                .with_coord_type(coord_type.into())
                .into()
        }
    };
}

impl_native_type_constructor!(point, PointType);
impl_native_type_constructor!(linestring, LineStringType);
impl_native_type_constructor!(polygon, PolygonType);
impl_native_type_constructor!(multipoint, MultiPointType);
impl_native_type_constructor!(multilinestring, MultiLineStringType);
impl_native_type_constructor!(multipolygon, MultiPolygonType);
impl_native_type_constructor!(geometrycollection, GeometryCollectionType);

#[pyfunction]
#[pyo3(signature = (dimension, *, crs=None, edges=None))]
pub fn r#box(dimension: PyDimension, crs: Option<PyCrs>, edges: Option<PyEdges>) -> PyGeoType {
    let edges = edges.map(|e| e.into());
    let metadata = Arc::new(Metadata::new(crs.unwrap_or_default().into(), edges));
    BoxType::new(dimension.into(), metadata).into()
}

#[pyfunction]
#[pyo3(
    signature = (*, coord_type = PyCoordType::Separated, crs=None, edges=None),
    text_signature = "(*, coord_type='separated', crs=None, edges=None)"
)]
pub fn geometry(coord_type: PyCoordType, crs: Option<PyCrs>, edges: Option<PyEdges>) -> PyGeoType {
    let edges = edges.map(|e| e.into());
    let metadata = Arc::new(Metadata::new(crs.unwrap_or_default().into(), edges));
    GeometryType::new(metadata)
        .with_coord_type(coord_type.into())
        .into()
}

macro_rules! impl_wkb_wkt {
    ($method_name:ident, $type_constructor:ty, $variant:expr) => {
        #[allow(missing_docs)]
        #[pyfunction]
        #[pyo3(signature = (*, crs=None, edges=None))]
        pub fn $method_name(crs: Option<PyCrs>, edges: Option<PyEdges>) -> PyGeoType {
            let edges = edges.map(|e| e.into());
            let metadata = Arc::new(Metadata::new(crs.unwrap_or_default().into(), edges));
            $variant(<$type_constructor>::new(metadata)).into()
        }
    };
}

impl_wkb_wkt!(wkb, WkbType, GeoArrowType::Wkb);
impl_wkb_wkt!(large_wkb, WkbType, GeoArrowType::LargeWkb);
impl_wkb_wkt!(wkb_view, WkbType, GeoArrowType::WkbView);
impl_wkb_wkt!(wkt, WktType, GeoArrowType::Wkt);
impl_wkb_wkt!(large_wkt, WktType, GeoArrowType::LargeWkt);
impl_wkb_wkt!(wkt_view, WktType, GeoArrowType::WktView);
