//! Defines the logical data types of GeoArrow arrays.
//!
//! The most important things you might be looking for are:
//!
//!  * [`GeoArrowType`] to describe an array's geometry type.
//!  * [`Dimension`] to describe the dimension of an array.
//!  * [`CoordType`] to describe whether an array has interleaved or separated coordinatesx.

pub use geoarrow_schema::{
    BoxType, CoordType, Crs, CrsType, Dimension, Edges, GeoArrowType, GeometryCollectionType,
    GeometryType, LineStringType, Metadata, MultiLineStringType, MultiPointType, MultiPolygonType,
    PointType, PolygonType, WkbType, WktType,
};
