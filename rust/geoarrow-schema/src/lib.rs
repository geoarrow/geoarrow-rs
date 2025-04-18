//! GeoArrow geometry type and metadata definitions.

#![warn(missing_docs)]

mod coord_type;
mod crs;
mod dimension;
mod edges;
mod metadata;
mod r#type;

pub use coord_type::CoordType;
pub use crs::{Crs, CrsType};
pub use dimension::Dimension;
pub use edges::Edges;
pub use metadata::Metadata;
pub use r#type::{
    BoxType, GeometryCollectionType, GeometryType, LineStringType, MultiLineStringType,
    MultiPointType, MultiPolygonType, PointType, PolygonType, WkbType, WktType,
};
