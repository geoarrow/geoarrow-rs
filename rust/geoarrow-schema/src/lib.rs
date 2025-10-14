#![doc = include_str!("../README.md")]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]
#![warn(missing_docs)]
#![doc(
    html_logo_url = "https://github.com/geoarrow.png",
    html_favicon_url = "https://github.com/geoarrow.png?size=32"
)]

mod coord_type;
pub mod crs;
mod datatype;
mod dimension;
mod edges;
pub mod error;
mod metadata;
mod r#type;
pub mod type_id;

pub use coord_type::CoordType;
pub use crs::{Crs, CrsType};
pub use datatype::GeoArrowType;
pub use dimension::Dimension;
pub use edges::Edges;
pub use metadata::Metadata;
pub use r#type::{
    BoxType, GeometryCollectionType, GeometryType, LineStringType, MultiLineStringType,
    MultiPointType, MultiPolygonType, PointType, PolygonType, RectType, WkbType, WktType,
};
