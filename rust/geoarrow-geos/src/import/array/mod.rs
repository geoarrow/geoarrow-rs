mod geometry;
mod geometrycollection;
mod linestring;
mod multilinestring;
mod multipoint;
mod multipolygon;
mod point;
mod polygon;
mod wkb;

use arrow_schema::extension::ExtensionType;
use geoarrow_array::error::Result;

pub trait FromGEOS: Sized {
    type GeoArrowType: ExtensionType;

    /// Convert a sequence of GEOS geometries to a GeoArrow array.
    fn from_geos(
        geoms: impl IntoIterator<Item = Option<geos::Geometry>>,
        typ: Self::GeoArrowType,
    ) -> Result<Self>;
}
