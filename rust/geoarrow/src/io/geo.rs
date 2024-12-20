//! Convert structs that implement geo-traits to [geo-types] objects.
//!
//! Note that this is the same underlying implementation as upstream [geo] in
//! <https://github.com/georust/geo/pull/1255>. However, the trait-based implementation hits this
//! compiler regression <https://github.com/rust-lang/rust/issues/128887>,
//! <https://github.com/rust-lang/rust/issues/131960>, which prevents from compiling in release
//! mode on a stable Rust version. For some reason, the **function-based implementation** does not
//! hit this regression, and thus allows building geoarrow without using latest nightly and a
//! custom `RUSTFLAGS`.
//!
//! Note that it's only `GeometryTrait` and `GeometryCollectionTrait` that hit this compiler bug.
//! Other traits can use the upstream impls.

use geo::{CoordNum, Geometry, GeometryCollection};

use geo_traits::to_geo::{
    ToGeoLine, ToGeoLineString, ToGeoMultiLineString, ToGeoMultiPoint, ToGeoMultiPolygon,
    ToGeoPoint, ToGeoPolygon, ToGeoRect, ToGeoTriangle,
};
use geo_traits::{GeometryCollectionTrait, GeometryTrait, GeometryType};

/// Convert any Geometry to a [`Geometry`].
///
/// Only the first two dimensions will be kept.
pub fn geometry_to_geo<T: CoordNum>(geometry: &impl GeometryTrait<T = T>) -> Geometry<T> {
    use GeometryType::*;

    match geometry.as_type() {
        Point(geom) => Geometry::Point(geom.to_point()),
        LineString(geom) => Geometry::LineString(geom.to_line_string()),
        Polygon(geom) => Geometry::Polygon(geom.to_polygon()),
        MultiPoint(geom) => Geometry::MultiPoint(geom.to_multi_point()),
        MultiLineString(geom) => Geometry::MultiLineString(geom.to_multi_line_string()),
        MultiPolygon(geom) => Geometry::MultiPolygon(geom.to_multi_polygon()),
        GeometryCollection(geom) => Geometry::GeometryCollection(geometry_collection_to_geo(geom)),
        Rect(geom) => Geometry::Rect(geom.to_rect()),
        Line(geom) => Geometry::Line(geom.to_line()),
        Triangle(geom) => Geometry::Triangle(geom.to_triangle()),
    }
}

/// Convert any GeometryCollection to a [`GeometryCollection`].
///
/// Only the first two dimensions will be kept.
pub fn geometry_collection_to_geo<T: CoordNum>(
    geometry_collection: &impl GeometryCollectionTrait<T = T>,
) -> GeometryCollection<T> {
    GeometryCollection::new_from(
        geometry_collection
            .geometries()
            .map(|geometry| geometry_to_geo(&geometry))
            .collect(),
    )
}
