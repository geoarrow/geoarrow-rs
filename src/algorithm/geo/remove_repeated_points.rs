use crate::array::*;
use arrow_array::OffsetSizeTrait;
use geo::RemoveRepeatedPoints as _RemoveRepeatedPoints;

/// Remove repeated points from a `MultiPoint` and repeated consecutive coordinates
/// from `LineString`, `Polygon`, `MultiLineString` and `MultiPolygon`.
///
/// For `GeometryCollection` it individually removes the repeated points
/// of each geometry in the collection.
///
/// For `Point`, `Line`, `Rect` and `Triangle` the geometry remains the same.
pub trait RemoveRepeatedPoints {
    /// Create a new geometry with (consecutive) repeated points removed.
    fn remove_repeated_points(&self) -> Self;

    // /// Remove (consecutive) repeated points inplace.
    // fn remove_repeated_points_mut(&mut self);
}

// Note: this implementation is outside the macro because it is not generic over O
impl RemoveRepeatedPoints for PointArray {
    fn remove_repeated_points(&self) -> Self {
        self.clone()
    }
}

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($type:ty, $geo_type:ty) => {
        impl<O: OffsetSizeTrait> RemoveRepeatedPoints for $type {
            fn remove_repeated_points(&self) -> Self {
                let output_geoms: Vec<Option<$geo_type>> = self
                    .iter_geo()
                    .map(|maybe_g| maybe_g.map(|geom| geom.remove_repeated_points()))
                    .collect();

                output_geoms.into()
            }
        }
    };
}

iter_geo_impl!(MultiPointArray<O>, geo::MultiPoint);
iter_geo_impl!(LineStringArray<O>, geo::LineString);
iter_geo_impl!(PolygonArray<O>, geo::Polygon);
iter_geo_impl!(MultiLineStringArray<O>, geo::MultiLineString);
iter_geo_impl!(MultiPolygonArray<O>, geo::MultiPolygon);

impl<O: OffsetSizeTrait> RemoveRepeatedPoints for GeometryArray<O> {
    fn remove_repeated_points(&self) -> Self {
        use GeometryArray::*;

        match self {
            Point(arr) => Point(arr.remove_repeated_points()),
            LineString(arr) => LineString(arr.remove_repeated_points()),
            Polygon(arr) => Polygon(arr.remove_repeated_points()),
            MultiPoint(arr) => MultiPoint(arr.remove_repeated_points()),
            MultiLineString(arr) => MultiLineString(arr.remove_repeated_points()),
            MultiPolygon(arr) => MultiPolygon(arr.remove_repeated_points()),
            Rect(arr) => Rect(arr.clone()),
        }
    }
}
