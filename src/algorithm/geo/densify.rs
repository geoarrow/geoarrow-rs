use crate::algorithm::broadcasting::BroadcastablePrimitive;
use crate::array::*;
use arrow_array::types::Float64Type;
use arrow_array::OffsetSizeTrait;
use geo::Densify as _Densify;

/// Return a new linear geometry containing both existing and new interpolated coordinates with
/// a maximum distance of `max_distance` between them.
///
/// Note: `max_distance` must be greater than 0.
///
/// # Examples
/// ```
/// use geo::{coord, Line, LineString};
/// use geo::Densify;
///
/// let line: Line<f64> = Line::new(coord! {x: 0.0, y: 6.0}, coord! {x: 1.0, y: 8.0});
/// let correct: LineString<f64> = vec![[0.0, 6.0], [0.5, 7.0], [1.0, 8.0]].into();
/// let max_dist = 2.0;
/// let densified = line.densify(max_dist);
/// assert_eq!(densified, correct);
///```
pub trait Densify {
    // Note: This is a trait parameter instead of generic because some types like Rect/Triangle
    // densify to non-self types
    type Output;

    fn densify(&self, max_distance: BroadcastablePrimitive<Float64Type>) -> Self::Output;
}

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($type:ty, $geo_type:ty) => {
        impl<O: OffsetSizeTrait> Densify for $type {
            type Output = $type;

            fn densify(&self, max_distance: BroadcastablePrimitive<Float64Type>) -> Self::Output {
                let output_geoms: Vec<Option<$geo_type>> = self
                    .iter_geo()
                    .zip(max_distance.into_iter())
                    .map(|(maybe_g, max_distance)| {
                        maybe_g.map(|geom| geom.densify(max_distance.unwrap()))
                    })
                    .collect();

                output_geoms.into()
            }
        }
    };
}

iter_geo_impl!(LineStringArray<O>, geo::LineString);
iter_geo_impl!(PolygonArray<O>, geo::Polygon);
iter_geo_impl!(MultiLineStringArray<O>, geo::MultiLineString);
iter_geo_impl!(MultiPolygonArray<O>, geo::MultiPolygon);
