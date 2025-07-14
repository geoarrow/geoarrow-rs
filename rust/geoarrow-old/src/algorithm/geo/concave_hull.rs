use crate::NativeArray;
use crate::algorithm::broadcasting::BroadcastablePrimitive;
use crate::array::*;
use crate::datatypes::NativeType;
use crate::error::{GeoArrowError, Result};
use crate::trait_::ArrayAccessor;
use arrow::datatypes::Float64Type;
use geo::Polygon;
use geo::algorithm::ConcaveHull as _;
use geoarrow_schema::Dimension;

/// Returns a polygon which covers a geometry. Unlike convex hulls, which also cover
/// their geometry, a concave hull does so while trying to further minimize its area by
/// constructing edges such that the exterior of the polygon incorporates points that would
/// be interior points in a convex hull.
///
/// This implementation is inspired by <https://github.com/mapbox/concaveman>
/// and also uses ideas from the following paper:
/// www.iis.sinica.edu.tw/page/jise/2012/201205_10.pdf
pub trait ConcaveHull {
    type Output;

    fn concave_hull(&self, concavity: &BroadcastablePrimitive<Float64Type>) -> Self::Output;
}

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($type:ty) => {
        impl ConcaveHull for $type {
            type Output = PolygonArray;

            fn concave_hull(
                &self,
                concavity: &BroadcastablePrimitive<Float64Type>,
            ) -> Self::Output {
                let output_geoms: Vec<Option<Polygon>> = self
                    .iter_geo()
                    .zip(concavity)
                    .map(|(maybe_g, concavity)| {
                        if let (Some(geom), Some(concavity)) = (maybe_g, concavity) {
                            Some(geom.concave_hull(concavity))
                        } else {
                            None
                        }
                    })
                    .collect();

                PolygonBuilder::from_nullable_polygons(
                    output_geoms.as_slice(),
                    Dimension::XY,
                    self.coord_type(),
                    self.metadata().clone(),
                )
                .finish()
            }
        }
    };
}

iter_geo_impl!(LineStringArray);
iter_geo_impl!(PolygonArray);
iter_geo_impl!(MultiPointArray);
iter_geo_impl!(MultiLineStringArray);
iter_geo_impl!(MultiPolygonArray);

impl ConcaveHull for GeometryArray {
    type Output = Result<PolygonArray>;

    fn concave_hull(&self, concavity: &BroadcastablePrimitive<Float64Type>) -> Self::Output {
        let output_geoms: Vec<Option<Polygon>> = self
            .iter_geo()
            .zip(concavity)
            .map(|(maybe_g, concavity)| {
                if let (Some(geom), Some(concavity)) = (maybe_g, concavity) {
                    let out = match geom {
                        geo::Geometry::LineString(g) => g.concave_hull(concavity),
                        geo::Geometry::Polygon(g) => g.concave_hull(concavity),
                        geo::Geometry::MultiLineString(g) => g.concave_hull(concavity),
                        geo::Geometry::MultiPolygon(g) => g.concave_hull(concavity),
                        _ => {
                            return Err(GeoArrowError::IncorrectType(
                                "incorrect type in concave_hull".into(),
                            ));
                        }
                    };
                    Ok(Some(out))
                } else {
                    Ok(None)
                }
            })
            .collect::<Result<_>>()?;

        Ok(PolygonBuilder::from_nullable_polygons(
            output_geoms.as_slice(),
            Dimension::XY,
            self.coord_type(),
            self.metadata().clone(),
        )
        .finish())
    }
}

impl ConcaveHull for &dyn NativeArray {
    type Output = Result<PolygonArray>;

    fn concave_hull(&self, concavity: &BroadcastablePrimitive<Float64Type>) -> Self::Output {
        use NativeType::*;

        let result = match self.data_type() {
            LineString(_) => self.as_line_string().concave_hull(concavity),
            Polygon(_) => self.as_polygon().concave_hull(concavity),
            MultiPoint(_) => self.as_multi_point().concave_hull(concavity),
            MultiLineString(_) => self.as_multi_line_string().concave_hull(concavity),
            MultiPolygon(_) => self.as_multi_polygon().concave_hull(concavity),
            Geometry(_) => self.as_geometry().concave_hull(concavity)?,
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}
