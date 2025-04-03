use crate::array::*;
use crate::datatypes::NativeType;
use crate::error::Result;
use crate::trait_::ArrayAccessor;
use crate::NativeArray;
use geo::algorithm::interior_point::InteriorPoint as _;

/// Calculation of interior points.
///
/// An interior point is a point that's guaranteed to intersect a given geometry, and will be
/// strictly on the interior of the geometry if possible, or on the edge if the geometry has zero
/// area. A best effort will additionally be made to locate the point reasonably centrally.
///
/// For polygons, this point is located by drawing a line that approximately subdivides the
/// bounding box around the polygon in half, intersecting it with the polygon, then calculating
/// the midpoint of the longest line produced by the intersection. For lines, the non-endpoint
/// vertex closest to the line's centroid is returned if the line has interior points, or an
/// endpoint is returned otherwise.
///
/// For multi-geometries or collections, the interior points of the constituent components are
/// calculated, and one of those is returned (for MultiPolygons, it's the point that's the midpoint
/// of the longest intersection of the intersection lines of any of the constituent polygons, as
/// described above; for all others, the interior point closest to the collection's centroid is
/// used).
///
pub trait InteriorPoint {
    type Output;

    fn interior_point(&self) -> Self::Output;
}

impl InteriorPoint for PointArray {
    type Output = PointArray;

    fn interior_point(&self) -> Self::Output {
        self.clone()
    }
}

impl InteriorPoint for RectArray {
    type Output = PointArray;

    fn interior_point(&self) -> Self::Output {
        let mut output_array = PointBuilder::with_capacity_and_options(
            Dimension::XY,
            self.len(),
            self.coord_type(),
            self.metadata().clone(),
        );
        self.iter_geo().for_each(|maybe_g| {
            output_array.push_point(maybe_g.map(|g| g.interior_point()).as_ref())
        });
        output_array.into()
    }
}

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($type:ty) => {
        impl InteriorPoint for $type {
            type Output = PointArray;

            fn interior_point(&self) -> Self::Output {
                let mut output_array = PointBuilder::with_capacity_and_options(
                    Dimension::XY,
                    self.len(),
                    self.coord_type(),
                    self.metadata().clone(),
                );
                self.iter_geo().for_each(|maybe_g| {
                    output_array.push_point(maybe_g.and_then(|g| g.interior_point()).as_ref())
                });
                output_array.into()
            }
        }
    };
}

iter_geo_impl!(LineStringArray);
iter_geo_impl!(PolygonArray);
iter_geo_impl!(MultiPointArray);
iter_geo_impl!(MultiLineStringArray);
iter_geo_impl!(MultiPolygonArray);
iter_geo_impl!(MixedGeometryArray);
iter_geo_impl!(GeometryCollectionArray);
iter_geo_impl!(GeometryArray);

impl InteriorPoint for &dyn NativeArray {
    type Output = Result<PointArray>;

    fn interior_point(&self) -> Self::Output {
        use NativeType::*;

        let result = match self.data_type() {
            Point(_) => self.as_point().interior_point(),
            LineString(_) => self.as_line_string().interior_point(),
            Polygon(_) => self.as_polygon().interior_point(),
            MultiPoint(_) => self.as_multi_point().interior_point(),
            MultiLineString(_) => self.as_multi_line_string().interior_point(),
            MultiPolygon(_) => self.as_multi_polygon().interior_point(),
            GeometryCollection(_) => self.as_geometry_collection().interior_point(),
            Rect(_) => self.as_rect().interior_point(),
            Geometry(_) => self.as_geometry().interior_point(),
        };
        Ok(result)
    }
}
