use geo::Rect;
use geo::algorithm::bounding_rect::BoundingRect as GeoBoundingRect;
use geoarrow_schema::Dimension;

use crate::NativeArray;
use crate::array::*;
use crate::chunked_array::{ChunkedGeometryArray, ChunkedNativeArray};
use crate::datatypes::NativeType;
use crate::error::Result;
use crate::trait_::ArrayAccessor;

/// Calculation of the bounding rectangle of a geometry.
pub trait BoundingRect {
    type Output;

    /// Return the bounding rectangle of a geometry
    ///
    /// # Examples
    ///
    /// ```
    /// use geo::BoundingRect;
    /// use geo::line_string;
    ///
    /// let line_string = line_string![
    ///     (x: 40.02f64, y: 116.34),
    ///     (x: 42.02f64, y: 116.34),
    ///     (x: 42.02f64, y: 118.34),
    /// ];
    ///
    /// let bounding_rect = line_string.bounding_rect().unwrap();
    ///
    /// assert_eq!(40.02f64, bounding_rect.min().x);
    /// assert_eq!(42.02f64, bounding_rect.max().x);
    /// assert_eq!(116.34, bounding_rect.min().y);
    /// assert_eq!(118.34, bounding_rect.max().y);
    /// ```
    fn bounding_rect(&self) -> Self::Output;
}

impl BoundingRect for PointArray {
    type Output = RectArray;

    fn bounding_rect(&self) -> Self::Output {
        let output_geoms: Vec<Option<Rect>> = self
            .iter_geo()
            .map(|maybe_g| maybe_g.map(|geom| geom.bounding_rect()))
            .collect();

        RectBuilder::from_nullable_rects(
            output_geoms.iter().map(|x| x.as_ref()),
            Dimension::XY,
            self.metadata().clone(),
        )
        .finish()
    }
}

impl BoundingRect for RectArray {
    type Output = RectArray;

    fn bounding_rect(&self) -> Self::Output {
        self.clone()
    }
}

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($type:ty) => {
        impl BoundingRect for $type {
            type Output = RectArray;

            fn bounding_rect(&self) -> Self::Output {
                let output_geoms: Vec<Option<Rect>> = self
                    .iter_geo()
                    .map(|maybe_g| maybe_g.and_then(|geom| geom.bounding_rect()))
                    .collect();

                RectBuilder::from_nullable_rects(
                    output_geoms.iter().map(|x| x.as_ref()),
                    Dimension::XY,
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
iter_geo_impl!(MixedGeometryArray);
iter_geo_impl!(GeometryCollectionArray);
iter_geo_impl!(GeometryArray);

impl BoundingRect for &dyn NativeArray {
    type Output = Result<RectArray>;

    fn bounding_rect(&self) -> Self::Output {
        use NativeType::*;

        let result = match self.data_type() {
            Point(_) => self.as_point().bounding_rect(),
            LineString(_) => self.as_line_string().bounding_rect(),
            Polygon(_) => self.as_polygon().bounding_rect(),
            MultiPoint(_) => self.as_multi_point().bounding_rect(),
            MultiLineString(_) => self.as_multi_line_string().bounding_rect(),
            MultiPolygon(_) => self.as_multi_polygon().bounding_rect(),
            GeometryCollection(_) => self.as_geometry_collection().bounding_rect(),
            Geometry(_) => self.as_geometry().bounding_rect(),
            Rect(_) => self.as_rect().bounding_rect(),
        };
        Ok(result)
    }
}

impl<G: NativeArray> BoundingRect for ChunkedGeometryArray<G> {
    type Output = Result<ChunkedGeometryArray<RectArray>>;

    fn bounding_rect(&self) -> Self::Output {
        self.try_map(|chunk| chunk.as_ref().bounding_rect())?
            .try_into()
    }
}

impl BoundingRect for &dyn ChunkedNativeArray {
    type Output = Result<ChunkedGeometryArray<RectArray>>;

    fn bounding_rect(&self) -> Self::Output {
        use NativeType::*;

        match self.data_type() {
            Point(_) => self.as_point().bounding_rect(),
            LineString(_) => self.as_line_string().bounding_rect(),
            Polygon(_) => self.as_polygon().bounding_rect(),
            MultiPoint(_) => self.as_multi_point().bounding_rect(),
            MultiLineString(_) => self.as_multi_line_string().bounding_rect(),
            MultiPolygon(_) => self.as_multi_polygon().bounding_rect(),
            GeometryCollection(_) => self.as_geometry_collection().bounding_rect(),
            Geometry(_) => self.as_geometry().bounding_rect(),
            Rect(_) => self.as_rect().bounding_rect(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use geo::point;

    use crate::ArrayBase;
    use crate::array::{GeometryCollectionArray, PointArray};

    #[test]
    fn stack_overflow_repro_issue_979() {
        let points: Vec<geo::Point<f64>> = vec![
            point! {x: -104.991, y: 39.7392},
            point! {x: -73.7897, y: 40.7379},
        ];
        let point_array: PointArray = (points.as_slice(), Dimension::XY).into();
        assert!(point_array.is_valid(0));
        assert!(point_array.is_valid(1));
        let _ = point_array.bounding_rect();

        let gc_array: GeometryCollectionArray = point_array.into();
        assert!(gc_array.is_valid(0));
        assert!(gc_array.is_valid(1));
        let _ = gc_array.bounding_rect();
    }
}
