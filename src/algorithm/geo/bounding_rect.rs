use crate::array::dyn_geometry_array::{as_line_string_array, as_point_array};
use crate::array::*;
use crate::datatypes::GeoDataType;
use crate::GeometryArrayTrait;
use arrow_array::OffsetSizeTrait;
use geo::algorithm::bounding_rect::BoundingRect as GeoBoundingRect;
use geo::Rect;

/// Calculation of the bounding rectangle of a geometry.
pub trait BoundingRect {
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
    fn bounding_rect(&self) -> RectArray;
}

// TODO: just an example to show how to impl algorithms for dyn GeometryArrayTrait
impl BoundingRect for dyn GeometryArrayTrait {
    fn bounding_rect(&self) -> RectArray {
        match self.data_type() {
            GeoDataType::Point(_) => {
                let array = as_point_array(self);
                let output_geoms: Vec<Option<Rect>> = array
                    .iter_geo()
                    .map(|maybe_g| maybe_g.map(|geom| geom.bounding_rect()))
                    .collect();

                RectArray::from(output_geoms)
            }
            GeoDataType::LineString(_) => {
                let array = as_line_string_array::<i32>(self);
                let output_geoms: Vec<Option<Rect>> = array
                    .iter_geo()
                    .map(|maybe_g| maybe_g.and_then(|geom| geom.bounding_rect()))
                    .collect();

                RectArray::from(output_geoms)
            }
            _ => unimplemented!(),
        }
    }
}

impl BoundingRect for PointArray {
    fn bounding_rect(&self) -> RectArray {
        let output_geoms: Vec<Option<Rect>> = self
            .iter_geo()
            .map(|maybe_g| maybe_g.map(|geom| geom.bounding_rect()))
            .collect();

        output_geoms.into()
    }
}

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($type:ty) => {
        impl<O: OffsetSizeTrait> BoundingRect for $type {
            fn bounding_rect(&self) -> RectArray {
                let output_geoms: Vec<Option<Rect>> = self
                    .iter_geo()
                    .map(|maybe_g| maybe_g.and_then(|geom| geom.bounding_rect()))
                    .collect();

                output_geoms.into()
            }
        }
    };
}

iter_geo_impl!(LineStringArray<O>);
iter_geo_impl!(PolygonArray<O>);
iter_geo_impl!(MultiPointArray<O>);
iter_geo_impl!(MultiLineStringArray<O>);
iter_geo_impl!(MultiPolygonArray<O>);
iter_geo_impl!(WKBArray<O>);
