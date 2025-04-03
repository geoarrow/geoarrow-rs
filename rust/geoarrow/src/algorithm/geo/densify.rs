use std::sync::Arc;

use crate::array::*;
use crate::chunked_array::*;
use crate::datatypes::{Dimension, NativeType};
use crate::error::{GeoArrowError, Result};
use crate::trait_::ArrayAccessor;
use crate::NativeArray;
use geo::line_measures::Densify as _Densify;
use geo::{CoordFloat, Euclidean};
use num_traits::FromPrimitive;

/// Return a new linear geometry containing both existing and new interpolated coordinates with
/// a maximum distance of `max_distance` between them.
///
/// Note: `max_distance` must be greater than 0.
pub trait Densify {
    type Output;

    fn densify(&self, max_distance: f64) -> Self::Output;
}

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($type:ty, $builder_type:ty, $method:ident, $geo_type:ty) => {
        impl Densify for $type {
            type Output = $type;

            fn densify(&self, max_distance: f64) -> Self::Output {
                let output_geoms: Vec<Option<$geo_type>> = self
                    .iter_geo()
                    .map(|maybe_g| maybe_g.map(|geom| Euclidean.densify(&geom, max_distance)))
                    .collect();

                <$builder_type>::$method(
                    output_geoms.as_slice(),
                    Dimension::XY,
                    self.coord_type(),
                    self.metadata.clone(),
                )
                .finish()
            }
        }
    };
}

iter_geo_impl!(
    LineStringArray,
    LineStringBuilder,
    from_nullable_line_strings,
    geo::LineString
);
iter_geo_impl!(
    PolygonArray,
    PolygonBuilder,
    from_nullable_polygons,
    geo::Polygon
);
iter_geo_impl!(
    MultiLineStringArray,
    MultiLineStringBuilder,
    from_nullable_multi_line_strings,
    geo::MultiLineString
);
iter_geo_impl!(
    MultiPolygonArray,
    MultiPolygonBuilder,
    from_nullable_multi_polygons,
    geo::MultiPolygon
);

#[repr(transparent)]
struct GeometryDensifyWrapper<'a, T: CoordFloat>(&'a geo::Geometry<T>);

impl<F: geo::CoordFloat + FromPrimitive> GeometryDensifyWrapper<'_, F> {
    fn densify(&self, max_segment_length: F) -> geo::Geometry<F> {
        match &self.0 {
            geo::Geometry::Point(g) => geo::Geometry::Point(*g),
            geo::Geometry::LineString(g) => {
                geo::Geometry::LineString(Euclidean.densify(g, max_segment_length))
            }
            geo::Geometry::Polygon(g) => {
                geo::Geometry::Polygon(Euclidean.densify(g, max_segment_length))
            }
            geo::Geometry::MultiPoint(g) => geo::Geometry::MultiPoint(g.clone()),
            geo::Geometry::MultiLineString(g) => {
                geo::Geometry::MultiLineString(Euclidean.densify(g, max_segment_length))
            }
            geo::Geometry::MultiPolygon(g) => {
                geo::Geometry::MultiPolygon(Euclidean.densify(g, max_segment_length))
            }
            geo::Geometry::Triangle(g) => {
                geo::Geometry::Polygon(Euclidean.densify(g, max_segment_length))
            }
            geo::Geometry::Rect(g) => {
                geo::Geometry::Polygon(Euclidean.densify(g, max_segment_length))
            }
            geo::Geometry::Line(g) => {
                geo::Geometry::LineString(Euclidean.densify(g, max_segment_length))
            }
            geo::Geometry::GeometryCollection(g) => {
                let mut output = Vec::with_capacity(g.len());
                for inner_geom in g.iter() {
                    output.push(GeometryDensifyWrapper(inner_geom).densify(max_segment_length));
                }
                geo::Geometry::GeometryCollection(geo::GeometryCollection::new_from(output))
            }
        }
    }
}

impl Densify for GeometryArray {
    type Output = Result<Self>;

    fn densify(&self, max_distance: f64) -> Self::Output {
        let output_geoms: Vec<Option<geo::Geometry>> = self
            .iter_geo()
            .map(|maybe_g| maybe_g.map(|geom| GeometryDensifyWrapper(&geom).densify(max_distance)))
            .collect();

        Ok(GeometryBuilder::from_nullable_geometries(
            output_geoms.as_slice(),
            self.coord_type(),
            self.metadata.clone(),
            false,
        )?
        .finish())
    }
}

impl Densify for &dyn NativeArray {
    type Output = Result<Arc<dyn NativeArray>>;

    fn densify(&self, max_distance: f64) -> Self::Output {
        use NativeType::*;

        let result: Arc<dyn NativeArray> = match self.data_type() {
            LineString(_) => Arc::new(self.as_line_string().densify(max_distance)),
            Polygon(_) => Arc::new(self.as_polygon().densify(max_distance)),
            MultiLineString(_) => Arc::new(self.as_multi_line_string().densify(max_distance)),
            MultiPolygon(_) => Arc::new(self.as_multi_polygon().densify(max_distance)),
            Geometry(_) => Arc::new(self.as_geometry().densify(max_distance)?),
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}

macro_rules! impl_chunked {
    ($struct_name:ty) => {
        impl Densify for $struct_name {
            type Output = $struct_name;

            fn densify(&self, max_distance: f64) -> Self::Output {
                self.map(|chunk| chunk.densify(max_distance))
                    .try_into()
                    .unwrap()
            }
        }
    };
}

impl_chunked!(ChunkedLineStringArray);
impl_chunked!(ChunkedPolygonArray);
impl_chunked!(ChunkedMultiLineStringArray);
impl_chunked!(ChunkedMultiPolygonArray);

impl Densify for &dyn ChunkedNativeArray {
    type Output = Result<Arc<dyn ChunkedNativeArray>>;

    fn densify(&self, max_distance: f64) -> Self::Output {
        use NativeType::*;

        let result: Arc<dyn ChunkedNativeArray> = match self.data_type() {
            LineString(_) => Arc::new(self.as_line_string().densify(max_distance)),
            Polygon(_) => Arc::new(self.as_polygon().densify(max_distance)),
            MultiLineString(_) => Arc::new(self.as_multi_line_string().densify(max_distance)),
            MultiPolygon(_) => Arc::new(self.as_multi_polygon().densify(max_distance)),
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}
