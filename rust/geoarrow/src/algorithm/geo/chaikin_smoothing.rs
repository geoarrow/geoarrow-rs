use std::sync::Arc;

use crate::array::*;
use crate::chunked_array::*;
use crate::datatypes::NativeType;
use crate::error::{GeoArrowError, Result};
use crate::trait_::ArrayAccessor;
use crate::NativeArray;
use geo::ChaikinSmoothing as _ChaikinSmoothing;
use geoarrow_schema::Dimension;

/// Smoothen `LineString`, `Polygon`, `MultiLineString` and `MultiPolygon` using Chaikins algorithm.
///
/// [Chaikins smoothing algorithm](http://www.idav.ucdavis.edu/education/CAGDNotes/Chaikins-Algorithm/Chaikins-Algorithm.html)
///
/// Each iteration of the smoothing doubles the number of vertices of the geometry, so in some
/// cases it may make sense to apply a simplification afterwards to remove insignificant
/// coordinates.
///
/// This implementation preserves the start and end vertices of an open linestring and
/// smoothes the corner between start and end of a closed linestring.
pub trait ChaikinSmoothing {
    type Output;

    /// create a new geometry with the Chaikin smoothing being
    /// applied `n_iterations` times.
    fn chaikin_smoothing(&self, n_iterations: u32) -> Self::Output;
}

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($type:ty, $builder_type:ty, $method:ident, $geo_type:ty) => {
        impl ChaikinSmoothing for $type {
            type Output = Self;

            fn chaikin_smoothing(&self, n_iterations: u32) -> Self::Output {
                let output_geoms: Vec<Option<$geo_type>> = self
                    .iter_geo()
                    .map(|maybe_g| {
                        maybe_g.map(|geom| geom.chaikin_smoothing(n_iterations.try_into().unwrap()))
                    })
                    .collect();

                <$builder_type>::$method(
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

impl ChaikinSmoothing for GeometryArray {
    type Output = Result<Self>;

    fn chaikin_smoothing(&self, n_iterations: u32) -> Self::Output {
        let output_geoms: Vec<Option<geo::Geometry>> = self
            .iter_geo()
            .map(|maybe_g| {
                maybe_g.map(|geom| geom.chaikin_smoothing(n_iterations.try_into().unwrap()))
            })
            .collect();

        Ok(GeometryBuilder::from_nullable_geometries(
            output_geoms.as_slice(),
            self.coord_type(),
            self.metadata().clone(),
            false,
        )?
        .finish())
    }
}

impl ChaikinSmoothing for &dyn NativeArray {
    type Output = Result<Arc<dyn NativeArray>>;

    fn chaikin_smoothing(&self, n_iterations: u32) -> Self::Output {
        use NativeType::*;

        let result: Arc<dyn NativeArray> = match self.data_type() {
            LineString(_) => Arc::new(self.as_line_string().chaikin_smoothing(n_iterations)),
            Polygon(_) => Arc::new(self.as_polygon().chaikin_smoothing(n_iterations)),
            MultiLineString(_) => {
                Arc::new(self.as_multi_line_string().chaikin_smoothing(n_iterations))
            }
            MultiPolygon(_) => Arc::new(self.as_multi_polygon().chaikin_smoothing(n_iterations)),
            Geometry(_) => Arc::new(self.as_geometry().chaikin_smoothing(n_iterations)?),
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}

macro_rules! impl_chunked {
    ($chunked_array:ty) => {
        impl ChaikinSmoothing for $chunked_array {
            type Output = Self;

            fn chaikin_smoothing(&self, n_iterations: u32) -> Self::Output {
                self.map(|chunk| chunk.chaikin_smoothing(n_iterations.into()))
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

impl ChaikinSmoothing for &dyn ChunkedNativeArray {
    type Output = Result<Arc<dyn ChunkedNativeArray>>;

    fn chaikin_smoothing(&self, n_iterations: u32) -> Self::Output {
        use NativeType::*;

        let result: Arc<dyn ChunkedNativeArray> = match self.data_type() {
            LineString(_) => Arc::new(self.as_line_string().chaikin_smoothing(n_iterations)),
            Polygon(_) => Arc::new(self.as_polygon().chaikin_smoothing(n_iterations)),
            MultiLineString(_) => {
                Arc::new(self.as_multi_line_string().chaikin_smoothing(n_iterations))
            }
            MultiPolygon(_) => Arc::new(self.as_multi_polygon().chaikin_smoothing(n_iterations)),
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}
