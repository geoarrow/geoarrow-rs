//! Note: In the future, it may be possible to optimize some of these casts, e.g. from Point to
//! MultiPoint by only constructing a new offsets array, but you have to check that the coordinate
//! type is not casted!
//!
//! todo: have a set of "fast cast" functions, where you first try to fast cast and fall back to
//! slower copies if necessary. Can check that the coord type of the input and output is the same.

use std::sync::Arc;

use crate::array::*;
use crate::chunked_array::*;
use crate::datatypes::{Dimension, NativeType};
use crate::error::{GeoArrowError, Result};
use crate::NativeArray;

/// CastOptions provides a way to override the default cast behaviors
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CastOptions {
    /// how to handle cast failures, either return NULL (safe=true) or return ERR (safe=false)
    pub safe: bool,
}

impl Default for CastOptions {
    fn default() -> Self {
        Self { safe: true }
    }
}

/// Note: not currently used and outdated
#[allow(dead_code)]
fn can_cast_types(from_type: &NativeType, to_type: &NativeType) -> bool {
    if from_type == to_type {
        return true;
    }

    use Dimension::*;
    use NativeType::*;

    match (from_type, to_type) {
        (Point(_, XY), Point(_, XY) | MultiPoint(_, XY)) => true,
        (LineString(_, XY), LineString(_, XY) | MultiLineString(_, XY)) => true,
        (Polygon(_, XY), Polygon(_, XY) | MultiPolygon(_, XY)) => true,
        (MultiPoint(_, XY), MultiPoint(_, XY)) => true,
        (MultiLineString(_, XY), MultiLineString(_, XY)) => true,
        (MultiPolygon(_, XY), MultiPolygon(_, XY)) => true,
        _ => todo!(),
    }
}

pub trait Cast {
    type Output;

    /// Note: **does not currently implement dimension casts**
    fn cast(&self, to_type: &NativeType) -> Self::Output;
}

impl Cast for PointArray {
    type Output = Result<Arc<dyn NativeArray>>;

    fn cast(&self, to_type: &NativeType) -> Self::Output {
        use NativeType::*;

        let array = self.to_coord_type(to_type.coord_type());
        match to_type {
            Point(_, _) => Ok(Arc::new(array)),
            MultiPoint(_, _) => Ok(Arc::new(MultiPointArray::from(array))),
            Mixed(_, _) => Ok(Arc::new(MixedGeometryArray::from(array))),
            GeometryCollection(_, _) => Ok(Arc::new(GeometryCollectionArray::from(array))),
            dt => Err(GeoArrowError::General(format!(
                "invalid cast to type {dt:?}"
            ))),
        }
    }
}

impl Cast for LineStringArray {
    type Output = Result<Arc<dyn NativeArray>>;

    fn cast(&self, to_type: &NativeType) -> Self::Output {
        use NativeType::*;

        let array = self.to_coord_type(to_type.coord_type());

        match to_type {
            LineString(_, _) => Ok(Arc::new(array)),
            MultiLineString(_, _) => Ok(Arc::new(MultiLineStringArray::from(array))),
            Mixed(_, _) => Ok(Arc::new(MixedGeometryArray::from(array))),
            GeometryCollection(_, _) => Ok(Arc::new(GeometryCollectionArray::from(array))),
            dt => Err(GeoArrowError::General(format!(
                "invalid cast to type {dt:?}"
            ))),
        }
    }
}

impl Cast for PolygonArray {
    type Output = Result<Arc<dyn NativeArray>>;

    fn cast(&self, to_type: &NativeType) -> Self::Output {
        use NativeType::*;

        let array = self.to_coord_type(to_type.coord_type());

        match to_type {
            Polygon(_, _) => Ok(Arc::new(array)),
            MultiPolygon(_, _) => Ok(Arc::new(MultiPolygonArray::from(array))),
            Mixed(_, _) => Ok(Arc::new(MixedGeometryArray::from(array))),
            GeometryCollection(_, _) => Ok(Arc::new(GeometryCollectionArray::from(array))),
            dt => Err(GeoArrowError::General(format!(
                "invalid cast to type {dt:?}"
            ))),
        }
    }
}

impl Cast for MultiPointArray {
    type Output = Result<Arc<dyn NativeArray>>;

    fn cast(&self, to_type: &NativeType) -> Self::Output {
        use NativeType::*;

        let array = self.to_coord_type(to_type.coord_type());

        match to_type {
            Point(_, _) => Ok(Arc::new(PointArray::try_from(array)?)),
            MultiPoint(_, _) => Ok(Arc::new(array)),
            Mixed(_, _) => Ok(Arc::new(MixedGeometryArray::from(array))),
            GeometryCollection(_, _) => Ok(Arc::new(GeometryCollectionArray::from(array))),
            dt => Err(GeoArrowError::General(format!(
                "invalid cast to type {dt:?}"
            ))),
        }
    }
}

impl Cast for MultiLineStringArray {
    type Output = Result<Arc<dyn NativeArray>>;

    fn cast(&self, to_type: &NativeType) -> Self::Output {
        use NativeType::*;

        let array = self.to_coord_type(to_type.coord_type());

        match to_type {
            LineString(_, _) => Ok(Arc::new(LineStringArray::try_from(array)?)),
            Mixed(_, _) => Ok(Arc::new(MixedGeometryArray::from(array))),
            GeometryCollection(_, _) => Ok(Arc::new(GeometryCollectionArray::from(array))),
            dt => Err(GeoArrowError::General(format!(
                "invalid cast to type {dt:?}"
            ))),
        }
    }
}

impl Cast for MultiPolygonArray {
    type Output = Result<Arc<dyn NativeArray>>;

    fn cast(&self, to_type: &NativeType) -> Self::Output {
        use NativeType::*;

        let array = self.to_coord_type(to_type.coord_type());

        match to_type {
            Polygon(_, _) => Ok(Arc::new(PolygonArray::try_from(array)?)),
            Mixed(_, _) => Ok(Arc::new(MixedGeometryArray::from(array))),
            GeometryCollection(_, _) => Ok(Arc::new(GeometryCollectionArray::from(array))),
            dt => Err(GeoArrowError::General(format!(
                "invalid cast to type {dt:?}"
            ))),
        }
    }
}

impl Cast for MixedGeometryArray {
    type Output = Result<Arc<dyn NativeArray>>;

    fn cast(&self, to_type: &NativeType) -> Self::Output {
        use NativeType::*;

        let array = self.to_coord_type(to_type.coord_type());

        match to_type {
            Point(_, _) => Ok(Arc::new(PointArray::try_from(array)?)),
            LineString(_, _) => Ok(Arc::new(LineStringArray::try_from(array)?)),
            Polygon(_, _) => Ok(Arc::new(PolygonArray::try_from(array)?)),
            MultiPoint(_, _) => Ok(Arc::new(MultiPointArray::try_from(array)?)),
            MultiLineString(_, _) => Ok(Arc::new(MultiLineStringArray::try_from(array)?)),
            MultiPolygon(_, _) => Ok(Arc::new(MultiPolygonArray::try_from(array)?)),
            Mixed(_, _) => Ok(Arc::new(array)),
            GeometryCollection(_, _) => Ok(Arc::new(GeometryCollectionArray::from(array))),
            dt => Err(GeoArrowError::General(format!(
                "invalid cast to type {dt:?}"
            ))),
        }
    }
}

impl Cast for GeometryCollectionArray {
    type Output = Result<Arc<dyn NativeArray>>;

    fn cast(&self, to_type: &NativeType) -> Self::Output {
        use NativeType::*;

        let array = self.to_coord_type(to_type.coord_type());

        match to_type {
            Point(_, _) => Ok(Arc::new(PointArray::try_from(array)?)),
            LineString(_, _) => Ok(Arc::new(LineStringArray::try_from(array)?)),
            Polygon(_, _) => Ok(Arc::new(PolygonArray::try_from(array)?)),
            MultiPoint(_, _) => Ok(Arc::new(MultiPointArray::try_from(array)?)),
            MultiLineString(_, _) => Ok(Arc::new(MultiLineStringArray::try_from(array)?)),
            MultiPolygon(_, _) => Ok(Arc::new(MultiPolygonArray::try_from(array)?)),
            Mixed(_, _) => Ok(Arc::new(MixedGeometryArray::try_from(array)?)),
            GeometryCollection(_, _) => Ok(Arc::new(array)),
            dt => Err(GeoArrowError::General(format!(
                "invalid cast to type {dt:?}"
            ))),
        }
    }
}

impl Cast for &dyn NativeArray {
    type Output = Result<Arc<dyn NativeArray>>;

    fn cast(&self, to_type: &NativeType) -> Self::Output {
        // TODO: not working :/
        // if self.data_type() == to_type {
        //     return Ok(Arc::new(self.to_owned()));
        // }

        use Dimension::*;
        use NativeType::*;

        match self.data_type() {
            Point(_, XY) => self.as_ref().as_point().cast(to_type),
            LineString(_, XY) => self.as_ref().as_line_string().cast(to_type),
            Polygon(_, XY) => self.as_ref().as_polygon().cast(to_type),
            MultiPoint(_, XY) => self.as_ref().as_multi_point().cast(to_type),
            MultiLineString(_, XY) => self.as_ref().as_multi_line_string().cast(to_type),
            MultiPolygon(_, XY) => self.as_ref().as_multi_polygon().cast(to_type),
            Mixed(_, XY) => self.as_ref().as_mixed().cast(to_type),
            GeometryCollection(_, XY) => self.as_ref().as_geometry_collection().cast(to_type),
            Point(_, XYZ) => self.as_ref().as_point().cast(to_type),
            LineString(_, XYZ) => self.as_ref().as_line_string().cast(to_type),
            Polygon(_, XYZ) => self.as_ref().as_polygon().cast(to_type),
            MultiPoint(_, XYZ) => self.as_ref().as_multi_point().cast(to_type),
            MultiLineString(_, XYZ) => self.as_ref().as_multi_line_string().cast(to_type),
            MultiPolygon(_, XYZ) => self.as_ref().as_multi_polygon().cast(to_type),
            Mixed(_, XYZ) => self.as_ref().as_mixed().cast(to_type),
            GeometryCollection(_, XYZ) => self.as_ref().as_geometry_collection().cast(to_type),
            _ => todo!(),
        }
    }
}

macro_rules! impl_chunked_cast_non_generic {
    ($chunked_array:ty) => {
        impl Cast for $chunked_array {
            type Output = Result<Arc<dyn ChunkedNativeArray>>;

            fn cast(&self, to_type: &NativeType) -> Self::Output {
                macro_rules! impl_cast {
                    ($method:ident) => {
                        Arc::new(ChunkedGeometryArray::new(
                            self.geometry_chunks()
                                .iter()
                                .map(|chunk| {
                                    Ok(chunk.as_ref().cast(to_type)?.as_ref().$method().clone())
                                })
                                .collect::<Result<Vec<_>>>()?,
                        ))
                    };
                    ($method:ident, $dim:expr) => {
                        Arc::new(ChunkedGeometryArray::new(
                            self.geometry_chunks()
                                .iter()
                                .map(|chunk| {
                                    Ok(chunk
                                        .as_ref()
                                        .cast(to_type)?
                                        .as_ref()
                                        .$method::<$dim>()
                                        .clone())
                                })
                                .collect::<Result<Vec<_>>>()?,
                        ))
                    };
                }

                use Dimension::*;
                use NativeType::*;

                let result: Arc<dyn ChunkedNativeArray> = match to_type {
                    Point(_, XY) => impl_cast!(as_point, 2),
                    LineString(_, XY) => impl_cast!(as_line_string, 2),
                    Polygon(_, XY) => impl_cast!(as_polygon, 2),
                    MultiPoint(_, XY) => impl_cast!(as_multi_point, 2),
                    MultiLineString(_, XY) => impl_cast!(as_multi_line_string, 2),
                    MultiPolygon(_, XY) => impl_cast!(as_multi_polygon, 2),
                    Mixed(_, XY) => impl_cast!(as_mixed, 2),
                    GeometryCollection(_, XY) => impl_cast!(as_geometry_collection, 2),
                    Point(_, XYZ) => impl_cast!(as_point, 3),
                    LineString(_, XYZ) => impl_cast!(as_line_string, 3),
                    Polygon(_, XYZ) => impl_cast!(as_polygon, 3),
                    MultiPoint(_, XYZ) => impl_cast!(as_multi_point, 3),
                    MultiLineString(_, XYZ) => impl_cast!(as_multi_line_string, 3),
                    MultiPolygon(_, XYZ) => impl_cast!(as_multi_polygon, 3),
                    Mixed(_, XYZ) => impl_cast!(as_mixed, 3),
                    GeometryCollection(_, XYZ) => impl_cast!(as_geometry_collection, 3),
                    Rect(XY) => impl_cast!(as_rect, 2),
                    Rect(XYZ) => impl_cast!(as_rect, 3),
                };
                Ok(result)
            }
        }
    };
}

macro_rules! impl_chunked_cast_generic {
    ($chunked_array:ty) => {
        impl Cast for $chunked_array {
            type Output = Result<Arc<dyn ChunkedNativeArray>>;

            fn cast(&self, to_type: &NativeType) -> Self::Output {
                macro_rules! impl_cast {
                    ($method:ident) => {
                        Arc::new(ChunkedGeometryArray::new(
                            self.geometry_chunks()
                                .iter()
                                .map(|chunk| {
                                    Ok(chunk.as_ref().cast(to_type)?.as_ref().$method().clone())
                                })
                                .collect::<Result<Vec<_>>>()?,
                        ))
                    };
                    ($method:ident, $dim:expr) => {
                        Arc::new(ChunkedGeometryArray::new(
                            self.geometry_chunks()
                                .iter()
                                .map(|chunk| {
                                    Ok(chunk
                                        .as_ref()
                                        .cast(to_type)?
                                        .as_ref()
                                        .$method::<$dim>()
                                        .clone())
                                })
                                .collect::<Result<Vec<_>>>()?,
                        ))
                    };
                }

                use Dimension::*;
                use NativeType::*;

                let result: Arc<dyn ChunkedNativeArray> = match to_type {
                    Point(_, XY) => impl_cast!(as_point, 2),
                    LineString(_, XY) => impl_cast!(as_line_string, 2),
                    Polygon(_, XY) => impl_cast!(as_polygon, 2),
                    MultiPoint(_, XY) => impl_cast!(as_multi_point, 2),
                    MultiLineString(_, XY) => impl_cast!(as_multi_line_string, 2),
                    MultiPolygon(_, XY) => impl_cast!(as_multi_polygon, 2),
                    Mixed(_, XY) => impl_cast!(as_mixed, 2),
                    GeometryCollection(_, XY) => impl_cast!(as_geometry_collection, 2),
                    Point(_, XYZ) => impl_cast!(as_point, 3),
                    LineString(_, XYZ) => impl_cast!(as_line_string, 3),
                    Polygon(_, XYZ) => impl_cast!(as_polygon, 3),
                    MultiPoint(_, XYZ) => impl_cast!(as_multi_point, 3),
                    MultiLineString(_, XYZ) => impl_cast!(as_multi_line_string, 3),
                    MultiPolygon(_, XYZ) => impl_cast!(as_multi_polygon, 3),
                    Mixed(_, XYZ) => impl_cast!(as_mixed, 3),
                    GeometryCollection(_, XYZ) => impl_cast!(as_geometry_collection, 3),
                    Rect(XY) => impl_cast!(as_rect, 2),
                    Rect(XYZ) => impl_cast!(as_rect, 3),
                };
                Ok(result)
            }
        }
    };
}

impl_chunked_cast_non_generic!(ChunkedPointArray);
impl_chunked_cast_non_generic!(ChunkedRectArray);
impl_chunked_cast_non_generic!(&dyn ChunkedNativeArray);
impl_chunked_cast_generic!(ChunkedLineStringArray);
impl_chunked_cast_generic!(ChunkedPolygonArray);
impl_chunked_cast_generic!(ChunkedMultiPointArray);
impl_chunked_cast_generic!(ChunkedMultiLineStringArray);
impl_chunked_cast_generic!(ChunkedMultiPolygonArray);
impl_chunked_cast_generic!(ChunkedMixedGeometryArray);
impl_chunked_cast_generic!(ChunkedGeometryCollectionArray);
impl_chunked_cast_non_generic!(ChunkedPointArray);
impl_chunked_cast_non_generic!(ChunkedRectArray);
impl_chunked_cast_generic!(ChunkedLineStringArray);
impl_chunked_cast_generic!(ChunkedPolygonArray);
impl_chunked_cast_generic!(ChunkedMultiPointArray);
impl_chunked_cast_generic!(ChunkedMultiLineStringArray);
impl_chunked_cast_generic!(ChunkedMultiPolygonArray);
impl_chunked_cast_generic!(ChunkedMixedGeometryArray);
impl_chunked_cast_generic!(ChunkedGeometryCollectionArray);
