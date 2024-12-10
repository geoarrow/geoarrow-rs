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
            Geometry(_) => Ok(Arc::new(GeometryArray::from(array))),
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
            Geometry(_) => Ok(Arc::new(GeometryArray::from(array))),
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
            Geometry(_) => Ok(Arc::new(GeometryArray::from(array))),
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
            Geometry(_) => Ok(Arc::new(GeometryArray::from(array))),
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
            Geometry(_) => Ok(Arc::new(GeometryArray::from(array))),
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
            Geometry(_) => Ok(Arc::new(GeometryArray::from(array))),
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
            // Geometry(_) => Ok(Arc::new(GeometryArray::from(array))),
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
            Geometry(_) => Ok(Arc::new(GeometryArray::from(array))),
            dt => Err(GeoArrowError::General(format!(
                "invalid cast to type {dt:?}"
            ))),
        }
    }
}

impl Cast for GeometryArray {
    type Output = Result<Arc<dyn NativeArray>>;

    fn cast(&self, to_type: &NativeType) -> Self::Output {
        // TODO: validate dimension
        let array = self.to_coord_type(to_type.coord_type());
        let mixed_array = MixedGeometryArray::try_from(array)?;
        mixed_array.cast(to_type)
    }
}

impl Cast for &dyn NativeArray {
    type Output = Result<Arc<dyn NativeArray>>;

    fn cast(&self, to_type: &NativeType) -> Self::Output {
        // TODO: not working :/
        // if self.data_type() == to_type {
        //     return Ok(Arc::new(self.to_owned()));
        // }

        use NativeType::*;

        match self.data_type() {
            Point(_, _) => self.as_ref().as_point().cast(to_type),
            LineString(_, _) => self.as_ref().as_line_string().cast(to_type),
            Polygon(_, _) => self.as_ref().as_polygon().cast(to_type),
            MultiPoint(_, _) => self.as_ref().as_multi_point().cast(to_type),
            MultiLineString(_, _) => self.as_ref().as_multi_line_string().cast(to_type),
            MultiPolygon(_, _) => self.as_ref().as_multi_polygon().cast(to_type),
            Mixed(_, _) => self.as_ref().as_mixed().cast(to_type),
            GeometryCollection(_, _) => self.as_ref().as_geometry_collection().cast(to_type),
            Geometry(_) => self.as_ref().as_geometry().cast(to_type),
            _ => todo!(),
        }
    }
}

macro_rules! impl_chunked_cast {
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
                }

                use NativeType::*;

                let result: Arc<dyn ChunkedNativeArray> = match to_type {
                    Point(_, _) => impl_cast!(as_point),
                    LineString(_, _) => impl_cast!(as_line_string),
                    Polygon(_, _) => impl_cast!(as_polygon),
                    MultiPoint(_, _) => impl_cast!(as_multi_point),
                    MultiLineString(_, _) => impl_cast!(as_multi_line_string),
                    MultiPolygon(_, _) => impl_cast!(as_multi_polygon),
                    Mixed(_, _) => impl_cast!(as_mixed),
                    GeometryCollection(_, _) => impl_cast!(as_geometry_collection),
                    Rect(_) => impl_cast!(as_rect),
                    Geometry(_) => todo!("cast to unknown"),
                };
                Ok(result)
            }
        }
    };
}

impl_chunked_cast!(ChunkedPointArray);
impl_chunked_cast!(ChunkedRectArray);
impl_chunked_cast!(&dyn ChunkedNativeArray);
impl_chunked_cast!(ChunkedLineStringArray);
impl_chunked_cast!(ChunkedPolygonArray);
impl_chunked_cast!(ChunkedMultiPointArray);
impl_chunked_cast!(ChunkedMultiLineStringArray);
impl_chunked_cast!(ChunkedMultiPolygonArray);
impl_chunked_cast!(ChunkedMixedGeometryArray);
impl_chunked_cast!(ChunkedGeometryCollectionArray);
