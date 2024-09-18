//! Note: In the future, it may be possible to optimize some of these casts, e.g. from Point to
//! MultiPoint by only constructing a new offsets array, but you have to check that the coordinate
//! type is not casted!
//!
//! todo: have a set of "fast cast" functions, where you first try to fast cast and fall back to
//! slower copies if necessary. Can check that the coord type of the input and output is the same.

use std::sync::Arc;

use arrow_array::OffsetSizeTrait;

use crate::array::*;
use crate::chunked_array::*;
use crate::datatypes::{Dimension, GeoDataType};
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
fn can_cast_types(from_type: &GeoDataType, to_type: &GeoDataType) -> bool {
    if from_type == to_type {
        return true;
    }

    use Dimension::*;
    use GeoDataType::*;

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
    fn cast(&self, to_type: &GeoDataType) -> Self::Output;
}

impl<const D: usize> Cast for PointArray<D> {
    type Output = Result<Arc<dyn NativeArray>>;

    fn cast(&self, to_type: &GeoDataType) -> Self::Output {
        use GeoDataType::*;

        let array = self.to_coord_type(to_type.coord_type().unwrap());
        match to_type {
            Point(_, _) => Ok(Arc::new(array)),
            MultiPoint(_, _) => Ok(Arc::new(MultiPointArray::<i32, D>::from(array))),
            LargeMultiPoint(_, _) => Ok(Arc::new(MultiPointArray::<i64, D>::from(array))),
            Mixed(_, _) => Ok(Arc::new(MixedGeometryArray::<i32, D>::from(array))),
            LargeMixed(_, _) => Ok(Arc::new(MixedGeometryArray::<i64, D>::from(array))),
            GeometryCollection(_, _) => {
                Ok(Arc::new(GeometryCollectionArray::<i32, D>::from(array)))
            }
            LargeGeometryCollection(_, _) => {
                Ok(Arc::new(GeometryCollectionArray::<i64, D>::from(array)))
            }
            dt => Err(GeoArrowError::General(format!(
                "invalid cast to type {dt:?}"
            ))),
        }
    }
}

impl<O: OffsetSizeTrait, const D: usize> Cast for LineStringArray<O, D> {
    type Output = Result<Arc<dyn NativeArray>>;

    fn cast(&self, to_type: &GeoDataType) -> Self::Output {
        use GeoDataType::*;

        let array = self.to_coord_type(to_type.coord_type().unwrap());

        match to_type {
            LineString(_, _) => {
                if O::IS_LARGE {
                    Ok(Arc::new(array.to_small_offsets()?))
                } else {
                    Ok(Arc::new(array))
                }
            }
            LargeLineString(_, _) => {
                if O::IS_LARGE {
                    Ok(Arc::new(array))
                } else {
                    Ok(Arc::new(array.to_large_offsets()))
                }
            }
            MultiLineString(_, _) => {
                if O::IS_LARGE {
                    Ok(Arc::new(MultiLineStringArray::<i32, D>::from(
                        array.to_small_offsets()?,
                    )))
                } else {
                    Ok(Arc::new(MultiLineStringArray::<O, D>::from(array)))
                }
            }
            LargeMultiLineString(_, _) => {
                if O::IS_LARGE {
                    Ok(Arc::new(MultiLineStringArray::<O, D>::from(array)))
                } else {
                    Ok(Arc::new(MultiLineStringArray::<i64, D>::from(
                        array.to_large_offsets(),
                    )))
                }
            }
            Mixed(_, _) => {
                if O::IS_LARGE {
                    Ok(Arc::new(MixedGeometryArray::<i32, D>::from(
                        array.to_small_offsets()?,
                    )))
                } else {
                    Ok(Arc::new(MixedGeometryArray::<O, D>::from(array)))
                }
            }
            LargeMixed(_, _) => {
                if O::IS_LARGE {
                    Ok(Arc::new(MixedGeometryArray::<O, D>::from(array)))
                } else {
                    Ok(Arc::new(MixedGeometryArray::<i64, D>::from(
                        array.to_large_offsets(),
                    )))
                }
            }
            GeometryCollection(_, _) => {
                if O::IS_LARGE {
                    Ok(Arc::new(GeometryCollectionArray::<i32, D>::from(
                        array.to_small_offsets()?,
                    )))
                } else {
                    Ok(Arc::new(GeometryCollectionArray::<O, D>::from(array)))
                }
            }
            LargeGeometryCollection(_, _) => {
                if O::IS_LARGE {
                    Ok(Arc::new(GeometryCollectionArray::<O, D>::from(array)))
                } else {
                    Ok(Arc::new(GeometryCollectionArray::<i64, D>::from(
                        array.to_large_offsets(),
                    )))
                }
            }
            dt => Err(GeoArrowError::General(format!(
                "invalid cast to type {dt:?}"
            ))),
        }
    }
}

impl<O: OffsetSizeTrait, const D: usize> Cast for PolygonArray<O, D> {
    type Output = Result<Arc<dyn NativeArray>>;

    fn cast(&self, to_type: &GeoDataType) -> Self::Output {
        use GeoDataType::*;

        let array = self.to_coord_type(to_type.coord_type().unwrap());

        match to_type {
            Polygon(_, _) => {
                if O::IS_LARGE {
                    Ok(Arc::new(array.to_small_offsets()?))
                } else {
                    Ok(Arc::new(array))
                }
            }
            LargePolygon(_, _) => {
                if O::IS_LARGE {
                    Ok(Arc::new(array))
                } else {
                    Ok(Arc::new(array.to_large_offsets()))
                }
            }
            MultiPolygon(_, _) => {
                if O::IS_LARGE {
                    Ok(Arc::new(MultiPolygonArray::<i32, D>::from(
                        array.to_small_offsets()?,
                    )))
                } else {
                    Ok(Arc::new(MultiPolygonArray::<O, D>::from(array)))
                }
            }
            LargeMultiPolygon(_, _) => {
                if O::IS_LARGE {
                    Ok(Arc::new(MultiLineStringArray::<O, D>::from(array)))
                } else {
                    Ok(Arc::new(MultiLineStringArray::<i64, D>::from(
                        array.to_large_offsets(),
                    )))
                }
            }
            Mixed(_, _) => {
                if O::IS_LARGE {
                    Ok(Arc::new(MixedGeometryArray::<i32, D>::from(
                        array.to_small_offsets()?,
                    )))
                } else {
                    Ok(Arc::new(MixedGeometryArray::<O, D>::from(array)))
                }
            }
            LargeMixed(_, _) => {
                if O::IS_LARGE {
                    Ok(Arc::new(MixedGeometryArray::<O, D>::from(array)))
                } else {
                    Ok(Arc::new(MixedGeometryArray::<i64, D>::from(
                        array.to_large_offsets(),
                    )))
                }
            }
            GeometryCollection(_, _) => {
                if O::IS_LARGE {
                    Ok(Arc::new(GeometryCollectionArray::<i32, D>::from(
                        array.to_small_offsets()?,
                    )))
                } else {
                    Ok(Arc::new(GeometryCollectionArray::<O, D>::from(array)))
                }
            }
            LargeGeometryCollection(_, _) => {
                if O::IS_LARGE {
                    Ok(Arc::new(GeometryCollectionArray::<O, D>::from(array)))
                } else {
                    Ok(Arc::new(GeometryCollectionArray::<i64, D>::from(
                        array.to_large_offsets(),
                    )))
                }
            }
            dt => Err(GeoArrowError::General(format!(
                "invalid cast to type {dt:?}"
            ))),
        }
    }
}

impl<O: OffsetSizeTrait, const D: usize> Cast for MultiPointArray<O, D> {
    type Output = Result<Arc<dyn NativeArray>>;

    fn cast(&self, to_type: &GeoDataType) -> Self::Output {
        use GeoDataType::*;

        let array = self.to_coord_type(to_type.coord_type().unwrap());

        match to_type {
            Point(_, _) => Ok(Arc::new(PointArray::<D>::try_from(array)?)),
            MultiPoint(_, _) => {
                if O::IS_LARGE {
                    Ok(Arc::new(array.to_small_offsets()?))
                } else {
                    Ok(Arc::new(array))
                }
            }
            LargeMultiPoint(_, _) => {
                if O::IS_LARGE {
                    Ok(Arc::new(array))
                } else {
                    Ok(Arc::new(array.to_large_offsets()))
                }
            }
            Mixed(_, _) => {
                if O::IS_LARGE {
                    Ok(Arc::new(MixedGeometryArray::<i32, D>::from(
                        array.to_small_offsets()?,
                    )))
                } else {
                    Ok(Arc::new(MixedGeometryArray::<O, D>::from(array)))
                }
            }
            LargeMixed(_, _) => {
                if O::IS_LARGE {
                    Ok(Arc::new(MixedGeometryArray::<O, D>::from(array)))
                } else {
                    Ok(Arc::new(MixedGeometryArray::<i64, D>::from(
                        array.to_large_offsets(),
                    )))
                }
            }
            GeometryCollection(_, _) => {
                if O::IS_LARGE {
                    Ok(Arc::new(GeometryCollectionArray::<i32, D>::from(
                        array.to_small_offsets()?,
                    )))
                } else {
                    Ok(Arc::new(GeometryCollectionArray::<O, D>::from(array)))
                }
            }
            LargeGeometryCollection(_, _) => {
                if O::IS_LARGE {
                    Ok(Arc::new(GeometryCollectionArray::<O, D>::from(array)))
                } else {
                    Ok(Arc::new(GeometryCollectionArray::<i64, D>::from(
                        array.to_large_offsets(),
                    )))
                }
            }
            dt => Err(GeoArrowError::General(format!(
                "invalid cast to type {dt:?}"
            ))),
        }
    }
}

impl<O: OffsetSizeTrait, const D: usize> Cast for MultiLineStringArray<O, D> {
    type Output = Result<Arc<dyn NativeArray>>;

    fn cast(&self, to_type: &GeoDataType) -> Self::Output {
        use GeoDataType::*;

        let array = self.to_coord_type(to_type.coord_type().unwrap());

        match to_type {
            LineString(_, _) => {
                if O::IS_LARGE {
                    Ok(Arc::new(LineStringArray::<i32, D>::try_from(
                        array.to_small_offsets()?,
                    )?))
                } else {
                    Ok(Arc::new(LineStringArray::<O, D>::try_from(array)?))
                }
            }
            LargeLineString(_, _) => {
                if O::IS_LARGE {
                    Ok(Arc::new(LineStringArray::<O, D>::try_from(array)?))
                } else {
                    Ok(Arc::new(LineStringArray::<i64, D>::try_from(
                        array.to_large_offsets(),
                    )?))
                }
            }
            Mixed(_, _) => {
                if O::IS_LARGE {
                    Ok(Arc::new(MixedGeometryArray::<i32, D>::from(
                        array.to_small_offsets()?,
                    )))
                } else {
                    Ok(Arc::new(MixedGeometryArray::<O, D>::from(array)))
                }
            }
            LargeMixed(_, _) => {
                if O::IS_LARGE {
                    Ok(Arc::new(MixedGeometryArray::<O, D>::from(array)))
                } else {
                    Ok(Arc::new(MixedGeometryArray::<i64, D>::from(
                        array.to_large_offsets(),
                    )))
                }
            }
            GeometryCollection(_, _) => {
                if O::IS_LARGE {
                    Ok(Arc::new(GeometryCollectionArray::<i32, D>::from(
                        array.to_small_offsets()?,
                    )))
                } else {
                    Ok(Arc::new(GeometryCollectionArray::<O, D>::from(array)))
                }
            }
            LargeGeometryCollection(_, _) => {
                if O::IS_LARGE {
                    Ok(Arc::new(GeometryCollectionArray::<O, D>::from(array)))
                } else {
                    Ok(Arc::new(GeometryCollectionArray::<i64, D>::from(
                        array.to_large_offsets(),
                    )))
                }
            }
            dt => Err(GeoArrowError::General(format!(
                "invalid cast to type {dt:?}"
            ))),
        }
    }
}

impl<O: OffsetSizeTrait, const D: usize> Cast for MultiPolygonArray<O, D> {
    type Output = Result<Arc<dyn NativeArray>>;

    fn cast(&self, to_type: &GeoDataType) -> Self::Output {
        use GeoDataType::*;

        let array = self.to_coord_type(to_type.coord_type().unwrap());

        match to_type {
            Polygon(_, _) => {
                if O::IS_LARGE {
                    Ok(Arc::new(PolygonArray::<i32, D>::try_from(
                        array.to_small_offsets()?,
                    )?))
                } else {
                    Ok(Arc::new(PolygonArray::<O, D>::try_from(array)?))
                }
            }
            LargePolygon(_, _) => {
                if O::IS_LARGE {
                    Ok(Arc::new(PolygonArray::<O, D>::try_from(array)?))
                } else {
                    Ok(Arc::new(PolygonArray::<i64, D>::try_from(
                        array.to_large_offsets(),
                    )?))
                }
            }
            Mixed(_, _) => {
                if O::IS_LARGE {
                    Ok(Arc::new(MixedGeometryArray::<i32, D>::from(
                        array.to_small_offsets()?,
                    )))
                } else {
                    Ok(Arc::new(MixedGeometryArray::<O, D>::from(array)))
                }
            }
            LargeMixed(_, _) => {
                if O::IS_LARGE {
                    Ok(Arc::new(MixedGeometryArray::<O, D>::from(array)))
                } else {
                    Ok(Arc::new(MixedGeometryArray::<i64, D>::from(
                        array.to_large_offsets(),
                    )))
                }
            }
            GeometryCollection(_, _) => {
                if O::IS_LARGE {
                    Ok(Arc::new(GeometryCollectionArray::<i32, D>::from(
                        array.to_small_offsets()?,
                    )))
                } else {
                    Ok(Arc::new(GeometryCollectionArray::<O, D>::from(array)))
                }
            }
            LargeGeometryCollection(_, _) => {
                if O::IS_LARGE {
                    Ok(Arc::new(GeometryCollectionArray::<O, D>::from(array)))
                } else {
                    Ok(Arc::new(GeometryCollectionArray::<i64, D>::from(
                        array.to_large_offsets(),
                    )))
                }
            }
            dt => Err(GeoArrowError::General(format!(
                "invalid cast to type {dt:?}"
            ))),
        }
    }
}

impl<O: OffsetSizeTrait, const D: usize> Cast for MixedGeometryArray<O, D> {
    type Output = Result<Arc<dyn NativeArray>>;

    fn cast(&self, to_type: &GeoDataType) -> Self::Output {
        use GeoDataType::*;

        let array = self.to_coord_type(to_type.coord_type().unwrap());

        match to_type {
            Point(_, _) => Ok(Arc::new(PointArray::try_from(array)?)),
            LineString(_, _) => {
                if O::IS_LARGE {
                    Ok(Arc::new(LineStringArray::<i32, D>::try_from(
                        array.to_small_offsets()?,
                    )?))
                } else {
                    Ok(Arc::new(LineStringArray::<O, D>::try_from(array)?))
                }
            }
            LargeLineString(_, _) => {
                if O::IS_LARGE {
                    Ok(Arc::new(LineStringArray::<O, D>::try_from(array)?))
                } else {
                    Ok(Arc::new(LineStringArray::<i64, D>::try_from(
                        array.to_large_offsets(),
                    )?))
                }
            }
            Polygon(_, _) => {
                if O::IS_LARGE {
                    Ok(Arc::new(PolygonArray::<i32, D>::try_from(
                        array.to_small_offsets()?,
                    )?))
                } else {
                    Ok(Arc::new(PolygonArray::<O, D>::try_from(array)?))
                }
            }
            LargePolygon(_, _) => {
                if O::IS_LARGE {
                    Ok(Arc::new(PolygonArray::<O, D>::try_from(array)?))
                } else {
                    Ok(Arc::new(PolygonArray::<i64, D>::try_from(
                        array.to_large_offsets(),
                    )?))
                }
            }
            MultiPoint(_, _) => {
                if O::IS_LARGE {
                    Ok(Arc::new(MultiPointArray::<i32, D>::try_from(
                        array.to_small_offsets()?,
                    )?))
                } else {
                    Ok(Arc::new(MultiPointArray::<O, D>::try_from(array)?))
                }
            }
            LargeMultiPoint(_, _) => {
                if O::IS_LARGE {
                    Ok(Arc::new(MultiPointArray::<O, D>::try_from(array)?))
                } else {
                    Ok(Arc::new(MultiPointArray::<i64, D>::try_from(
                        array.to_large_offsets(),
                    )?))
                }
            }
            MultiLineString(_, _) => {
                if O::IS_LARGE {
                    Ok(Arc::new(MultiLineStringArray::<i32, D>::try_from(
                        array.to_small_offsets()?,
                    )?))
                } else {
                    Ok(Arc::new(MultiLineStringArray::<O, D>::try_from(array)?))
                }
            }
            LargeMultiLineString(_, _) => {
                if O::IS_LARGE {
                    Ok(Arc::new(MultiLineStringArray::<O, D>::try_from(array)?))
                } else {
                    Ok(Arc::new(MultiLineStringArray::<i64, D>::try_from(
                        array.to_large_offsets(),
                    )?))
                }
            }
            MultiPolygon(_, _) => {
                if O::IS_LARGE {
                    Ok(Arc::new(MultiPolygonArray::<i32, D>::try_from(
                        array.to_small_offsets()?,
                    )?))
                } else {
                    Ok(Arc::new(MultiPolygonArray::<O, D>::try_from(array)?))
                }
            }
            LargeMultiPolygon(_, _) => {
                if O::IS_LARGE {
                    Ok(Arc::new(MultiPolygonArray::<O, D>::try_from(array)?))
                } else {
                    Ok(Arc::new(MultiPolygonArray::<i64, D>::try_from(
                        array.to_large_offsets(),
                    )?))
                }
            }
            Mixed(_, _) => {
                if O::IS_LARGE {
                    Ok(Arc::new(array.to_small_offsets()?))
                } else {
                    Ok(Arc::new(array))
                }
            }
            LargeMixed(_, _) => {
                if O::IS_LARGE {
                    Ok(Arc::new(array))
                } else {
                    Ok(Arc::new(array.to_large_offsets()))
                }
            }
            GeometryCollection(_, _) => {
                if O::IS_LARGE {
                    Ok(Arc::new(GeometryCollectionArray::<i32, D>::from(
                        array.to_small_offsets()?,
                    )))
                } else {
                    Ok(Arc::new(GeometryCollectionArray::<O, D>::from(array)))
                }
            }
            LargeGeometryCollection(_, _) => {
                if O::IS_LARGE {
                    Ok(Arc::new(GeometryCollectionArray::<O, D>::from(array)))
                } else {
                    Ok(Arc::new(GeometryCollectionArray::<i64, D>::from(
                        array.to_large_offsets(),
                    )))
                }
            }
            dt => Err(GeoArrowError::General(format!(
                "invalid cast to type {dt:?}"
            ))),
        }
    }
}

impl<O: OffsetSizeTrait, const D: usize> Cast for GeometryCollectionArray<O, D> {
    type Output = Result<Arc<dyn NativeArray>>;

    fn cast(&self, to_type: &GeoDataType) -> Self::Output {
        use GeoDataType::*;

        let array = self.to_coord_type(to_type.coord_type().unwrap());

        match to_type {
            Point(_, _) => Ok(Arc::new(PointArray::try_from(array)?)),
            LineString(_, _) => {
                if O::IS_LARGE {
                    Ok(Arc::new(LineStringArray::<i32, D>::try_from(
                        array.to_small_offsets()?,
                    )?))
                } else {
                    Ok(Arc::new(LineStringArray::<O, D>::try_from(array)?))
                }
            }
            LargeLineString(_, _) => {
                if O::IS_LARGE {
                    Ok(Arc::new(LineStringArray::<O, D>::try_from(array)?))
                } else {
                    Ok(Arc::new(LineStringArray::<i64, D>::try_from(
                        array.to_large_offsets(),
                    )?))
                }
            }
            Polygon(_, _) => {
                if O::IS_LARGE {
                    Ok(Arc::new(PolygonArray::<i32, D>::try_from(
                        array.to_small_offsets()?,
                    )?))
                } else {
                    Ok(Arc::new(PolygonArray::<O, D>::try_from(array)?))
                }
            }
            LargePolygon(_, _) => {
                if O::IS_LARGE {
                    Ok(Arc::new(PolygonArray::<O, D>::try_from(array)?))
                } else {
                    Ok(Arc::new(PolygonArray::<i64, D>::try_from(
                        array.to_large_offsets(),
                    )?))
                }
            }
            MultiPoint(_, _) => {
                if O::IS_LARGE {
                    Ok(Arc::new(MultiPointArray::<i32, D>::try_from(
                        array.to_small_offsets()?,
                    )?))
                } else {
                    Ok(Arc::new(MultiPointArray::<O, D>::try_from(array)?))
                }
            }
            LargeMultiPoint(_, _) => {
                if O::IS_LARGE {
                    Ok(Arc::new(MultiPointArray::<O, D>::try_from(array)?))
                } else {
                    Ok(Arc::new(MultiPointArray::<i64, D>::try_from(
                        array.to_large_offsets(),
                    )?))
                }
            }
            MultiLineString(_, _) => {
                if O::IS_LARGE {
                    Ok(Arc::new(MultiLineStringArray::<i32, D>::try_from(
                        array.to_small_offsets()?,
                    )?))
                } else {
                    Ok(Arc::new(MultiLineStringArray::<O, D>::try_from(array)?))
                }
            }
            LargeMultiLineString(_, _) => {
                if O::IS_LARGE {
                    Ok(Arc::new(MultiLineStringArray::<O, D>::try_from(array)?))
                } else {
                    Ok(Arc::new(MultiLineStringArray::<i64, D>::try_from(
                        array.to_large_offsets(),
                    )?))
                }
            }
            MultiPolygon(_, _) => {
                if O::IS_LARGE {
                    Ok(Arc::new(MultiPolygonArray::<i32, D>::try_from(
                        array.to_small_offsets()?,
                    )?))
                } else {
                    Ok(Arc::new(MultiPolygonArray::<O, D>::try_from(array)?))
                }
            }
            LargeMultiPolygon(_, _) => {
                if O::IS_LARGE {
                    Ok(Arc::new(MultiPolygonArray::<O, D>::try_from(array)?))
                } else {
                    Ok(Arc::new(MultiPolygonArray::<i64, D>::try_from(
                        array.to_large_offsets(),
                    )?))
                }
            }
            Mixed(_, _) => {
                if O::IS_LARGE {
                    Ok(Arc::new(MixedGeometryArray::<i32, D>::try_from(
                        array.to_small_offsets()?,
                    )?))
                } else {
                    Ok(Arc::new(MixedGeometryArray::<O, D>::try_from(array)?))
                }
            }
            LargeMixed(_, _) => {
                if O::IS_LARGE {
                    Ok(Arc::new(MixedGeometryArray::<O, D>::try_from(array)?))
                } else {
                    Ok(Arc::new(MixedGeometryArray::<i64, D>::try_from(
                        array.to_large_offsets(),
                    )?))
                }
            }
            GeometryCollection(_, _) => {
                if O::IS_LARGE {
                    Ok(Arc::new(array.to_small_offsets()?))
                } else {
                    Ok(Arc::new(array))
                }
            }
            LargeGeometryCollection(_, _) => {
                if O::IS_LARGE {
                    Ok(Arc::new(array))
                } else {
                    Ok(Arc::new(array.to_large_offsets()))
                }
            }
            dt => Err(GeoArrowError::General(format!(
                "invalid cast to type {dt:?}"
            ))),
        }
    }
}

impl Cast for &dyn NativeArray {
    type Output = Result<Arc<dyn NativeArray>>;

    fn cast(&self, to_type: &GeoDataType) -> Self::Output {
        // TODO: not working :/
        // if self.data_type() == to_type {
        //     return Ok(Arc::new(self.to_owned()));
        // }

        use Dimension::*;
        use GeoDataType::*;

        match self.data_type() {
            Point(_, XY) => self.as_ref().as_point::<2>().cast(to_type),
            LineString(_, XY) => self.as_ref().as_line_string::<2>().cast(to_type),
            LargeLineString(_, XY) => self.as_ref().as_large_line_string::<2>().cast(to_type),
            Polygon(_, XY) => self.as_ref().as_polygon::<2>().cast(to_type),
            LargePolygon(_, XY) => self.as_ref().as_large_polygon::<2>().cast(to_type),
            MultiPoint(_, XY) => self.as_ref().as_multi_point::<2>().cast(to_type),
            LargeMultiPoint(_, XY) => self.as_ref().as_large_multi_point::<2>().cast(to_type),
            MultiLineString(_, XY) => self.as_ref().as_multi_line_string::<2>().cast(to_type),
            LargeMultiLineString(_, XY) => self
                .as_ref()
                .as_large_multi_line_string::<2>()
                .cast(to_type),
            MultiPolygon(_, XY) => self.as_ref().as_multi_polygon::<2>().cast(to_type),
            LargeMultiPolygon(_, XY) => self.as_ref().as_large_multi_polygon::<2>().cast(to_type),
            Mixed(_, XY) => self.as_ref().as_mixed::<2>().cast(to_type),
            LargeMixed(_, XY) => self.as_ref().as_large_mixed::<2>().cast(to_type),
            GeometryCollection(_, XY) => self.as_ref().as_geometry_collection::<2>().cast(to_type),
            LargeGeometryCollection(_, XY) => self
                .as_ref()
                .as_large_geometry_collection::<2>()
                .cast(to_type),
            Point(_, XYZ) => self.as_ref().as_point::<3>().cast(to_type),
            LineString(_, XYZ) => self.as_ref().as_line_string::<3>().cast(to_type),
            LargeLineString(_, XYZ) => self.as_ref().as_large_line_string::<3>().cast(to_type),
            Polygon(_, XYZ) => self.as_ref().as_polygon::<3>().cast(to_type),
            LargePolygon(_, XYZ) => self.as_ref().as_large_polygon::<3>().cast(to_type),
            MultiPoint(_, XYZ) => self.as_ref().as_multi_point::<3>().cast(to_type),
            LargeMultiPoint(_, XYZ) => self.as_ref().as_large_multi_point::<3>().cast(to_type),
            MultiLineString(_, XYZ) => self.as_ref().as_multi_line_string::<3>().cast(to_type),
            LargeMultiLineString(_, XYZ) => self
                .as_ref()
                .as_large_multi_line_string::<3>()
                .cast(to_type),
            MultiPolygon(_, XYZ) => self.as_ref().as_multi_polygon::<3>().cast(to_type),
            LargeMultiPolygon(_, XYZ) => self.as_ref().as_large_multi_polygon::<3>().cast(to_type),
            Mixed(_, XYZ) => self.as_ref().as_mixed::<3>().cast(to_type),
            LargeMixed(_, XYZ) => self.as_ref().as_large_mixed::<3>().cast(to_type),
            GeometryCollection(_, XYZ) => self.as_ref().as_geometry_collection::<3>().cast(to_type),
            LargeGeometryCollection(_, XYZ) => self
                .as_ref()
                .as_large_geometry_collection::<3>()
                .cast(to_type),
            _ => todo!(),
        }
    }
}

macro_rules! impl_chunked_cast_non_generic {
    ($chunked_array:ty) => {
        impl Cast for $chunked_array {
            type Output = Result<Arc<dyn ChunkedNativeArray>>;

            fn cast(&self, to_type: &GeoDataType) -> Self::Output {
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
                use GeoDataType::*;

                let result: Arc<dyn ChunkedNativeArray> = match to_type {
                    Point(_, XY) => impl_cast!(as_point, 2),
                    LineString(_, XY) => impl_cast!(as_line_string, 2),
                    LargeLineString(_, XY) => impl_cast!(as_large_line_string, 2),
                    Polygon(_, XY) => impl_cast!(as_polygon, 2),
                    LargePolygon(_, XY) => impl_cast!(as_large_polygon, 2),
                    MultiPoint(_, XY) => impl_cast!(as_multi_point, 2),
                    LargeMultiPoint(_, XY) => impl_cast!(as_large_multi_point, 2),
                    MultiLineString(_, XY) => impl_cast!(as_multi_line_string, 2),
                    LargeMultiLineString(_, XY) => {
                        impl_cast!(as_large_multi_line_string, 2)
                    }
                    MultiPolygon(_, XY) => impl_cast!(as_multi_polygon, 2),
                    LargeMultiPolygon(_, XY) => impl_cast!(as_large_multi_polygon, 2),
                    Mixed(_, XY) => impl_cast!(as_mixed, 2),
                    LargeMixed(_, XY) => impl_cast!(as_large_mixed, 2),
                    GeometryCollection(_, XY) => impl_cast!(as_geometry_collection, 2),
                    LargeGeometryCollection(_, XY) => {
                        impl_cast!(as_large_geometry_collection, 2)
                    }
                    Point(_, XYZ) => impl_cast!(as_point, 3),
                    LineString(_, XYZ) => impl_cast!(as_line_string, 3),
                    LargeLineString(_, XYZ) => impl_cast!(as_large_line_string, 3),
                    Polygon(_, XYZ) => impl_cast!(as_polygon, 3),
                    LargePolygon(_, XYZ) => impl_cast!(as_large_polygon, 3),
                    MultiPoint(_, XYZ) => impl_cast!(as_multi_point, 3),
                    LargeMultiPoint(_, XYZ) => impl_cast!(as_large_multi_point, 3),
                    MultiLineString(_, XYZ) => impl_cast!(as_multi_line_string, 3),
                    LargeMultiLineString(_, XYZ) => {
                        impl_cast!(as_large_multi_line_string, 3)
                    }
                    MultiPolygon(_, XYZ) => impl_cast!(as_multi_polygon, 3),
                    LargeMultiPolygon(_, XYZ) => impl_cast!(as_large_multi_polygon, 3),
                    Mixed(_, XYZ) => impl_cast!(as_mixed, 3),
                    LargeMixed(_, XYZ) => impl_cast!(as_large_mixed, 3),
                    GeometryCollection(_, XYZ) => impl_cast!(as_geometry_collection, 3),
                    LargeGeometryCollection(_, XYZ) => {
                        impl_cast!(as_large_geometry_collection, 3)
                    }
                    WKB => impl_cast!(as_wkb),
                    LargeWKB => impl_cast!(as_large_wkb),
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
        impl<O: OffsetSizeTrait> Cast for $chunked_array {
            type Output = Result<Arc<dyn ChunkedNativeArray>>;

            fn cast(&self, to_type: &GeoDataType) -> Self::Output {
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
                use GeoDataType::*;

                let result: Arc<dyn ChunkedNativeArray> = match to_type {
                    Point(_, XY) => impl_cast!(as_point, 2),
                    LineString(_, XY) => impl_cast!(as_line_string, 2),
                    LargeLineString(_, XY) => impl_cast!(as_large_line_string, 2),
                    Polygon(_, XY) => impl_cast!(as_polygon, 2),
                    LargePolygon(_, XY) => impl_cast!(as_large_polygon, 2),
                    MultiPoint(_, XY) => impl_cast!(as_multi_point, 2),
                    LargeMultiPoint(_, XY) => impl_cast!(as_large_multi_point, 2),
                    MultiLineString(_, XY) => impl_cast!(as_multi_line_string, 2),
                    LargeMultiLineString(_, XY) => {
                        impl_cast!(as_large_multi_line_string, 2)
                    }
                    MultiPolygon(_, XY) => impl_cast!(as_multi_polygon, 2),
                    LargeMultiPolygon(_, XY) => impl_cast!(as_large_multi_polygon, 2),
                    Mixed(_, XY) => impl_cast!(as_mixed, 2),
                    LargeMixed(_, XY) => impl_cast!(as_large_mixed, 2),
                    GeometryCollection(_, XY) => impl_cast!(as_geometry_collection, 2),
                    LargeGeometryCollection(_, XY) => {
                        impl_cast!(as_large_geometry_collection, 2)
                    }
                    Point(_, XYZ) => impl_cast!(as_point, 3),
                    LineString(_, XYZ) => impl_cast!(as_line_string, 3),
                    LargeLineString(_, XYZ) => impl_cast!(as_large_line_string, 3),
                    Polygon(_, XYZ) => impl_cast!(as_polygon, 3),
                    LargePolygon(_, XYZ) => impl_cast!(as_large_polygon, 3),
                    MultiPoint(_, XYZ) => impl_cast!(as_multi_point, 3),
                    LargeMultiPoint(_, XYZ) => impl_cast!(as_large_multi_point, 3),
                    MultiLineString(_, XYZ) => impl_cast!(as_multi_line_string, 3),
                    LargeMultiLineString(_, XYZ) => {
                        impl_cast!(as_large_multi_line_string, 3)
                    }
                    MultiPolygon(_, XYZ) => impl_cast!(as_multi_polygon, 3),
                    LargeMultiPolygon(_, XYZ) => impl_cast!(as_large_multi_polygon, 3),
                    Mixed(_, XYZ) => impl_cast!(as_mixed, 3),
                    LargeMixed(_, XYZ) => impl_cast!(as_large_mixed, 3),
                    GeometryCollection(_, XYZ) => impl_cast!(as_geometry_collection, 3),
                    LargeGeometryCollection(_, XYZ) => {
                        impl_cast!(as_large_geometry_collection, 3)
                    }
                    WKB => impl_cast!(as_wkb),
                    LargeWKB => impl_cast!(as_large_wkb),
                    Rect(XY) => impl_cast!(as_rect, 2),
                    Rect(XYZ) => impl_cast!(as_rect, 3),
                };
                Ok(result)
            }
        }
    };
}

impl_chunked_cast_non_generic!(ChunkedPointArray<2>);
impl_chunked_cast_non_generic!(ChunkedRectArray<2>);
impl_chunked_cast_non_generic!(&dyn ChunkedNativeArray);
impl_chunked_cast_generic!(ChunkedLineStringArray<O, 2>);
impl_chunked_cast_generic!(ChunkedPolygonArray<O, 2>);
impl_chunked_cast_generic!(ChunkedMultiPointArray<O, 2>);
impl_chunked_cast_generic!(ChunkedMultiLineStringArray<O, 2>);
impl_chunked_cast_generic!(ChunkedMultiPolygonArray<O, 2>);
impl_chunked_cast_generic!(ChunkedMixedGeometryArray<O, 2>);
impl_chunked_cast_generic!(ChunkedGeometryCollectionArray<O, 2>);
impl_chunked_cast_non_generic!(ChunkedPointArray<3>);
impl_chunked_cast_non_generic!(ChunkedRectArray<3>);
impl_chunked_cast_generic!(ChunkedLineStringArray<O, 3>);
impl_chunked_cast_generic!(ChunkedPolygonArray<O, 3>);
impl_chunked_cast_generic!(ChunkedMultiPointArray<O, 3>);
impl_chunked_cast_generic!(ChunkedMultiLineStringArray<O, 3>);
impl_chunked_cast_generic!(ChunkedMultiPolygonArray<O, 3>);
impl_chunked_cast_generic!(ChunkedMixedGeometryArray<O, 3>);
impl_chunked_cast_generic!(ChunkedGeometryCollectionArray<O, 3>);
