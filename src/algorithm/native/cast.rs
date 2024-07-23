//! Note: In the future, it may be possible to optimize some of these casts, e.g. from Point to
//! MultiPoint by only constructing a new offsets array, but you have to check that the coordinate
//! type is not casted!
//!
//! todo: have a set of "fast cast" functions, where you first try to fast cast and fall back to
//! slower copies if necessary. Can check that the coord type of the input and output is the same.

use std::sync::Arc;

use arrow_array::OffsetSizeTrait;

use crate::array::util::OffsetBufferUtils;
use crate::array::*;
use crate::chunked_array::*;
use crate::datatypes::{Dimension, GeoDataType};
use crate::error::{GeoArrowError, Result};
use crate::geo_traits::{MultiLineStringTrait, MultiPointTrait, MultiPolygonTrait};
use crate::trait_::GeometryArrayAccessor;
use crate::GeometryArrayTrait;

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

/// This cast only covers
#[allow(dead_code)]
pub fn can_cast_types(from_type: &GeoDataType, to_type: &GeoDataType) -> bool {
    if from_type == to_type {
        return true;
    }

    use GeoDataType::*;
    match (from_type, to_type) {
        (Point(_, Dimension::XY), Point(_, Dimension::XY) | MultiPoint(_, Dimension::XY)) => true,
        (
            LineString(_, Dimension::XY),
            LineString(_, Dimension::XY) | MultiLineString(_, Dimension::XY),
        ) => true,
        (Polygon(_, Dimension::XY), Polygon(_, Dimension::XY) | MultiPolygon(_, Dimension::XY)) => {
            true
        }
        (MultiPoint(_, Dimension::XY), MultiPoint(_, Dimension::XY)) => true,
        (MultiLineString(_, Dimension::XY), MultiLineString(_, Dimension::XY)) => true,
        (MultiPolygon(_, Dimension::XY), MultiPolygon(_, Dimension::XY)) => true,
        _ => todo!(),
    }
}

pub trait Cast {
    type Output;

    fn cast(&self, to_type: &GeoDataType) -> Self::Output;
}

impl Cast for PointArray<2> {
    type Output = Result<Arc<dyn GeometryArrayTrait>>;

    fn cast(&self, to_type: &GeoDataType) -> Self::Output {
        use GeoDataType::*;
        match to_type {
            Point(ct, Dimension::XY) => {
                let mut builder = PointBuilder::with_capacity_and_options(
                    self.buffer_lengths(),
                    *ct,
                    self.metadata(),
                );
                self.iter().for_each(|x| builder.push_point(x.as_ref()));
                Ok(Arc::new(builder.finish()))
            }
            MultiPoint(ct, Dimension::XY) => {
                let capacity =
                    MultiPointCapacity::new(self.buffer_lengths(), self.buffer_lengths());
                let mut builder = MultiPointBuilder::<i32, 2>::with_capacity_and_options(
                    capacity,
                    *ct,
                    self.metadata(),
                );
                self.iter()
                    .try_for_each(|x| builder.push_point(x.as_ref()))?;
                Ok(Arc::new(builder.finish()))
            }
            LargeMultiPoint(ct, Dimension::XY) => {
                let capacity =
                    MultiPointCapacity::new(self.buffer_lengths(), self.buffer_lengths());
                let mut builder = MultiPointBuilder::<i64, 2>::with_capacity_and_options(
                    capacity,
                    *ct,
                    self.metadata(),
                );
                self.iter()
                    .try_for_each(|x| builder.push_point(x.as_ref()))?;
                Ok(Arc::new(builder.finish()))
            }
            Mixed(ct, Dimension::XY) => {
                let capacity = MixedCapacity {
                    point: self.buffer_lengths(),
                    ..Default::default()
                };
                let mut builder = MixedGeometryBuilder::<i32, 2>::with_capacity_and_options(
                    capacity,
                    *ct,
                    self.metadata(),
                );
                self.iter().for_each(|x| builder.push_point(x.as_ref()));
                Ok(Arc::new(builder.finish()))
            }
            LargeMixed(ct, Dimension::XY) => {
                let capacity = MixedCapacity {
                    point: self.buffer_lengths(),
                    ..Default::default()
                };
                let mut builder = MixedGeometryBuilder::<i64, 2>::with_capacity_and_options(
                    capacity,
                    *ct,
                    self.metadata(),
                );
                self.iter().for_each(|x| builder.push_point(x.as_ref()));
                Ok(Arc::new(builder.finish()))
            }
            GeometryCollection(ct, Dimension::XY) => {
                let mixed_capacity = MixedCapacity {
                    point: self.buffer_lengths(),
                    ..Default::default()
                };
                let capacity =
                    GeometryCollectionCapacity::new(mixed_capacity, self.buffer_lengths());
                let mut builder = GeometryCollectionBuilder::<i32, 2>::with_capacity_and_options(
                    capacity,
                    *ct,
                    self.metadata(),
                );
                self.iter()
                    .try_for_each(|x| builder.push_point(x.as_ref(), false))?;
                Ok(Arc::new(builder.finish()))
            }
            LargeGeometryCollection(ct, Dimension::XY) => {
                let mixed_capacity = MixedCapacity {
                    point: self.buffer_lengths(),
                    ..Default::default()
                };
                let capacity =
                    GeometryCollectionCapacity::new(mixed_capacity, self.buffer_lengths());
                let mut builder = GeometryCollectionBuilder::<i64, 2>::with_capacity_and_options(
                    capacity,
                    *ct,
                    self.metadata(),
                );
                self.iter()
                    .try_for_each(|x| builder.push_point(x.as_ref(), false))?;
                Ok(Arc::new(builder.finish()))
            }

            _ => Err(GeoArrowError::General("invalid cast".to_string())),
        }
    }
}

impl<O: OffsetSizeTrait> Cast for LineStringArray<O, 2> {
    type Output = Result<Arc<dyn GeometryArrayTrait>>;

    fn cast(&self, to_type: &GeoDataType) -> Self::Output {
        use GeoDataType::*;
        match to_type {
            LineString(ct, Dimension::XY) => {
                let mut builder = LineStringBuilder::<i32, 2>::with_capacity_and_options(
                    self.buffer_lengths(),
                    *ct,
                    self.metadata(),
                );
                self.iter()
                    .try_for_each(|x| builder.push_line_string(x.as_ref()))?;
                Ok(Arc::new(builder.finish()))
            }
            LargeLineString(ct, Dimension::XY) => {
                let mut builder = LineStringBuilder::<i64, 2>::with_capacity_and_options(
                    self.buffer_lengths(),
                    *ct,
                    self.metadata(),
                );
                self.iter()
                    .try_for_each(|x| builder.push_line_string(x.as_ref()))?;
                Ok(Arc::new(builder.finish()))
            }
            MultiLineString(ct, Dimension::XY) => {
                let mut capacity = MultiLineStringCapacity::new_empty();
                capacity += self.buffer_lengths();
                let mut builder = MultiLineStringBuilder::<i32, 2>::with_capacity_and_options(
                    capacity,
                    *ct,
                    self.metadata(),
                );
                self.iter()
                    .try_for_each(|x| builder.push_line_string(x.as_ref()))?;
                Ok(Arc::new(builder.finish()))
            }
            LargeMultiLineString(ct, Dimension::XY) => {
                let mut capacity = MultiLineStringCapacity::new_empty();
                capacity += self.buffer_lengths();
                let mut builder = MultiLineStringBuilder::<i64, 2>::with_capacity_and_options(
                    capacity,
                    *ct,
                    self.metadata(),
                );
                self.iter()
                    .try_for_each(|x| builder.push_line_string(x.as_ref()))?;
                Ok(Arc::new(builder.finish()))
            }
            Mixed(ct, Dimension::XY) => {
                let capacity = MixedCapacity {
                    line_string: self.buffer_lengths(),
                    ..Default::default()
                };
                let mut builder = MixedGeometryBuilder::<i32, 2>::with_capacity_and_options(
                    capacity,
                    *ct,
                    self.metadata(),
                );
                self.iter()
                    .try_for_each(|x| builder.push_line_string(x.as_ref()))?;
                Ok(Arc::new(builder.finish()))
            }
            LargeMixed(ct, Dimension::XY) => {
                let capacity = MixedCapacity {
                    line_string: self.buffer_lengths(),
                    ..Default::default()
                };
                let mut builder = MixedGeometryBuilder::<i64, 2>::with_capacity_and_options(
                    capacity,
                    *ct,
                    self.metadata(),
                );
                self.iter()
                    .try_for_each(|x| builder.push_line_string(x.as_ref()))?;
                Ok(Arc::new(builder.finish()))
            }
            GeometryCollection(ct, Dimension::XY) => {
                let mixed_capacity = MixedCapacity {
                    line_string: self.buffer_lengths(),
                    ..Default::default()
                };
                let capacity = GeometryCollectionCapacity::new(mixed_capacity, self.len());
                let mut builder = GeometryCollectionBuilder::<i32, 2>::with_capacity_and_options(
                    capacity,
                    *ct,
                    self.metadata(),
                );
                self.iter()
                    .try_for_each(|x| builder.push_line_string(x.as_ref(), false))?;
                Ok(Arc::new(builder.finish()))
            }
            LargeGeometryCollection(ct, Dimension::XY) => {
                let mixed_capacity = MixedCapacity {
                    line_string: self.buffer_lengths(),
                    ..Default::default()
                };
                let capacity = GeometryCollectionCapacity::new(mixed_capacity, self.len());
                let mut builder = GeometryCollectionBuilder::<i64, 2>::with_capacity_and_options(
                    capacity,
                    *ct,
                    self.metadata(),
                );
                self.iter()
                    .try_for_each(|x| builder.push_line_string(x.as_ref(), false))?;
                Ok(Arc::new(builder.finish()))
            }

            _ => Err(GeoArrowError::General("invalid cast".to_string())),
        }
    }
}

impl<O: OffsetSizeTrait> Cast for PolygonArray<O, 2> {
    type Output = Result<Arc<dyn GeometryArrayTrait>>;

    fn cast(&self, to_type: &GeoDataType) -> Self::Output {
        use GeoDataType::*;
        match to_type {
            Polygon(ct, Dimension::XY) => {
                let mut builder = PolygonBuilder::<i32, 2>::with_capacity_and_options(
                    self.buffer_lengths(),
                    *ct,
                    self.metadata(),
                );
                self.iter()
                    .try_for_each(|x| builder.push_polygon(x.as_ref()))?;
                Ok(Arc::new(builder.finish()))
            }
            LargePolygon(ct, Dimension::XY) => {
                let mut builder = PolygonBuilder::<i64, 2>::with_capacity_and_options(
                    self.buffer_lengths(),
                    *ct,
                    self.metadata(),
                );
                self.iter()
                    .try_for_each(|x| builder.push_polygon(x.as_ref()))?;
                Ok(Arc::new(builder.finish()))
            }
            MultiPolygon(ct, Dimension::XY) => {
                let mut capacity = MultiPolygonCapacity::new_empty();
                capacity += self.buffer_lengths();
                let mut builder = MultiPolygonBuilder::<i32, 2>::with_capacity_and_options(
                    capacity,
                    *ct,
                    self.metadata(),
                );
                self.iter()
                    .try_for_each(|x| builder.push_polygon(x.as_ref()))?;
                Ok(Arc::new(builder.finish()))
            }
            LargeMultiPolygon(ct, Dimension::XY) => {
                let mut capacity = MultiPolygonCapacity::new_empty();
                capacity += self.buffer_lengths();
                let mut builder = MultiPolygonBuilder::<i64, 2>::with_capacity_and_options(
                    capacity,
                    *ct,
                    self.metadata(),
                );
                self.iter()
                    .try_for_each(|x| builder.push_polygon(x.as_ref()))?;
                Ok(Arc::new(builder.finish()))
            }
            Mixed(ct, Dimension::XY) => {
                let capacity = MixedCapacity {
                    polygon: self.buffer_lengths(),
                    ..Default::default()
                };
                let mut builder = MixedGeometryBuilder::<i32, 2>::with_capacity_and_options(
                    capacity,
                    *ct,
                    self.metadata(),
                );
                self.iter()
                    .try_for_each(|x| builder.push_polygon(x.as_ref()))?;
                Ok(Arc::new(builder.finish()))
            }
            LargeMixed(ct, Dimension::XY) => {
                let capacity = MixedCapacity {
                    polygon: self.buffer_lengths(),
                    ..Default::default()
                };
                let mut builder = MixedGeometryBuilder::<i64, 2>::with_capacity_and_options(
                    capacity,
                    *ct,
                    self.metadata(),
                );
                self.iter()
                    .try_for_each(|x| builder.push_polygon(x.as_ref()))?;
                Ok(Arc::new(builder.finish()))
            }
            GeometryCollection(ct, Dimension::XY) => {
                let mixed_capacity = MixedCapacity {
                    polygon: self.buffer_lengths(),
                    ..Default::default()
                };
                let capacity = GeometryCollectionCapacity::new(mixed_capacity, self.len());
                let mut builder = GeometryCollectionBuilder::<i32, 2>::with_capacity_and_options(
                    capacity,
                    *ct,
                    self.metadata(),
                );
                self.iter()
                    .try_for_each(|x| builder.push_polygon(x.as_ref(), false))?;
                Ok(Arc::new(builder.finish()))
            }
            LargeGeometryCollection(ct, Dimension::XY) => {
                let mixed_capacity = MixedCapacity {
                    polygon: self.buffer_lengths(),
                    ..Default::default()
                };
                let capacity = GeometryCollectionCapacity::new(mixed_capacity, self.len());
                let mut builder = GeometryCollectionBuilder::<i64, 2>::with_capacity_and_options(
                    capacity,
                    *ct,
                    self.metadata(),
                );
                self.iter()
                    .try_for_each(|x| builder.push_polygon(x.as_ref(), false))?;
                Ok(Arc::new(builder.finish()))
            }

            _ => Err(GeoArrowError::General("invalid cast".to_string())),
        }
    }
}

impl<O: OffsetSizeTrait> Cast for MultiPointArray<O, 2> {
    type Output = Result<Arc<dyn GeometryArrayTrait>>;

    fn cast(&self, to_type: &GeoDataType) -> Self::Output {
        use GeoDataType::*;
        match to_type {
            Point(ct, Dimension::XY) => {
                if self.geom_offsets.last().to_usize().unwrap() != self.len() {
                    return Err(GeoArrowError::General("Unable to cast".to_string()));
                }

                let mut builder =
                    PointBuilder::with_capacity_and_options(self.len(), *ct, self.metadata());
                self.iter()
                    .for_each(|x| builder.push_point(x.map(|mp| mp.point(0).unwrap()).as_ref()));
                Ok(Arc::new(builder.finish()))
            }
            MultiPoint(ct, Dimension::XY) => {
                let capacity = self.buffer_lengths();
                let mut builder = MultiPointBuilder::<i32, 2>::with_capacity_and_options(
                    capacity,
                    *ct,
                    self.metadata(),
                );
                self.iter()
                    .try_for_each(|x| builder.push_multi_point(x.as_ref()))?;
                Ok(Arc::new(builder.finish()))
            }
            LargeMultiPoint(ct, Dimension::XY) => {
                let capacity = self.buffer_lengths();
                let mut builder = MultiPointBuilder::<i64, 2>::with_capacity_and_options(
                    capacity,
                    *ct,
                    self.metadata(),
                );
                self.iter()
                    .try_for_each(|x| builder.push_multi_point(x.as_ref()))?;
                Ok(Arc::new(builder.finish()))
            }
            Mixed(ct, Dimension::XY) => {
                let capacity = MixedCapacity {
                    multi_point: self.buffer_lengths(),
                    ..Default::default()
                };
                let mut builder = MixedGeometryBuilder::<i32, 2>::with_capacity_and_options(
                    capacity,
                    *ct,
                    self.metadata(),
                );
                self.iter()
                    .try_for_each(|x| builder.push_multi_point(x.as_ref()))?;
                Ok(Arc::new(builder.finish()))
            }
            LargeMixed(ct, Dimension::XY) => {
                let capacity = MixedCapacity {
                    multi_point: self.buffer_lengths(),
                    ..Default::default()
                };
                let mut builder = MixedGeometryBuilder::<i64, 2>::with_capacity_and_options(
                    capacity,
                    *ct,
                    self.metadata(),
                );
                self.iter()
                    .try_for_each(|x| builder.push_multi_point(x.as_ref()))?;
                Ok(Arc::new(builder.finish()))
            }
            GeometryCollection(ct, Dimension::XY) => {
                let mixed_capacity = MixedCapacity {
                    multi_point: self.buffer_lengths(),
                    ..Default::default()
                };
                let capacity = GeometryCollectionCapacity::new(mixed_capacity, self.len());
                let mut builder = GeometryCollectionBuilder::<i32, 2>::with_capacity_and_options(
                    capacity,
                    *ct,
                    self.metadata(),
                );
                self.iter()
                    .try_for_each(|x| builder.push_multi_point(x.as_ref()))?;
                Ok(Arc::new(builder.finish()))
            }
            LargeGeometryCollection(ct, Dimension::XY) => {
                let mixed_capacity = MixedCapacity {
                    multi_point: self.buffer_lengths(),
                    ..Default::default()
                };
                let capacity = GeometryCollectionCapacity::new(mixed_capacity, self.len());
                let mut builder = GeometryCollectionBuilder::<i64, 2>::with_capacity_and_options(
                    capacity,
                    *ct,
                    self.metadata(),
                );
                self.iter()
                    .try_for_each(|x| builder.push_multi_point(x.as_ref()))?;
                Ok(Arc::new(builder.finish()))
            }

            _ => Err(GeoArrowError::General("invalid cast".to_string())),
        }
    }
}

impl<O: OffsetSizeTrait> Cast for MultiLineStringArray<O, 2> {
    type Output = Result<Arc<dyn GeometryArrayTrait>>;

    fn cast(&self, to_type: &GeoDataType) -> Self::Output {
        use GeoDataType::*;
        match to_type {
            LineString(ct, Dimension::XY) => {
                if self.geom_offsets.last().to_usize().unwrap() != self.len() {
                    return Err(GeoArrowError::General("Unable to cast".to_string()));
                }

                let existing_capacity = self.buffer_lengths();
                let capacity = LineStringCapacity {
                    coord_capacity: existing_capacity.coord_capacity,
                    geom_capacity: existing_capacity.ring_capacity,
                };
                let mut builder = LineStringBuilder::<i32, 2>::with_capacity_and_options(
                    capacity,
                    *ct,
                    self.metadata(),
                );
                self.iter().try_for_each(|x| {
                    builder.push_line_string(x.map(|mp| mp.line(0).unwrap()).as_ref())
                })?;
                Ok(Arc::new(builder.finish()))
            }
            LargeLineString(ct, Dimension::XY) => {
                if self.geom_offsets.last().to_usize().unwrap() != self.len() {
                    return Err(GeoArrowError::General("Unable to cast".to_string()));
                }

                let existing_capacity = self.buffer_lengths();
                let capacity = LineStringCapacity {
                    coord_capacity: existing_capacity.coord_capacity,
                    geom_capacity: existing_capacity.ring_capacity,
                };
                let mut builder = LineStringBuilder::<i64, 2>::with_capacity_and_options(
                    capacity,
                    *ct,
                    self.metadata(),
                );
                self.iter().try_for_each(|x| {
                    builder.push_line_string(x.map(|mp| mp.line(0).unwrap()).as_ref())
                })?;
                Ok(Arc::new(builder.finish()))
            }
            Mixed(ct, Dimension::XY) => {
                let capacity = MixedCapacity {
                    multi_line_string: self.buffer_lengths(),
                    ..Default::default()
                };
                let mut builder = MixedGeometryBuilder::<i32, 2>::with_capacity_and_options(
                    capacity,
                    *ct,
                    self.metadata(),
                );
                self.iter()
                    .try_for_each(|x| builder.push_multi_line_string(x.as_ref()))?;
                Ok(Arc::new(builder.finish()))
            }
            LargeMixed(ct, Dimension::XY) => {
                let capacity = MixedCapacity {
                    multi_line_string: self.buffer_lengths(),
                    ..Default::default()
                };
                let mut builder = MixedGeometryBuilder::<i64, 2>::with_capacity_and_options(
                    capacity,
                    *ct,
                    self.metadata(),
                );
                self.iter()
                    .try_for_each(|x| builder.push_multi_line_string(x.as_ref()))?;
                Ok(Arc::new(builder.finish()))
            }
            GeometryCollection(ct, Dimension::XY) => {
                let mixed_capacity = MixedCapacity {
                    multi_line_string: self.buffer_lengths(),
                    ..Default::default()
                };
                let capacity = GeometryCollectionCapacity::new(mixed_capacity, self.len());
                let mut builder = GeometryCollectionBuilder::<i32, 2>::with_capacity_and_options(
                    capacity,
                    *ct,
                    self.metadata(),
                );
                self.iter()
                    .try_for_each(|x| builder.push_multi_line_string(x.as_ref()))?;
                Ok(Arc::new(builder.finish()))
            }
            LargeGeometryCollection(ct, Dimension::XY) => {
                let mixed_capacity = MixedCapacity {
                    multi_line_string: self.buffer_lengths(),
                    ..Default::default()
                };
                let capacity = GeometryCollectionCapacity::new(mixed_capacity, self.len());
                let mut builder = GeometryCollectionBuilder::<i64, 2>::with_capacity_and_options(
                    capacity,
                    *ct,
                    self.metadata(),
                );
                self.iter()
                    .try_for_each(|x| builder.push_multi_line_string(x.as_ref()))?;
                Ok(Arc::new(builder.finish()))
            }

            _ => Err(GeoArrowError::General("invalid cast".to_string())),
        }
    }
}

impl<O: OffsetSizeTrait> Cast for MultiPolygonArray<O, 2> {
    type Output = Result<Arc<dyn GeometryArrayTrait>>;

    fn cast(&self, to_type: &GeoDataType) -> Self::Output {
        use GeoDataType::*;
        match to_type {
            Polygon(ct, Dimension::XY) => {
                if self.geom_offsets.last().to_usize().unwrap() != self.len() {
                    return Err(GeoArrowError::General("Unable to cast".to_string()));
                }

                let existing_capacity = self.buffer_lengths();
                let capacity = PolygonCapacity {
                    coord_capacity: existing_capacity.coord_capacity,
                    ring_capacity: existing_capacity.ring_capacity,
                    geom_capacity: existing_capacity.polygon_capacity,
                };
                let mut builder = PolygonBuilder::<i32, 2>::with_capacity_and_options(
                    capacity,
                    *ct,
                    self.metadata(),
                );
                self.iter().try_for_each(|x| {
                    builder.push_polygon(x.map(|mp| mp.polygon(0).unwrap()).as_ref())
                })?;
                Ok(Arc::new(builder.finish()))
            }
            LargePolygon(ct, Dimension::XY) => {
                if self.geom_offsets.last().to_usize().unwrap() != self.len() {
                    return Err(GeoArrowError::General("Unable to cast".to_string()));
                }

                let existing_capacity = self.buffer_lengths();
                let capacity = PolygonCapacity {
                    coord_capacity: existing_capacity.coord_capacity,
                    ring_capacity: existing_capacity.ring_capacity,
                    geom_capacity: existing_capacity.polygon_capacity,
                };
                let mut builder = PolygonBuilder::<i64, 2>::with_capacity_and_options(
                    capacity,
                    *ct,
                    self.metadata(),
                );
                self.iter().try_for_each(|x| {
                    builder.push_polygon(x.map(|mp| mp.polygon(0).unwrap()).as_ref())
                })?;
                Ok(Arc::new(builder.finish()))
            }
            Mixed(ct, Dimension::XY) => {
                let capacity = MixedCapacity {
                    multi_polygon: self.buffer_lengths(),
                    ..Default::default()
                };
                let mut builder = MixedGeometryBuilder::<i32, 2>::with_capacity_and_options(
                    capacity,
                    *ct,
                    self.metadata(),
                );
                self.iter()
                    .try_for_each(|x| builder.push_multi_polygon(x.as_ref()))?;
                Ok(Arc::new(builder.finish()))
            }
            LargeMixed(ct, Dimension::XY) => {
                let capacity = MixedCapacity {
                    multi_polygon: self.buffer_lengths(),
                    ..Default::default()
                };
                let mut builder = MixedGeometryBuilder::<i64, 2>::with_capacity_and_options(
                    capacity,
                    *ct,
                    self.metadata(),
                );
                self.iter()
                    .try_for_each(|x| builder.push_multi_polygon(x.as_ref()))?;
                Ok(Arc::new(builder.finish()))
            }
            GeometryCollection(ct, Dimension::XY) => {
                let mixed_capacity = MixedCapacity {
                    multi_polygon: self.buffer_lengths(),
                    ..Default::default()
                };
                let capacity = GeometryCollectionCapacity::new(mixed_capacity, self.len());
                let mut builder = GeometryCollectionBuilder::<i32, 2>::with_capacity_and_options(
                    capacity,
                    *ct,
                    self.metadata(),
                );
                self.iter()
                    .try_for_each(|x| builder.push_multi_polygon(x.as_ref()))?;
                Ok(Arc::new(builder.finish()))
            }
            LargeGeometryCollection(ct, Dimension::XY) => {
                let mixed_capacity = MixedCapacity {
                    multi_polygon: self.buffer_lengths(),
                    ..Default::default()
                };
                let capacity = GeometryCollectionCapacity::new(mixed_capacity, self.len());
                let mut builder = GeometryCollectionBuilder::<i64, 2>::with_capacity_and_options(
                    capacity,
                    *ct,
                    self.metadata(),
                );
                self.iter()
                    .try_for_each(|x| builder.push_multi_polygon(x.as_ref()))?;
                Ok(Arc::new(builder.finish()))
            }

            _ => Err(GeoArrowError::General("invalid cast".to_string())),
        }
    }
}

impl<O: OffsetSizeTrait> Cast for MixedGeometryArray<O, 2> {
    type Output = Result<Arc<dyn GeometryArrayTrait>>;

    /// TODO: in the future, do more validation before trying to fill all geometries
    fn cast(&self, to_type: &GeoDataType) -> Self::Output {
        use GeoDataType::*;
        match to_type {
            Point(ct, Dimension::XY) => {
                if self.has_line_string_2ds()
                    | self.has_polygon_2ds()
                    | self.has_multi_line_string_2ds()
                    | self.has_multi_polygon_2ds()
                {
                    return Err(GeoArrowError::General("".to_string()));
                }

                let mut builder =
                    PointBuilder::with_capacity_and_options(self.len(), *ct, self.metadata());
                self.iter()
                    .try_for_each(|x| builder.push_geometry(x.as_ref()))?;
                Ok(Arc::new(builder.finish()))
            }
            LineString(ct, Dimension::XY) => {
                if self.has_points()
                    | self.has_polygon_2ds()
                    | self.has_multi_point_2ds()
                    | self.has_multi_polygon_2ds()
                {
                    return Err(GeoArrowError::General("".to_string()));
                }

                let mut capacity = self
                    .line_strings
                    .as_ref()
                    .map(|x| x.buffer_lengths())
                    .unwrap_or_default();
                if let Some(multi_line_strings) = &self.multi_line_strings {
                    if multi_line_strings.geom_offsets.last().to_usize().unwrap()
                        != multi_line_strings.len()
                    {
                        return Err(GeoArrowError::General("Unable to cast".to_string()));
                    }
                    let buffer_lengths = multi_line_strings.buffer_lengths();
                    capacity.coord_capacity += buffer_lengths.coord_capacity;
                    capacity.geom_capacity += buffer_lengths.ring_capacity;
                }

                let mut builder = LineStringBuilder::<i32, 2>::with_capacity_and_options(
                    capacity,
                    *ct,
                    self.metadata(),
                );
                self.iter()
                    .try_for_each(|x| builder.push_geometry(x.as_ref()))?;
                Ok(Arc::new(builder.finish()))
            }
            LargeLineString(ct, Dimension::XY) => {
                if self.has_points()
                    | self.has_polygon_2ds()
                    | self.has_multi_point_2ds()
                    | self.has_multi_polygon_2ds()
                {
                    return Err(GeoArrowError::General("".to_string()));
                }

                let mut capacity = self
                    .line_strings
                    .as_ref()
                    .map(|x| x.buffer_lengths())
                    .unwrap_or_default();
                if let Some(multi_line_strings) = &self.multi_line_strings {
                    if multi_line_strings.geom_offsets.last().to_usize().unwrap()
                        != multi_line_strings.len()
                    {
                        return Err(GeoArrowError::General("Unable to cast".to_string()));
                    }
                    let buffer_lengths = multi_line_strings.buffer_lengths();
                    capacity.coord_capacity += buffer_lengths.coord_capacity;
                    capacity.geom_capacity += buffer_lengths.ring_capacity;
                }

                let mut builder = LineStringBuilder::<i64, 2>::with_capacity_and_options(
                    capacity,
                    *ct,
                    self.metadata(),
                );
                self.iter()
                    .try_for_each(|x| builder.push_geometry(x.as_ref()))?;
                Ok(Arc::new(builder.finish()))
            }
            Polygon(ct, Dimension::XY) => {
                if self.has_points()
                    | self.has_line_string_2ds()
                    | self.has_multi_point_2ds()
                    | self.has_multi_line_string_2ds()
                {
                    return Err(GeoArrowError::General("".to_string()));
                }

                let mut capacity = self
                    .polygons
                    .as_ref()
                    .map(|x| x.buffer_lengths())
                    .unwrap_or_default();
                if let Some(multi_polygons) = &self.multi_polygons {
                    if multi_polygons.geom_offsets.last().to_usize().unwrap()
                        != multi_polygons.len()
                    {
                        return Err(GeoArrowError::General("Unable to cast".to_string()));
                    }
                    let buffer_lengths = multi_polygons.buffer_lengths();
                    capacity.coord_capacity += buffer_lengths.coord_capacity;
                    capacity.ring_capacity += buffer_lengths.ring_capacity;
                    capacity.geom_capacity += buffer_lengths.polygon_capacity;
                }

                let mut builder = PolygonBuilder::<i32, 2>::with_capacity_and_options(
                    capacity,
                    *ct,
                    self.metadata(),
                );
                self.iter()
                    .try_for_each(|x| builder.push_geometry(x.as_ref()))?;
                Ok(Arc::new(builder.finish()))
            }
            LargePolygon(ct, Dimension::XY) => {
                if self.has_points()
                    | self.has_line_string_2ds()
                    | self.has_multi_point_2ds()
                    | self.has_multi_line_string_2ds()
                {
                    return Err(GeoArrowError::General("".to_string()));
                }

                let mut capacity = self
                    .polygons
                    .as_ref()
                    .map(|x| x.buffer_lengths())
                    .unwrap_or_default();
                if let Some(multi_polygons) = &self.multi_polygons {
                    if multi_polygons.geom_offsets.last().to_usize().unwrap()
                        != multi_polygons.len()
                    {
                        return Err(GeoArrowError::General("Unable to cast".to_string()));
                    }
                    let buffer_lengths = multi_polygons.buffer_lengths();
                    capacity.coord_capacity += buffer_lengths.coord_capacity;
                    capacity.ring_capacity += buffer_lengths.ring_capacity;
                    capacity.geom_capacity += buffer_lengths.polygon_capacity;
                }

                let mut builder = PolygonBuilder::<i64, 2>::with_capacity_and_options(
                    capacity,
                    *ct,
                    self.metadata(),
                );
                self.iter()
                    .try_for_each(|x| builder.push_geometry(x.as_ref()))?;
                Ok(Arc::new(builder.finish()))
            }
            MultiPoint(ct, Dimension::XY) => {
                if self.has_line_string_2ds()
                    | self.has_polygon_2ds()
                    | self.has_multi_line_string_2ds()
                    | self.has_multi_polygon_2ds()
                {
                    return Err(GeoArrowError::General("".to_string()));
                }

                let mut capacity = self
                    .multi_points
                    .as_ref()
                    .map(|x| x.buffer_lengths())
                    .unwrap_or_default();
                if let Some(points) = &self.points {
                    // Hack: move to newtype
                    capacity.coord_capacity += points.buffer_lengths();
                    capacity.geom_capacity += points.buffer_lengths();
                }

                let mut builder = MultiPointBuilder::<i32, 2>::with_capacity_and_options(
                    capacity,
                    *ct,
                    self.metadata(),
                );
                self.iter()
                    .try_for_each(|x| builder.push_geometry(x.as_ref()))?;
                Ok(Arc::new(builder.finish()))
            }
            LargeMultiPoint(ct, Dimension::XY) => {
                if self.has_line_string_2ds()
                    | self.has_polygon_2ds()
                    | self.has_multi_line_string_2ds()
                    | self.has_multi_polygon_2ds()
                {
                    return Err(GeoArrowError::General("".to_string()));
                }

                let mut capacity = self
                    .multi_points
                    .as_ref()
                    .map(|x| x.buffer_lengths())
                    .unwrap_or_default();
                if let Some(points) = &self.points {
                    // Hack: move to newtype
                    capacity.coord_capacity += points.buffer_lengths();
                    capacity.geom_capacity += points.buffer_lengths();
                }

                let mut builder = MultiPointBuilder::<i64, 2>::with_capacity_and_options(
                    capacity,
                    *ct,
                    self.metadata(),
                );
                self.iter()
                    .try_for_each(|x| builder.push_geometry(x.as_ref()))?;
                Ok(Arc::new(builder.finish()))
            }
            MultiLineString(ct, Dimension::XY) => {
                if self.has_points()
                    | self.has_polygon_2ds()
                    | self.has_multi_point_2ds()
                    | self.has_multi_polygon_2ds()
                {
                    return Err(GeoArrowError::General("".to_string()));
                }

                let mut capacity = self
                    .multi_line_strings
                    .as_ref()
                    .map(|x| x.buffer_lengths())
                    .unwrap_or_default();
                if let Some(line_strings) = &self.line_strings {
                    capacity += line_strings.buffer_lengths();
                }

                let mut builder = MultiLineStringBuilder::<i32, 2>::with_capacity_and_options(
                    capacity,
                    *ct,
                    self.metadata(),
                );
                self.iter()
                    .try_for_each(|x| builder.push_geometry(x.as_ref()))?;
                Ok(Arc::new(builder.finish()))
            }
            LargeMultiLineString(ct, Dimension::XY) => {
                if self.has_points()
                    | self.has_polygon_2ds()
                    | self.has_multi_point_2ds()
                    | self.has_multi_polygon_2ds()
                {
                    return Err(GeoArrowError::General("".to_string()));
                }

                let mut capacity = self
                    .multi_line_strings
                    .as_ref()
                    .map(|x| x.buffer_lengths())
                    .unwrap_or_default();
                if let Some(line_strings) = &self.line_strings {
                    capacity += line_strings.buffer_lengths();
                }

                let mut builder = MultiLineStringBuilder::<i64, 2>::with_capacity_and_options(
                    capacity,
                    *ct,
                    self.metadata(),
                );
                self.iter()
                    .try_for_each(|x| builder.push_geometry(x.as_ref()))?;
                Ok(Arc::new(builder.finish()))
            }
            MultiPolygon(ct, Dimension::XY) => {
                if self.has_points()
                    | self.has_line_string_2ds()
                    | self.has_multi_point_2ds()
                    | self.has_multi_line_string_2ds()
                {
                    return Err(GeoArrowError::General("".to_string()));
                }

                let mut capacity = self
                    .multi_polygons
                    .as_ref()
                    .map(|x| x.buffer_lengths())
                    .unwrap_or_default();
                if let Some(polygons) = &self.polygons {
                    capacity += polygons.buffer_lengths();
                }

                let mut builder = MultiPolygonBuilder::<i32, 2>::with_capacity_and_options(
                    capacity,
                    *ct,
                    self.metadata(),
                );
                self.iter()
                    .try_for_each(|x| builder.push_geometry(x.as_ref()))?;
                Ok(Arc::new(builder.finish()))
            }
            LargeMultiPolygon(ct, Dimension::XY) => {
                if self.has_points()
                    | self.has_line_string_2ds()
                    | self.has_multi_point_2ds()
                    | self.has_multi_line_string_2ds()
                {
                    return Err(GeoArrowError::General("".to_string()));
                }

                let mut capacity = self
                    .multi_polygons
                    .as_ref()
                    .map(|x| x.buffer_lengths())
                    .unwrap_or_default();
                if let Some(polygons) = &self.polygons {
                    capacity += polygons.buffer_lengths();
                }

                let mut builder = MultiPolygonBuilder::<i64, 2>::with_capacity_and_options(
                    capacity,
                    *ct,
                    self.metadata(),
                );
                self.iter()
                    .try_for_each(|x| builder.push_geometry(x.as_ref()))?;
                Ok(Arc::new(builder.finish()))
            }
            Mixed(ct, Dimension::XY) => {
                let capacity = self.buffer_lengths();
                let mut builder = MixedGeometryBuilder::<i32, 2>::with_capacity_and_options(
                    capacity,
                    *ct,
                    self.metadata(),
                );
                self.iter()
                    .try_for_each(|x| builder.push_geometry(x.as_ref()))?;
                Ok(Arc::new(builder.finish()))
            }
            LargeMixed(ct, Dimension::XY) => {
                let capacity = self.buffer_lengths();
                let mut builder = MixedGeometryBuilder::<i64, 2>::with_capacity_and_options(
                    capacity,
                    *ct,
                    self.metadata(),
                );
                self.iter()
                    .try_for_each(|x| builder.push_geometry(x.as_ref()))?;
                Ok(Arc::new(builder.finish()))
            }
            GeometryCollection(ct, Dimension::XY) => {
                let capacity = GeometryCollectionCapacity::new(self.buffer_lengths(), self.len());
                let mut builder = GeometryCollectionBuilder::<i32, 2>::with_capacity_and_options(
                    capacity,
                    *ct,
                    self.metadata(),
                );
                self.iter()
                    .try_for_each(|x| builder.push_geometry(x.as_ref(), false))?;
                Ok(Arc::new(builder.finish()))
            }
            LargeGeometryCollection(ct, Dimension::XY) => {
                let capacity = GeometryCollectionCapacity::new(self.buffer_lengths(), self.len());
                let mut builder = GeometryCollectionBuilder::<i64, 2>::with_capacity_and_options(
                    capacity,
                    *ct,
                    self.metadata(),
                );
                self.iter()
                    .try_for_each(|x| builder.push_geometry(x.as_ref(), false))?;
                Ok(Arc::new(builder.finish()))
            }

            _ => Err(GeoArrowError::General("invalid cast".to_string())),
        }
    }
}

impl Cast for &dyn GeometryArrayTrait {
    type Output = Result<Arc<dyn GeometryArrayTrait>>;

    fn cast(&self, to_type: &GeoDataType) -> Self::Output {
        // TODO: not working :/
        // if self.data_type() == to_type {
        //     return Ok(Arc::new(self.to_owned()));
        // }

        use GeoDataType::*;
        match self.data_type() {
            Point(_, Dimension::XY) => self.as_ref().as_point_2d().cast(to_type),
            LineString(_, Dimension::XY) => self.as_ref().as_line_string_2d().cast(to_type),
            LargeLineString(_, Dimension::XY) => {
                self.as_ref().as_large_line_string_2d().cast(to_type)
            }
            Polygon(_, Dimension::XY) => self.as_ref().as_polygon_2d().cast(to_type),
            LargePolygon(_, Dimension::XY) => self.as_ref().as_large_polygon_2d().cast(to_type),
            MultiPoint(_, Dimension::XY) => self.as_ref().as_multi_point_2d().cast(to_type),
            LargeMultiPoint(_, Dimension::XY) => {
                self.as_ref().as_large_multi_point_2d().cast(to_type)
            }
            MultiLineString(_, Dimension::XY) => {
                self.as_ref().as_multi_line_string_2d().cast(to_type)
            }
            LargeMultiLineString(_, Dimension::XY) => {
                self.as_ref().as_large_multi_line_string_2d().cast(to_type)
            }
            MultiPolygon(_, Dimension::XY) => self.as_ref().as_multi_polygon_2d().cast(to_type),
            LargeMultiPolygon(_, Dimension::XY) => {
                self.as_ref().as_large_multi_polygon_2d().cast(to_type)
            }
            Mixed(_, Dimension::XY) => self.as_ref().as_mixed_2d().cast(to_type),
            LargeMixed(_, Dimension::XY) => self.as_ref().as_large_mixed_2d().cast(to_type),
            _ => todo!(),
        }
    }
}

macro_rules! impl_chunked_cast_non_generic {
    ($chunked_array:ty) => {
        impl Cast for $chunked_array {
            type Output = Result<Arc<dyn ChunkedGeometryArrayTrait>>;

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
                }

                use GeoDataType::*;
                let result: Arc<dyn ChunkedGeometryArrayTrait> = match to_type {
                    Point(_, Dimension::XY) => impl_cast!(as_point_2d),
                    LineString(_, Dimension::XY) => impl_cast!(as_line_string_2d),
                    LargeLineString(_, Dimension::XY) => impl_cast!(as_large_line_string_2d),
                    Polygon(_, Dimension::XY) => impl_cast!(as_polygon_2d),
                    LargePolygon(_, Dimension::XY) => impl_cast!(as_large_polygon_2d),
                    MultiPoint(_, Dimension::XY) => impl_cast!(as_multi_point_2d),
                    LargeMultiPoint(_, Dimension::XY) => impl_cast!(as_large_multi_point_2d),
                    MultiLineString(_, Dimension::XY) => impl_cast!(as_multi_line_string_2d),
                    LargeMultiLineString(_, Dimension::XY) => {
                        impl_cast!(as_large_multi_line_string_2d)
                    }
                    MultiPolygon(_, Dimension::XY) => impl_cast!(as_multi_polygon_2d),
                    LargeMultiPolygon(_, Dimension::XY) => impl_cast!(as_large_multi_polygon_2d),
                    Mixed(_, Dimension::XY) => impl_cast!(as_mixed_2d),
                    LargeMixed(_, Dimension::XY) => impl_cast!(as_large_mixed_2d),
                    GeometryCollection(_, Dimension::XY) => impl_cast!(as_geometry_collection_2d),
                    LargeGeometryCollection(_, Dimension::XY) => {
                        impl_cast!(as_large_geometry_collection_2d)
                    }
                    WKB => impl_cast!(as_wkb),
                    LargeWKB => impl_cast!(as_large_wkb),
                    Rect(Dimension::XY) => impl_cast!(as_rect_2d),
                    _ => todo!("3d support"),
                };
                Ok(result)
            }
        }
    };
}

macro_rules! impl_chunked_cast_generic {
    ($chunked_array:ty) => {
        impl<O: OffsetSizeTrait> Cast for $chunked_array {
            type Output = Result<Arc<dyn ChunkedGeometryArrayTrait>>;

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
                }

                use GeoDataType::*;
                let result: Arc<dyn ChunkedGeometryArrayTrait> = match to_type {
                    Point(_, Dimension::XY) => impl_cast!(as_point_2d),
                    LineString(_, Dimension::XY) => impl_cast!(as_line_string_2d),
                    LargeLineString(_, Dimension::XY) => impl_cast!(as_large_line_string_2d),
                    Polygon(_, Dimension::XY) => impl_cast!(as_polygon_2d),
                    LargePolygon(_, Dimension::XY) => impl_cast!(as_large_polygon_2d),
                    MultiPoint(_, Dimension::XY) => impl_cast!(as_multi_point_2d),
                    LargeMultiPoint(_, Dimension::XY) => impl_cast!(as_large_multi_point_2d),
                    MultiLineString(_, Dimension::XY) => impl_cast!(as_multi_line_string_2d),
                    LargeMultiLineString(_, Dimension::XY) => {
                        impl_cast!(as_large_multi_line_string_2d)
                    }
                    MultiPolygon(_, Dimension::XY) => impl_cast!(as_multi_polygon_2d),
                    LargeMultiPolygon(_, Dimension::XY) => impl_cast!(as_large_multi_polygon_2d),
                    Mixed(_, Dimension::XY) => impl_cast!(as_mixed_2d),
                    LargeMixed(_, Dimension::XY) => impl_cast!(as_large_mixed_2d),
                    GeometryCollection(_, Dimension::XY) => impl_cast!(as_geometry_collection_2d),
                    LargeGeometryCollection(_, Dimension::XY) => {
                        impl_cast!(as_large_geometry_collection_2d)
                    }
                    WKB => impl_cast!(as_wkb),
                    LargeWKB => impl_cast!(as_large_wkb),
                    Rect(Dimension::XY) => impl_cast!(as_rect_2d),
                    _ => todo!("3d support"),
                };
                Ok(result)
            }
        }
    };
}

impl_chunked_cast_non_generic!(ChunkedPointArray<2>);
impl_chunked_cast_non_generic!(ChunkedRectArray<2>);
impl_chunked_cast_non_generic!(&dyn ChunkedGeometryArrayTrait);
impl_chunked_cast_generic!(ChunkedLineStringArray<O, 2>);
impl_chunked_cast_generic!(ChunkedPolygonArray<O, 2>);
impl_chunked_cast_generic!(ChunkedMultiPointArray<O, 2>);
impl_chunked_cast_generic!(ChunkedMultiLineStringArray<O, 2>);
impl_chunked_cast_generic!(ChunkedMultiPolygonArray<O, 2>);
impl_chunked_cast_generic!(ChunkedMixedGeometryArray<O, 2>);
impl_chunked_cast_generic!(ChunkedGeometryCollectionArray<O, 2>);
