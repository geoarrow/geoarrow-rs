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

    fn cast(&self, to_type: &GeoDataType) -> Self::Output;
}

impl Cast for PointArray<2> {
    type Output = Result<Arc<dyn GeometryArrayTrait>>;

    fn cast(&self, to_type: &GeoDataType) -> Self::Output {
        use Dimension::*;
        use GeoDataType::*;

        match to_type {
            Point(ct, XY) => {
                let mut builder = PointBuilder::<2>::with_capacity_and_options(
                    self.buffer_lengths(),
                    *ct,
                    self.metadata(),
                );
                self.iter().for_each(|x| builder.push_point(x.as_ref()));
                Ok(Arc::new(builder.finish()))
            }
            MultiPoint(ct, XY) => {
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
            LargeMultiPoint(ct, XY) => {
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
            Mixed(ct, XY) => {
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
            LargeMixed(ct, XY) => {
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
            GeometryCollection(ct, XY) => {
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
            LargeGeometryCollection(ct, XY) => {
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

            dt => Err(GeoArrowError::General(format!(
                "invalid cast to type {dt:?}"
            ))),
        }
    }
}

impl<O: OffsetSizeTrait> Cast for LineStringArray<O, 2> {
    type Output = Result<Arc<dyn GeometryArrayTrait>>;

    fn cast(&self, to_type: &GeoDataType) -> Self::Output {
        use Dimension::*;
        use GeoDataType::*;

        match to_type {
            LineString(ct, XY) => {
                let mut builder = LineStringBuilder::<i32, 2>::with_capacity_and_options(
                    self.buffer_lengths(),
                    *ct,
                    self.metadata(),
                );
                self.iter()
                    .try_for_each(|x| builder.push_line_string(x.as_ref()))?;
                Ok(Arc::new(builder.finish()))
            }
            LargeLineString(ct, XY) => {
                let mut builder = LineStringBuilder::<i64, 2>::with_capacity_and_options(
                    self.buffer_lengths(),
                    *ct,
                    self.metadata(),
                );
                self.iter()
                    .try_for_each(|x| builder.push_line_string(x.as_ref()))?;
                Ok(Arc::new(builder.finish()))
            }
            MultiLineString(ct, XY) => {
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
            LargeMultiLineString(ct, XY) => {
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
            Mixed(ct, XY) => {
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
            LargeMixed(ct, XY) => {
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
            GeometryCollection(ct, XY) => {
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
            LargeGeometryCollection(ct, XY) => {
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

            dt => Err(GeoArrowError::General(format!(
                "invalid cast to type {dt:?}"
            ))),
        }
    }
}

impl<O: OffsetSizeTrait> Cast for PolygonArray<O, 2> {
    type Output = Result<Arc<dyn GeometryArrayTrait>>;

    fn cast(&self, to_type: &GeoDataType) -> Self::Output {
        use Dimension::*;
        use GeoDataType::*;

        match to_type {
            Polygon(ct, XY) => {
                let mut builder = PolygonBuilder::<i32, 2>::with_capacity_and_options(
                    self.buffer_lengths(),
                    *ct,
                    self.metadata(),
                );
                self.iter()
                    .try_for_each(|x| builder.push_polygon(x.as_ref()))?;
                Ok(Arc::new(builder.finish()))
            }
            LargePolygon(ct, XY) => {
                let mut builder = PolygonBuilder::<i64, 2>::with_capacity_and_options(
                    self.buffer_lengths(),
                    *ct,
                    self.metadata(),
                );
                self.iter()
                    .try_for_each(|x| builder.push_polygon(x.as_ref()))?;
                Ok(Arc::new(builder.finish()))
            }
            MultiPolygon(ct, XY) => {
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
            LargeMultiPolygon(ct, XY) => {
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
            Mixed(ct, XY) => {
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
            LargeMixed(ct, XY) => {
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
            GeometryCollection(ct, XY) => {
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
            LargeGeometryCollection(ct, XY) => {
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

            dt => Err(GeoArrowError::General(format!(
                "invalid cast to type {dt:?}"
            ))),
        }
    }
}

impl<O: OffsetSizeTrait> Cast for MultiPointArray<O, 2> {
    type Output = Result<Arc<dyn GeometryArrayTrait>>;

    fn cast(&self, to_type: &GeoDataType) -> Self::Output {
        use Dimension::*;
        use GeoDataType::*;

        match to_type {
            Point(ct, XY) => {
                if self.geom_offsets.last().to_usize().unwrap() != self.len() {
                    return Err(GeoArrowError::General("Unable to cast".to_string()));
                }

                let mut builder =
                    PointBuilder::<2>::with_capacity_and_options(self.len(), *ct, self.metadata());
                self.iter()
                    .for_each(|x| builder.push_point(x.map(|mp| mp.point(0).unwrap()).as_ref()));
                Ok(Arc::new(builder.finish()))
            }
            MultiPoint(ct, XY) => {
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
            LargeMultiPoint(ct, XY) => {
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
            Mixed(ct, XY) => {
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
            LargeMixed(ct, XY) => {
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
            GeometryCollection(ct, XY) => {
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
            LargeGeometryCollection(ct, XY) => {
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

            dt => Err(GeoArrowError::General(format!(
                "invalid cast to type {dt:?}"
            ))),
        }
    }
}

impl<O: OffsetSizeTrait> Cast for MultiLineStringArray<O, 2> {
    type Output = Result<Arc<dyn GeometryArrayTrait>>;

    fn cast(&self, to_type: &GeoDataType) -> Self::Output {
        use Dimension::*;
        use GeoDataType::*;

        match to_type {
            LineString(ct, XY) => {
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
            LargeLineString(ct, XY) => {
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
            Mixed(ct, XY) => {
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
            LargeMixed(ct, XY) => {
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
            GeometryCollection(ct, XY) => {
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
            LargeGeometryCollection(ct, XY) => {
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

            dt => Err(GeoArrowError::General(format!(
                "invalid cast to type {dt:?}"
            ))),
        }
    }
}

impl<O: OffsetSizeTrait> Cast for MultiPolygonArray<O, 2> {
    type Output = Result<Arc<dyn GeometryArrayTrait>>;

    fn cast(&self, to_type: &GeoDataType) -> Self::Output {
        use Dimension::*;
        use GeoDataType::*;

        match to_type {
            Polygon(ct, XY) => {
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
            LargePolygon(ct, XY) => {
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
            Mixed(ct, XY) => {
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
            LargeMixed(ct, XY) => {
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
            GeometryCollection(ct, XY) => {
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
            LargeGeometryCollection(ct, XY) => {
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

            dt => Err(GeoArrowError::General(format!(
                "invalid cast to type {dt:?}"
            ))),
        }
    }
}

impl<O: OffsetSizeTrait> Cast for MixedGeometryArray<O, 2> {
    type Output = Result<Arc<dyn GeometryArrayTrait>>;

    /// TODO: in the future, do more validation before trying to fill all geometries
    fn cast(&self, to_type: &GeoDataType) -> Self::Output {
        use Dimension::*;
        use GeoDataType::*;

        match to_type {
            Point(ct, XY) => {
                if self.has_line_strings()
                    | self.has_polygons()
                    | self.has_multi_line_strings()
                    | self.has_multi_polygons()
                {
                    return Err(GeoArrowError::General("".to_string()));
                }

                let mut builder =
                    PointBuilder::<2>::with_capacity_and_options(self.len(), *ct, self.metadata());
                self.iter()
                    .try_for_each(|x| builder.push_geometry(x.as_ref()))?;
                Ok(Arc::new(builder.finish()))
            }
            LineString(ct, XY) => {
                if self.has_points()
                    | self.has_polygons()
                    | self.has_multi_points()
                    | self.has_multi_polygons()
                {
                    return Err(GeoArrowError::General("".to_string()));
                }

                let mut capacity = self.line_strings.buffer_lengths();
                if self
                    .multi_line_strings
                    .geom_offsets
                    .last()
                    .to_usize()
                    .unwrap()
                    != self.multi_line_strings.len()
                {
                    return Err(GeoArrowError::General("Unable to cast".to_string()));
                }
                let buffer_lengths = self.multi_line_strings.buffer_lengths();
                capacity.coord_capacity += buffer_lengths.coord_capacity;
                capacity.geom_capacity += buffer_lengths.ring_capacity;

                let mut builder = LineStringBuilder::<i32, 2>::with_capacity_and_options(
                    capacity,
                    *ct,
                    self.metadata(),
                );
                self.iter()
                    .try_for_each(|x| builder.push_geometry(x.as_ref()))?;
                Ok(Arc::new(builder.finish()))
            }
            LargeLineString(ct, XY) => {
                if self.has_points()
                    | self.has_polygons()
                    | self.has_multi_points()
                    | self.has_multi_polygons()
                {
                    return Err(GeoArrowError::General("".to_string()));
                }

                let mut capacity = self.line_strings.buffer_lengths();
                if self
                    .multi_line_strings
                    .geom_offsets
                    .last()
                    .to_usize()
                    .unwrap()
                    != self.multi_line_strings.len()
                {
                    return Err(GeoArrowError::General("Unable to cast".to_string()));
                }
                let buffer_lengths = self.multi_line_strings.buffer_lengths();
                capacity.coord_capacity += buffer_lengths.coord_capacity;
                capacity.geom_capacity += buffer_lengths.ring_capacity;

                let mut builder = LineStringBuilder::<i64, 2>::with_capacity_and_options(
                    capacity,
                    *ct,
                    self.metadata(),
                );
                self.iter()
                    .try_for_each(|x| builder.push_geometry(x.as_ref()))?;
                Ok(Arc::new(builder.finish()))
            }
            Polygon(ct, XY) => {
                if self.has_points()
                    | self.has_line_strings()
                    | self.has_multi_points()
                    | self.has_multi_line_strings()
                {
                    return Err(GeoArrowError::General("".to_string()));
                }

                let mut capacity = self.polygons.buffer_lengths();
                if self.multi_polygons.geom_offsets.last().to_usize().unwrap()
                    != self.multi_polygons.len()
                {
                    return Err(GeoArrowError::General("Unable to cast".to_string()));
                }
                let buffer_lengths = self.multi_polygons.buffer_lengths();
                capacity.coord_capacity += buffer_lengths.coord_capacity;
                capacity.ring_capacity += buffer_lengths.ring_capacity;
                capacity.geom_capacity += buffer_lengths.polygon_capacity;

                let mut builder = PolygonBuilder::<i32, 2>::with_capacity_and_options(
                    capacity,
                    *ct,
                    self.metadata(),
                );
                self.iter()
                    .try_for_each(|x| builder.push_geometry(x.as_ref()))?;
                Ok(Arc::new(builder.finish()))
            }
            LargePolygon(ct, XY) => {
                if self.has_points()
                    | self.has_line_strings()
                    | self.has_multi_points()
                    | self.has_multi_line_strings()
                {
                    return Err(GeoArrowError::General("".to_string()));
                }

                let mut capacity = self.polygons.buffer_lengths();
                if self.multi_polygons.geom_offsets.last().to_usize().unwrap()
                    != self.multi_polygons.len()
                {
                    return Err(GeoArrowError::General("Unable to cast".to_string()));
                }
                let buffer_lengths = self.multi_polygons.buffer_lengths();
                capacity.coord_capacity += buffer_lengths.coord_capacity;
                capacity.ring_capacity += buffer_lengths.ring_capacity;
                capacity.geom_capacity += buffer_lengths.polygon_capacity;

                let mut builder = PolygonBuilder::<i64, 2>::with_capacity_and_options(
                    capacity,
                    *ct,
                    self.metadata(),
                );
                self.iter()
                    .try_for_each(|x| builder.push_geometry(x.as_ref()))?;
                Ok(Arc::new(builder.finish()))
            }
            MultiPoint(ct, XY) => {
                if self.has_line_strings()
                    | self.has_polygons()
                    | self.has_multi_line_strings()
                    | self.has_multi_polygons()
                {
                    return Err(GeoArrowError::General("".to_string()));
                }

                let mut capacity = self.multi_points.buffer_lengths();
                // Hack: move to newtype
                capacity.coord_capacity += self.points.buffer_lengths();
                capacity.geom_capacity += self.points.buffer_lengths();

                let mut builder = MultiPointBuilder::<i32, 2>::with_capacity_and_options(
                    capacity,
                    *ct,
                    self.metadata(),
                );
                self.iter()
                    .try_for_each(|x| builder.push_geometry(x.as_ref()))?;
                Ok(Arc::new(builder.finish()))
            }
            LargeMultiPoint(ct, XY) => {
                if self.has_line_strings()
                    | self.has_polygons()
                    | self.has_multi_line_strings()
                    | self.has_multi_polygons()
                {
                    return Err(GeoArrowError::General("".to_string()));
                }

                let mut capacity = self.multi_points.buffer_lengths();
                // Hack: move to newtype
                capacity.coord_capacity += self.points.buffer_lengths();
                capacity.geom_capacity += self.points.buffer_lengths();

                let mut builder = MultiPointBuilder::<i64, 2>::with_capacity_and_options(
                    capacity,
                    *ct,
                    self.metadata(),
                );
                self.iter()
                    .try_for_each(|x| builder.push_geometry(x.as_ref()))?;
                Ok(Arc::new(builder.finish()))
            }
            MultiLineString(ct, XY) => {
                if self.has_points()
                    | self.has_polygons()
                    | self.has_multi_points()
                    | self.has_multi_polygons()
                {
                    return Err(GeoArrowError::General("".to_string()));
                }

                let mut capacity = self.multi_line_strings.buffer_lengths();
                capacity += self.line_strings.buffer_lengths();

                let mut builder = MultiLineStringBuilder::<i32, 2>::with_capacity_and_options(
                    capacity,
                    *ct,
                    self.metadata(),
                );
                self.iter()
                    .try_for_each(|x| builder.push_geometry(x.as_ref()))?;
                Ok(Arc::new(builder.finish()))
            }
            LargeMultiLineString(ct, XY) => {
                if self.has_points()
                    | self.has_polygons()
                    | self.has_multi_points()
                    | self.has_multi_polygons()
                {
                    return Err(GeoArrowError::General("".to_string()));
                }

                let mut capacity = self.multi_line_strings.buffer_lengths();
                capacity += self.line_strings.buffer_lengths();

                let mut builder = MultiLineStringBuilder::<i64, 2>::with_capacity_and_options(
                    capacity,
                    *ct,
                    self.metadata(),
                );
                self.iter()
                    .try_for_each(|x| builder.push_geometry(x.as_ref()))?;
                Ok(Arc::new(builder.finish()))
            }
            MultiPolygon(ct, XY) => {
                if self.has_points()
                    | self.has_line_strings()
                    | self.has_multi_points()
                    | self.has_multi_line_strings()
                {
                    return Err(GeoArrowError::General("".to_string()));
                }

                let mut capacity = self.multi_polygons.buffer_lengths();
                capacity += self.polygons.buffer_lengths();

                let mut builder = MultiPolygonBuilder::<i32, 2>::with_capacity_and_options(
                    capacity,
                    *ct,
                    self.metadata(),
                );
                self.iter()
                    .try_for_each(|x| builder.push_geometry(x.as_ref()))?;
                Ok(Arc::new(builder.finish()))
            }
            LargeMultiPolygon(ct, XY) => {
                if self.has_points()
                    | self.has_line_strings()
                    | self.has_multi_points()
                    | self.has_multi_line_strings()
                {
                    return Err(GeoArrowError::General("".to_string()));
                }

                let mut capacity = self.multi_polygons.buffer_lengths();
                capacity += self.polygons.buffer_lengths();

                let mut builder = MultiPolygonBuilder::<i64, 2>::with_capacity_and_options(
                    capacity,
                    *ct,
                    self.metadata(),
                );
                self.iter()
                    .try_for_each(|x| builder.push_geometry(x.as_ref()))?;
                Ok(Arc::new(builder.finish()))
            }
            Mixed(ct, XY) => {
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
            LargeMixed(ct, XY) => {
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
            GeometryCollection(ct, XY) => {
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
            LargeGeometryCollection(ct, XY) => {
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

            dt => Err(GeoArrowError::General(format!(
                "invalid cast to type {dt:?}"
            ))),
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

                let result: Arc<dyn ChunkedGeometryArrayTrait> = match to_type {
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
                    WKB => impl_cast!(as_wkb),
                    LargeWKB => impl_cast!(as_large_wkb),
                    Rect(XY) => impl_cast!(as_rect, 2),
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

                let result: Arc<dyn ChunkedGeometryArrayTrait> = match to_type {
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
                    WKB => impl_cast!(as_wkb),
                    LargeWKB => impl_cast!(as_large_wkb),
                    Rect(XY) => impl_cast!(as_rect, 2),
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
