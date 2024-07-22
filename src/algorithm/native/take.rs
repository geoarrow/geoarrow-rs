use std::ops::Range;
use std::sync::Arc;

use crate::array::*;
use crate::chunked_array::ChunkedGeometryArray;
use crate::datatypes::{Dimension, GeoDataType};
use crate::error::{GeoArrowError, Result};
use crate::trait_::GeometryArrayAccessor;
use crate::GeometryArrayTrait;
use arrow_array::{OffsetSizeTrait, UInt32Array};
use arrow_buffer::ArrowNativeType;

/// Take elements by index from Array, creating a new Array from those indexes.
pub trait Take {
    type Output;

    fn take(&self, indices: &UInt32Array) -> Self::Output;

    fn take_range(&self, range: &Range<usize>) -> Self::Output;
}

impl Take for PointArray<2> {
    type Output = Self;

    fn take(&self, indices: &UInt32Array) -> Self::Output {
        let mut builder = PointBuilder::with_capacity_and_options(
            indices.len(),
            self.coord_type(),
            self.metadata(),
        );
        for index in indices.iter() {
            if let Some(index) = index {
                builder.push_point(self.get(index.as_usize()).as_ref())
            } else {
                builder.push_null();
            }
        }

        builder.finish()
    }

    fn take_range(&self, range: &Range<usize>) -> Self::Output {
        let mut builder = PointBuilder::with_capacity_and_options(
            range.end - range.start,
            self.coord_type(),
            self.metadata(),
        );
        for i in range.start..range.end {
            builder.push_point(self.get(i).as_ref());
        }
        builder.finish()
    }
}

// TODO: parameterize over input and output separately

macro_rules! take_impl {
    ($array_type:ty, $capacity_type:ty, $builder_type:ty, $capacity_add_func:ident, $push_func:ident) => {
        impl<O: OffsetSizeTrait> Take for $array_type {
            type Output = Result<Self>;

            fn take(&self, indices: &UInt32Array) -> Self::Output {
                let mut capacity = <$capacity_type>::new_empty();

                for index in indices.iter().flatten() {
                    capacity.$capacity_add_func(self.get(index.as_usize()).as_ref());
                }

                let mut builder = <$builder_type>::with_capacity_and_options(
                    capacity,
                    self.coord_type(),
                    self.metadata(),
                );

                for index in indices.iter() {
                    if let Some(index) = index {
                        builder.$push_func(self.get(index.as_usize()).as_ref())?;
                    } else {
                        builder.push_null();
                    }
                }

                Ok(builder.finish())
            }

            fn take_range(&self, range: &Range<usize>) -> Self::Output {
                let mut capacity = <$capacity_type>::new_empty();

                for i in range.start..range.end {
                    capacity.$capacity_add_func(self.get(i).as_ref());
                }

                let mut builder = <$builder_type>::with_capacity_and_options(
                    capacity,
                    self.coord_type(),
                    self.metadata(),
                );

                for i in range.start..range.end {
                    builder.$push_func(self.get(i).as_ref())?;
                }

                Ok(builder.finish())
            }
        }
    };
}

take_impl!(
    LineStringArray<O, 2>,
    LineStringCapacity,
    LineStringBuilder<O, 2>,
    add_line_string,
    push_line_string
);
take_impl!(
    PolygonArray<O, 2>,
    PolygonCapacity,
    PolygonBuilder<O, 2>,
    add_polygon,
    push_polygon
);
take_impl!(
    MultiPointArray<O, 2>,
    MultiPointCapacity,
    MultiPointBuilder<O, 2>,
    add_multi_point,
    push_multi_point
);
take_impl!(
    MultiLineStringArray<O, 2>,
    MultiLineStringCapacity,
    MultiLineStringBuilder<O, 2>,
    add_multi_line_string,
    push_multi_line_string
);
take_impl!(
    MultiPolygonArray<O, 2>,
    MultiPolygonCapacity,
    MultiPolygonBuilder<O, 2>,
    add_multi_polygon,
    push_multi_polygon
);

macro_rules! take_impl_fallible {
    ($array_type:ty, $capacity_type:ty, $builder_type:ty, $capacity_add_func:ident, $push_func:ident) => {
        impl<O: OffsetSizeTrait> Take for $array_type {
            type Output = Result<Self>;

            fn take(&self, indices: &UInt32Array) -> Self::Output {
                let mut capacity = <$capacity_type>::new_empty();

                for index in indices.iter().flatten() {
                    capacity.$capacity_add_func(self.get(index.as_usize()).as_ref())?;
                }

                let mut builder = <$builder_type>::with_capacity_and_options(
                    capacity,
                    self.coord_type(),
                    self.metadata(),
                );

                for index in indices.iter() {
                    if let Some(index) = index {
                        builder.$push_func(self.get(index.as_usize()).as_ref())?;
                    } else {
                        builder.push_null();
                    }
                }

                Ok(builder.finish())
            }

            fn take_range(&self, range: &Range<usize>) -> Self::Output {
                let mut capacity = <$capacity_type>::new_empty();

                for i in range.start..range.end {
                    capacity.$capacity_add_func(self.get(i).as_ref())?;
                }

                let mut builder = <$builder_type>::with_capacity_and_options(
                    capacity,
                    self.coord_type(),
                    self.metadata(),
                );

                for i in range.start..range.end {
                    builder.$push_func(self.get(i).as_ref())?;
                }

                Ok(builder.finish())
            }
        }
    };
}

take_impl_fallible!(
    MixedGeometryArray<O, 2>,
    MixedCapacity,
    MixedGeometryBuilder<O, 2>,
    add_geometry,
    push_geometry
);
take_impl_fallible!(
    GeometryCollectionArray<O, 2>,
    GeometryCollectionCapacity,
    GeometryCollectionBuilder<O, 2>,
    add_geometry_collection,
    push_geometry_collection
);

impl Take for &dyn GeometryArrayTrait {
    type Output = Result<Arc<dyn GeometryArrayTrait>>;

    fn take(&self, indices: &UInt32Array) -> Self::Output {
        let result: Arc<dyn GeometryArrayTrait> = match self.data_type() {
            GeoDataType::Point(_, Dimension::XY) => Arc::new(self.as_point_2d().take(indices)),
            GeoDataType::LineString(_, Dimension::XY) => {
                Arc::new(self.as_line_string_2d().take(indices)?)
            }
            GeoDataType::LargeLineString(_, Dimension::XY) => {
                Arc::new(self.as_large_line_string_2d().take(indices)?)
            }
            GeoDataType::Polygon(_, Dimension::XY) => Arc::new(self.as_polygon_2d().take(indices)?),
            GeoDataType::LargePolygon(_, Dimension::XY) => {
                Arc::new(self.as_large_polygon_2d().take(indices)?)
            }
            GeoDataType::MultiPoint(_, Dimension::XY) => {
                Arc::new(self.as_multi_point_2d().take(indices)?)
            }
            GeoDataType::LargeMultiPoint(_, Dimension::XY) => {
                Arc::new(self.as_large_multi_point_2d().take(indices)?)
            }
            GeoDataType::MultiLineString(_, Dimension::XY) => {
                Arc::new(self.as_multi_line_string_2d().take(indices)?)
            }
            GeoDataType::LargeMultiLineString(_, Dimension::XY) => {
                Arc::new(self.as_large_multi_line_string_2d().take(indices)?)
            }
            GeoDataType::MultiPolygon(_, Dimension::XY) => {
                Arc::new(self.as_multi_polygon_2d().take(indices)?)
            }
            GeoDataType::LargeMultiPolygon(_, Dimension::XY) => {
                Arc::new(self.as_large_multi_polygon_2d().take(indices)?)
            }
            GeoDataType::Mixed(_, Dimension::XY) => Arc::new(self.as_mixed_2d().take(indices)?),
            GeoDataType::LargeMixed(_, Dimension::XY) => {
                Arc::new(self.as_large_mixed_2d().take(indices)?)
            }
            GeoDataType::GeometryCollection(_, Dimension::XY) => {
                Arc::new(self.as_geometry_collection_2d().take(indices)?)
            }
            GeoDataType::LargeGeometryCollection(_, Dimension::XY) => {
                Arc::new(self.as_large_geometry_collection_2d().take(indices)?)
            }
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }

    fn take_range(&self, range: &Range<usize>) -> Self::Output {
        let result: Arc<dyn GeometryArrayTrait> = match self.data_type() {
            GeoDataType::Point(_, Dimension::XY) => Arc::new(self.as_point_2d().take_range(range)),
            GeoDataType::LineString(_, Dimension::XY) => {
                Arc::new(self.as_line_string_2d().take_range(range)?)
            }
            GeoDataType::LargeLineString(_, Dimension::XY) => {
                Arc::new(self.as_large_line_string_2d().take_range(range)?)
            }
            GeoDataType::Polygon(_, Dimension::XY) => {
                Arc::new(self.as_polygon_2d().take_range(range)?)
            }
            GeoDataType::LargePolygon(_, Dimension::XY) => {
                Arc::new(self.as_large_polygon_2d().take_range(range)?)
            }
            GeoDataType::MultiPoint(_, Dimension::XY) => {
                Arc::new(self.as_multi_point_2d().take_range(range)?)
            }
            GeoDataType::LargeMultiPoint(_, Dimension::XY) => {
                Arc::new(self.as_large_multi_point_2d().take_range(range)?)
            }
            GeoDataType::MultiLineString(_, Dimension::XY) => {
                Arc::new(self.as_multi_line_string_2d().take_range(range)?)
            }
            GeoDataType::LargeMultiLineString(_, Dimension::XY) => {
                Arc::new(self.as_large_multi_line_string_2d().take_range(range)?)
            }
            GeoDataType::MultiPolygon(_, Dimension::XY) => {
                Arc::new(self.as_multi_polygon_2d().take_range(range)?)
            }
            GeoDataType::LargeMultiPolygon(_, Dimension::XY) => {
                Arc::new(self.as_large_multi_polygon_2d().take_range(range)?)
            }
            GeoDataType::Mixed(_, Dimension::XY) => Arc::new(self.as_mixed_2d().take_range(range)?),
            GeoDataType::LargeMixed(_, Dimension::XY) => {
                Arc::new(self.as_large_mixed_2d().take_range(range)?)
            }
            GeoDataType::GeometryCollection(_, Dimension::XY) => {
                Arc::new(self.as_geometry_collection_2d().take_range(range)?)
            }
            GeoDataType::LargeGeometryCollection(_, Dimension::XY) => {
                Arc::new(self.as_large_geometry_collection_2d().take_range(range)?)
            }
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}

impl Take for ChunkedGeometryArray<PointArray<2>> {
    type Output = Result<ChunkedGeometryArray<PointArray<2>>>;

    fn take(&self, indices: &UInt32Array) -> Self::Output {
        let mut output_chunks = Vec::with_capacity(self.chunks.len());
        for chunk in self.chunks.iter() {
            output_chunks.push(chunk.take(indices));
        }

        Ok(ChunkedGeometryArray::new(output_chunks))
    }

    fn take_range(&self, range: &Range<usize>) -> Self::Output {
        let mut output_chunks = Vec::with_capacity(self.chunks.len());
        for chunk in self.chunks.iter() {
            output_chunks.push(chunk.take_range(range));
        }

        Ok(ChunkedGeometryArray::new(output_chunks))
    }
}

/// Implementation that iterates over chunks
macro_rules! chunked_impl {
    ($type:ty) => {
        impl<O: OffsetSizeTrait> Take for $type {
            type Output = Result<$type>;

            fn take(&self, indices: &UInt32Array) -> Self::Output {
                let mut output_chunks = Vec::with_capacity(self.chunks.len());
                for chunk in self.chunks.iter() {
                    output_chunks.push(chunk.take(indices)?);
                }

                Ok(ChunkedGeometryArray::new(output_chunks))
            }

            fn take_range(&self, range: &Range<usize>) -> Self::Output {
                let mut output_chunks = Vec::with_capacity(self.chunks.len());
                for chunk in self.chunks.iter() {
                    output_chunks.push(chunk.take_range(range)?);
                }

                Ok(ChunkedGeometryArray::new(output_chunks))
            }
        }
    };
}

chunked_impl!(ChunkedGeometryArray<LineStringArray<O, 2>>);
chunked_impl!(ChunkedGeometryArray<PolygonArray<O, 2>>);
chunked_impl!(ChunkedGeometryArray<MultiPointArray<O, 2>>);
chunked_impl!(ChunkedGeometryArray<MultiLineStringArray<O, 2>>);
chunked_impl!(ChunkedGeometryArray<MultiPolygonArray<O, 2>>);
chunked_impl!(ChunkedGeometryArray<MixedGeometryArray<O, 2>>);
chunked_impl!(ChunkedGeometryArray<GeometryCollectionArray<O, 2>>);
