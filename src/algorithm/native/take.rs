use std::ops::Range;
use std::sync::Arc;

use crate::array::*;
use crate::chunked_array::ChunkedGeometryArray;
use crate::datatypes::GeoDataType;
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

impl Take for PointArray {
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
    LineStringArray<O>,
    LineStringCapacity,
    LineStringBuilder<O>,
    add_line_string,
    push_line_string
);
take_impl!(
    PolygonArray<O>,
    PolygonCapacity,
    PolygonBuilder<O>,
    add_polygon,
    push_polygon
);
take_impl!(
    MultiPointArray<O>,
    MultiPointCapacity,
    MultiPointBuilder<O>,
    add_multi_point,
    push_multi_point
);
take_impl!(
    MultiLineStringArray<O>,
    MultiLineStringCapacity,
    MultiLineStringBuilder<O>,
    add_multi_line_string,
    push_multi_line_string
);
take_impl!(
    MultiPolygonArray<O>,
    MultiPolygonCapacity,
    MultiPolygonBuilder<O>,
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
    MixedGeometryArray<O>,
    MixedCapacity,
    MixedGeometryBuilder<O>,
    add_geometry,
    push_geometry
);
take_impl_fallible!(
    GeometryCollectionArray<O>,
    GeometryCollectionCapacity,
    GeometryCollectionBuilder<O>,
    add_geometry_collection,
    push_geometry_collection
);

impl Take for &dyn GeometryArrayTrait {
    type Output = Result<Arc<dyn GeometryArrayTrait>>;

    fn take(&self, indices: &UInt32Array) -> Self::Output {
        let result: Arc<dyn GeometryArrayTrait> = match self.data_type() {
            GeoDataType::Point(_) => Arc::new(self.as_point().take(indices)),
            GeoDataType::LineString(_) => Arc::new(self.as_line_string().take(indices)?),
            GeoDataType::LargeLineString(_) => Arc::new(self.as_large_line_string().take(indices)?),
            GeoDataType::Polygon(_) => Arc::new(self.as_polygon().take(indices)?),
            GeoDataType::LargePolygon(_) => Arc::new(self.as_large_polygon().take(indices)?),
            GeoDataType::MultiPoint(_) => Arc::new(self.as_multi_point().take(indices)?),
            GeoDataType::LargeMultiPoint(_) => Arc::new(self.as_large_multi_point().take(indices)?),
            GeoDataType::MultiLineString(_) => Arc::new(self.as_multi_line_string().take(indices)?),
            GeoDataType::LargeMultiLineString(_) => {
                Arc::new(self.as_large_multi_line_string().take(indices)?)
            }
            GeoDataType::MultiPolygon(_) => Arc::new(self.as_multi_polygon().take(indices)?),
            GeoDataType::LargeMultiPolygon(_) => {
                Arc::new(self.as_large_multi_polygon().take(indices)?)
            }
            GeoDataType::Mixed(_) => Arc::new(self.as_mixed().take(indices)?),
            GeoDataType::LargeMixed(_) => Arc::new(self.as_large_mixed().take(indices)?),
            GeoDataType::GeometryCollection(_) => {
                Arc::new(self.as_geometry_collection().take(indices)?)
            }
            GeoDataType::LargeGeometryCollection(_) => {
                Arc::new(self.as_large_geometry_collection().take(indices)?)
            }
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }

    fn take_range(&self, range: &Range<usize>) -> Self::Output {
        let result: Arc<dyn GeometryArrayTrait> = match self.data_type() {
            GeoDataType::Point(_) => Arc::new(self.as_point().take_range(range)),
            GeoDataType::LineString(_) => Arc::new(self.as_line_string().take_range(range)?),
            GeoDataType::LargeLineString(_) => {
                Arc::new(self.as_large_line_string().take_range(range)?)
            }
            GeoDataType::Polygon(_) => Arc::new(self.as_polygon().take_range(range)?),
            GeoDataType::LargePolygon(_) => Arc::new(self.as_large_polygon().take_range(range)?),
            GeoDataType::MultiPoint(_) => Arc::new(self.as_multi_point().take_range(range)?),
            GeoDataType::LargeMultiPoint(_) => {
                Arc::new(self.as_large_multi_point().take_range(range)?)
            }
            GeoDataType::MultiLineString(_) => {
                Arc::new(self.as_multi_line_string().take_range(range)?)
            }
            GeoDataType::LargeMultiLineString(_) => {
                Arc::new(self.as_large_multi_line_string().take_range(range)?)
            }
            GeoDataType::MultiPolygon(_) => Arc::new(self.as_multi_polygon().take_range(range)?),
            GeoDataType::LargeMultiPolygon(_) => {
                Arc::new(self.as_large_multi_polygon().take_range(range)?)
            }
            GeoDataType::Mixed(_) => Arc::new(self.as_mixed().take_range(range)?),
            GeoDataType::LargeMixed(_) => Arc::new(self.as_large_mixed().take_range(range)?),
            GeoDataType::GeometryCollection(_) => {
                Arc::new(self.as_geometry_collection().take_range(range)?)
            }
            GeoDataType::LargeGeometryCollection(_) => {
                Arc::new(self.as_large_geometry_collection().take_range(range)?)
            }
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}

impl Take for ChunkedGeometryArray<PointArray> {
    type Output = Result<ChunkedGeometryArray<PointArray>>;

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

chunked_impl!(ChunkedGeometryArray<LineStringArray<O>>);
chunked_impl!(ChunkedGeometryArray<PolygonArray<O>>);
chunked_impl!(ChunkedGeometryArray<MultiPointArray<O>>);
chunked_impl!(ChunkedGeometryArray<MultiLineStringArray<O>>);
chunked_impl!(ChunkedGeometryArray<MultiPolygonArray<O>>);
chunked_impl!(ChunkedGeometryArray<MixedGeometryArray<O>>);
chunked_impl!(ChunkedGeometryArray<GeometryCollectionArray<O>>);
