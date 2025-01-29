use std::ops::Range;
use std::sync::Arc;

use crate::array::mixed::builder::DEFAULT_PREFER_MULTI;
use crate::array::*;
use crate::chunked_array::ChunkedGeometryArray;
use crate::datatypes::{Dimension, NativeType};
use crate::error::{GeoArrowError, Result};
use crate::trait_::ArrayAccessor;
use crate::NativeArray;
use arrow_array::UInt32Array;
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
            self.dimension(),
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
            self.dimension(),
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

// Note that GeometryArray's builder parameters differ from other native array types which means
// it cannot use the macro to build a Take impl
impl Take for GeometryArray {
    type Output = Result<Self>;

    fn take(&self, indices: &UInt32Array) -> Self::Output {
        let mut capacity = GeometryCapacity::new_empty(DEFAULT_PREFER_MULTI);

        for index in indices.iter().flatten() {
            capacity.add_geometry(self.get(index.as_usize()).as_ref())?;
        }

        let mut builder = GeometryBuilder::with_capacity_and_options(
            capacity,
            self.coord_type(),
            self.metadata(),
            DEFAULT_PREFER_MULTI,
        );

        for index in indices.iter() {
            if let Some(index) = index {
                builder.push_geometry(self.get(index.as_usize()).as_ref())?;
            } else {
                builder.push_null();
            }
        }

        Ok(builder.finish())
    }

    fn take_range(&self, range: &Range<usize>) -> Self::Output {
        let mut capacity = GeometryCapacity::new_empty(DEFAULT_PREFER_MULTI);

        for i in range.start..range.end {
            capacity.add_geometry(self.get(i).as_ref())?;
        }

        let mut builder = GeometryBuilder::with_capacity_and_options(
            capacity,
            self.coord_type(),
            self.metadata(),
            DEFAULT_PREFER_MULTI,
        );

        for i in range.start..range.end {
            builder.push_geometry(self.get(i).as_ref())?;
        }

        Ok(builder.finish())
    }
}

// TODO: parameterize over input and output separately

macro_rules! take_impl {
    ($array_type:ty, $capacity_type:ty, $builder_type:ty, $capacity_add_func:ident, $push_func:ident) => {
        impl Take for $array_type {
            type Output = Result<Self>;

            fn take(&self, indices: &UInt32Array) -> Self::Output {
                let mut capacity = <$capacity_type>::new_empty();

                for index in indices.iter().flatten() {
                    capacity.$capacity_add_func(self.get(index.as_usize()).as_ref());
                }

                let mut builder = <$builder_type>::with_capacity_and_options(
                    self.dimension(),
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
                    self.dimension(),
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
    LineStringArray,
    LineStringCapacity,
    LineStringBuilder,
    add_line_string,
    push_line_string
);
take_impl!(
    PolygonArray,
    PolygonCapacity,
    PolygonBuilder,
    add_polygon,
    push_polygon
);
take_impl!(
    MultiPointArray,
    MultiPointCapacity,
    MultiPointBuilder,
    add_multi_point,
    push_multi_point
);
take_impl!(
    MultiLineStringArray,
    MultiLineStringCapacity,
    MultiLineStringBuilder,
    add_multi_line_string,
    push_multi_line_string
);
take_impl!(
    MultiPolygonArray,
    MultiPolygonCapacity,
    MultiPolygonBuilder,
    add_multi_polygon,
    push_multi_polygon
);

macro_rules! take_impl_fallible {
    ($array_type:ty, $capacity_type:ty, $builder_type:ty, $capacity_add_func:ident, $push_func:ident) => {
        impl Take for $array_type {
            type Output = Result<Self>;

            fn take(&self, indices: &UInt32Array) -> Self::Output {
                let mut capacity = <$capacity_type>::new_empty();

                for index in indices.iter().flatten() {
                    capacity.$capacity_add_func(self.get(index.as_usize()).as_ref())?;
                }

                let mut builder = <$builder_type>::with_capacity_and_options(
                    self.dimension(),
                    capacity,
                    self.coord_type(),
                    self.metadata(),
                    DEFAULT_PREFER_MULTI,
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
                    self.dimension(),
                    capacity,
                    self.coord_type(),
                    self.metadata(),
                    DEFAULT_PREFER_MULTI,
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
    MixedGeometryArray,
    MixedCapacity,
    MixedGeometryBuilder,
    add_geometry,
    push_geometry
);
take_impl_fallible!(
    GeometryCollectionArray,
    GeometryCollectionCapacity,
    GeometryCollectionBuilder,
    add_geometry_collection,
    push_geometry_collection
);

impl Take for &dyn NativeArray {
    type Output = Result<Arc<dyn NativeArray>>;

    fn take(&self, indices: &UInt32Array) -> Self::Output {
        use Dimension::*;
        use NativeType::*;

        let result: Arc<dyn NativeArray> = match self.data_type() {
            Point(_, XY) => Arc::new(self.as_point().take(indices)),
            LineString(_, XY) => Arc::new(self.as_line_string().take(indices)?),
            Polygon(_, XY) => Arc::new(self.as_polygon().take(indices)?),
            MultiPoint(_, XY) => Arc::new(self.as_multi_point().take(indices)?),
            MultiLineString(_, XY) => Arc::new(self.as_multi_line_string().take(indices)?),
            MultiPolygon(_, XY) => Arc::new(self.as_multi_polygon().take(indices)?),
            Geometry(_) => Arc::new(self.as_geometry().take(indices)?),
            GeometryCollection(_, XY) => Arc::new(self.as_geometry_collection().take(indices)?),
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }

    fn take_range(&self, range: &Range<usize>) -> Self::Output {
        use Dimension::*;
        use NativeType::*;

        let result: Arc<dyn NativeArray> = match self.data_type() {
            Point(_, XY) => Arc::new(self.as_point().take_range(range)),
            LineString(_, XY) => Arc::new(self.as_line_string().take_range(range)?),
            Polygon(_, XY) => Arc::new(self.as_polygon().take_range(range)?),
            MultiPoint(_, XY) => Arc::new(self.as_multi_point().take_range(range)?),
            MultiLineString(_, XY) => Arc::new(self.as_multi_line_string().take_range(range)?),
            MultiPolygon(_, XY) => Arc::new(self.as_multi_polygon().take_range(range)?),
            Geometry(_) => Arc::new(self.as_geometry().take_range(range)?),
            GeometryCollection(_, XY) => Arc::new(self.as_geometry_collection().take_range(range)?),
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
        impl Take for $type {
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

chunked_impl!(ChunkedGeometryArray<LineStringArray>);
chunked_impl!(ChunkedGeometryArray<PolygonArray>);
chunked_impl!(ChunkedGeometryArray<MultiPointArray>);
chunked_impl!(ChunkedGeometryArray<MultiLineStringArray>);
chunked_impl!(ChunkedGeometryArray<MultiPolygonArray>);
chunked_impl!(ChunkedGeometryArray<MixedGeometryArray>);
chunked_impl!(ChunkedGeometryArray<GeometryCollectionArray>);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn geometry_take_impl() -> Result<()> {
        let indices: UInt32Array = vec![0, 2].into();
        let point = geo::point!(x: 0., y: 0.);
        let ls: geo::LineString<f64> = vec![(1., 1.), (2., 2.)].into();

        let mut geo_array = GeometryBuilder::new();
        geo_array.push_geometry(Some(&point))?;
        geo_array.push_geometry(Some(&geo::point!(x: 1., y: 1.)))?;
        geo_array.push_geometry(Some(&ls))?;
        let geo_array = geo_array.finish();

        let take_array = geo_array.take(&indices)?;
        assert_eq!(
            2,
            take_array.len(),
            "take resulted in an unexpected number of items"
        );
        assert_eq!(take_array.value(0), point);
        assert_eq!(take_array.value(1), ls);

        Ok(())
    }

    #[test]
    fn geometry_take_range_impl() -> Result<()> {
        let point = geo::point!(x: 0., y: 0.);
        let ls: geo::LineString<f64> = vec![(1., 1.), (2., 2.)].into();

        let mut geo_array = GeometryBuilder::new();
        geo_array.push_geometry(Some(&geo::point!(x: 1., y: 1.)))?;
        geo_array.push_geometry(Some(&point))?;
        geo_array.push_geometry(Some(&ls))?;
        let geo_array = geo_array.finish();

        let range = 1..geo_array.len();
        let take_array = geo_array.take_range(&range)?;
        assert_eq!(
            2,
            take_array.len(),
            "take range resulted in an unexpected number of items"
        );
        assert_eq!(take_array.value(0), point);
        assert_eq!(take_array.value(1), ls);

        Ok(())
    }
}
