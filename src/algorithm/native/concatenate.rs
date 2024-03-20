use arrow_array::OffsetSizeTrait;

use crate::array::*;
use crate::chunked_array::*;
use crate::error::Result;
use crate::trait_::GeometryArrayAccessor;

pub trait Concatenate: Sized {
    type Output;

    fn concatenate(&self) -> Self::Output;
}

impl Concatenate for &[PointArray] {
    type Output = Result<PointArray>;

    fn concatenate(&self) -> Self::Output {
        let output_capacity = self.iter().fold(0, |sum, val| sum + val.buffer_lengths());
        let mut builder = PointBuilder::with_capacity(output_capacity);
        self.iter()
            .for_each(|chunk| chunk.iter().for_each(|p| builder.push_point(p.as_ref())));
        Ok(builder.finish())
    }
}

macro_rules! impl_concatenate {
    ($array:ty, $capacity:ty, $builder:ty, $push_func:ident) => {
        impl<O: OffsetSizeTrait> Concatenate for &[$array] {
            type Output = Result<$array>;

            fn concatenate(&self) -> Self::Output {
                let output_capacity = self.iter().fold(<$capacity>::new_empty(), |sum, val| {
                    sum + val.buffer_lengths()
                });
                let mut builder = <$builder>::with_capacity(output_capacity);
                for chunk in self.iter() {
                    for geom in chunk.iter() {
                        builder.$push_func(geom.as_ref())?;
                    }
                }
                Ok(builder.finish())
            }
        }
    };
}

impl_concatenate!(
    LineStringArray<O>,
    LineStringCapacity,
    LineStringBuilder<O>,
    push_line_string
);
impl_concatenate!(
    PolygonArray<O>,
    PolygonCapacity,
    PolygonBuilder<O>,
    push_polygon
);
impl_concatenate!(
    MultiPointArray<O>,
    MultiPointCapacity,
    MultiPointBuilder<O>,
    push_multi_point
);
impl_concatenate!(
    MultiLineStringArray<O>,
    MultiLineStringCapacity,
    MultiLineStringBuilder<O>,
    push_multi_line_string
);
impl_concatenate!(
    MultiPolygonArray<O>,
    MultiPolygonCapacity,
    MultiPolygonBuilder<O>,
    push_multi_polygon
);
impl_concatenate!(
    MixedGeometryArray<O>,
    MixedCapacity,
    MixedGeometryBuilder<O>,
    push_geometry
);
impl_concatenate!(
    GeometryCollectionArray<O>,
    GeometryCollectionCapacity,
    GeometryCollectionBuilder<O>,
    push_geometry_collection
);

impl Concatenate for ChunkedPointArray {
    type Output = Result<PointArray>;

    fn concatenate(&self) -> Self::Output {
        self.chunks.as_slice().concatenate()
    }
}

macro_rules! impl_chunked_concatenate {
    ($chunked_array:ty, $output_array:ty) => {
        impl<O: OffsetSizeTrait> Concatenate for $chunked_array {
            type Output = Result<$output_array>;

            fn concatenate(&self) -> Self::Output {
                self.chunks.as_slice().concatenate()
            }
        }
    };
}

impl_chunked_concatenate!(ChunkedLineStringArray<O>, LineStringArray<O>);
impl_chunked_concatenate!(ChunkedPolygonArray<O>, PolygonArray<O>);
impl_chunked_concatenate!(ChunkedMultiPointArray<O>, MultiPointArray<O>);
impl_chunked_concatenate!(ChunkedMultiLineStringArray<O>, MultiLineStringArray<O>);
impl_chunked_concatenate!(ChunkedMultiPolygonArray<O>, MultiPolygonArray<O>);
impl_chunked_concatenate!(ChunkedMixedGeometryArray<O>, MixedGeometryArray<O>);
impl_chunked_concatenate!(
    ChunkedGeometryCollectionArray<O>,
    GeometryCollectionArray<O>
);
