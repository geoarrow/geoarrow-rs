use arrow_array::OffsetSizeTrait;

use crate::array::*;
use crate::chunked_array::*;
use crate::error::Result;
use crate::trait_::GeometryArrayAccessor;

pub trait Concatenate: Sized {
    type Output;

    fn concatenate(&self) -> Self::Output;
}

impl Concatenate for &[PointArray<2>] {
    type Output = Result<PointArray<2>>;

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
    LineStringArray<O, 2>,
    LineStringCapacity,
    LineStringBuilder<O, 2>,
    push_line_string
);
impl_concatenate!(
    PolygonArray<O, 2>,
    PolygonCapacity,
    PolygonBuilder<O, 2>,
    push_polygon
);
impl_concatenate!(
    MultiPointArray<O, 2>,
    MultiPointCapacity,
    MultiPointBuilder<O, 2>,
    push_multi_point
);
impl_concatenate!(
    MultiLineStringArray<O, 2>,
    MultiLineStringCapacity,
    MultiLineStringBuilder<O, 2>,
    push_multi_line_string
);
impl_concatenate!(
    MultiPolygonArray<O, 2>,
    MultiPolygonCapacity,
    MultiPolygonBuilder<O, 2>,
    push_multi_polygon
);
impl_concatenate!(
    MixedGeometryArray<O, 2>,
    MixedCapacity,
    MixedGeometryBuilder<O, 2>,
    push_geometry
);
impl_concatenate!(
    GeometryCollectionArray<O, 2>,
    GeometryCollectionCapacity,
    GeometryCollectionBuilder<O, 2>,
    push_geometry_collection
);

impl Concatenate for ChunkedPointArray<2> {
    type Output = Result<PointArray<2>>;

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

impl_chunked_concatenate!(ChunkedLineStringArray<O, 2>, LineStringArray<O, 2>);
impl_chunked_concatenate!(ChunkedPolygonArray<O, 2>, PolygonArray<O, 2>);
impl_chunked_concatenate!(ChunkedMultiPointArray<O, 2>, MultiPointArray<O, 2>);
impl_chunked_concatenate!(ChunkedMultiLineStringArray<O, 2>, MultiLineStringArray<O, 2>);
impl_chunked_concatenate!(ChunkedMultiPolygonArray<O, 2>, MultiPolygonArray<O, 2>);
impl_chunked_concatenate!(ChunkedMixedGeometryArray<O, 2>, MixedGeometryArray<O, 2>);
impl_chunked_concatenate!(
    ChunkedGeometryCollectionArray<O, 2>,
    GeometryCollectionArray<O, 2>
);
