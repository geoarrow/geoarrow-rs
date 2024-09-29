use arrow_array::OffsetSizeTrait;

use crate::array::*;
use crate::chunked_array::*;
use crate::error::Result;
use crate::trait_::ArrayAccessor;

pub trait Concatenate: Sized {
    type Output;

    fn concatenate(&self) -> Self::Output;
}

impl Concatenate for &[PointArray<2>] {
    type Output = Result<PointArray<2>>;

    fn concatenate(&self) -> Self::Output {
        let output_capacity = self.iter().fold(0, |sum, val| sum + val.buffer_lengths());
        let mut builder = PointBuilder::with_capacity(output_capacity);
        self.iter().for_each(|chunk| chunk.iter().for_each(|p| builder.push_point(p.as_ref())));
        Ok(builder.finish())
    }
}

macro_rules! impl_concatenate {
    ($array:ty, $capacity:ty, $builder:ty, $push_func:ident) => {
        impl Concatenate for &[$array] {
            type Output = Result<$array>;

            fn concatenate(&self) -> Self::Output {
                let output_capacity = self.iter().fold(<$capacity>::new_empty(), |sum, val| sum + val.buffer_lengths());
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

impl_concatenate!(LineStringArray<2>, LineStringCapacity, LineStringBuilder<2>, push_line_string);
impl_concatenate!(PolygonArray<2>, PolygonCapacity, PolygonBuilder<2>, push_polygon);
impl_concatenate!(MultiPointArray<2>, MultiPointCapacity, MultiPointBuilder<2>, push_multi_point);
impl_concatenate!(MultiLineStringArray<2>, MultiLineStringCapacity, MultiLineStringBuilder<2>, push_multi_line_string);
impl_concatenate!(MultiPolygonArray<2>, MultiPolygonCapacity, MultiPolygonBuilder<2>, push_multi_polygon);
impl_concatenate!(MixedGeometryArray<2>, MixedCapacity, MixedGeometryBuilder<2>, push_geometry);
impl_concatenate!(GeometryCollectionArray<2>, GeometryCollectionCapacity, GeometryCollectionBuilder<2>, push_geometry_collection);

impl Concatenate for ChunkedPointArray<2> {
    type Output = Result<PointArray<2>>;

    fn concatenate(&self) -> Self::Output {
        self.chunks.as_slice().concatenate()
    }
}

macro_rules! impl_chunked_concatenate {
    ($chunked_array:ty, $output_array:ty) => {
        impl Concatenate for $chunked_array {
            type Output = Result<$output_array>;

            fn concatenate(&self) -> Self::Output {
                self.chunks.as_slice().concatenate()
            }
        }
    };
}

impl_chunked_concatenate!(ChunkedLineStringArray<2>, LineStringArray<2>);
impl_chunked_concatenate!(ChunkedPolygonArray<2>, PolygonArray<2>);
impl_chunked_concatenate!(ChunkedMultiPointArray<2>, MultiPointArray<2>);
impl_chunked_concatenate!(ChunkedMultiLineStringArray<2>, MultiLineStringArray<2>);
impl_chunked_concatenate!(ChunkedMultiPolygonArray<2>, MultiPolygonArray<2>);
impl_chunked_concatenate!(ChunkedMixedGeometryArray<2>, MixedGeometryArray<2>);
impl_chunked_concatenate!(ChunkedGeometryCollectionArray<2>, GeometryCollectionArray<2>);
