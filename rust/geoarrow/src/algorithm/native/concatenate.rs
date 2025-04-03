use std::collections::HashSet;

use crate::array::*;
use crate::chunked_array::*;
use crate::error::Result;
use crate::trait_::ArrayAccessor;
use geoarrow_schema::Dimension;

pub trait Concatenate: Sized {
    type Output;

    fn concatenate(&self) -> Self::Output;
}

impl Concatenate for &[PointArray] {
    type Output = Result<PointArray>;

    fn concatenate(&self) -> Self::Output {
        let common_dimension = infer_common_dimension(self.iter().map(|arr| arr.dimension()));

        let output_capacity = self.iter().fold(0, |sum, val| sum + val.buffer_lengths());
        let mut builder = PointBuilder::with_capacity(common_dimension, output_capacity);
        self.iter()
            .for_each(|chunk| chunk.iter().for_each(|p| builder.push_point(p.as_ref())));
        Ok(builder.finish())
    }
}

macro_rules! impl_concatenate {
    ($array:ty, $capacity:ty, $builder:ty, $push_func:ident) => {
        impl Concatenate for &[$array] {
            type Output = Result<$array>;

            fn concatenate(&self) -> Self::Output {
                let common_dimension =
                    infer_common_dimension(self.iter().map(|arr| arr.dimension()));

                let output_capacity = self.iter().fold(<$capacity>::new_empty(), |sum, val| {
                    sum + val.buffer_lengths()
                });
                let mut builder = <$builder>::with_capacity(common_dimension, output_capacity);
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
    LineStringArray,
    LineStringCapacity,
    LineStringBuilder,
    push_line_string
);
impl_concatenate!(PolygonArray, PolygonCapacity, PolygonBuilder, push_polygon);
impl_concatenate!(
    MultiPointArray,
    MultiPointCapacity,
    MultiPointBuilder,
    push_multi_point
);
impl_concatenate!(
    MultiLineStringArray,
    MultiLineStringCapacity,
    MultiLineStringBuilder,
    push_multi_line_string
);
impl_concatenate!(
    MultiPolygonArray,
    MultiPolygonCapacity,
    MultiPolygonBuilder,
    push_multi_polygon
);
impl_concatenate!(
    MixedGeometryArray,
    MixedCapacity,
    MixedGeometryBuilder,
    push_geometry
);
impl_concatenate!(
    GeometryCollectionArray,
    GeometryCollectionCapacity,
    GeometryCollectionBuilder,
    push_geometry_collection
);

fn infer_common_dimension(dimensions: impl Iterator<Item = Dimension>) -> Dimension {
    let dimensions: HashSet<Dimension> = HashSet::from_iter(dimensions);
    assert_eq!(dimensions.len(), 1);
    dimensions.into_iter().next().unwrap()
}

impl Concatenate for ChunkedPointArray {
    type Output = Result<PointArray>;

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

impl_chunked_concatenate!(ChunkedLineStringArray, LineStringArray);
impl_chunked_concatenate!(ChunkedPolygonArray, PolygonArray);
impl_chunked_concatenate!(ChunkedMultiPointArray, MultiPointArray);
impl_chunked_concatenate!(ChunkedMultiLineStringArray, MultiLineStringArray);
impl_chunked_concatenate!(ChunkedMultiPolygonArray, MultiPolygonArray);
impl_chunked_concatenate!(ChunkedMixedGeometryArray, MixedGeometryArray);
impl_chunked_concatenate!(ChunkedGeometryCollectionArray, GeometryCollectionArray);
