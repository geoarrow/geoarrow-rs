use std::ops::Range;

use arrow_array::OffsetSizeTrait;

use crate::algorithm::native::Take;
use crate::array::*;
use crate::chunked_array::ChunkedGeometryArray;
use crate::error::Result;

pub trait Rechunk {
    type Output;

    fn rechunk(&self, chunks: &[Range<usize>]) -> Self::Output;

    // /// Rechunk the input given a number of geometries per output chunk
    // fn rechunk_num_geoms(&self, n_geoms_per_chunk: usize) -> Self::Output {
    //     let num_geoms = self.len
    // }
}

impl Rechunk for PointArray<2> {
    type Output = ChunkedGeometryArray<PointArray<2>>;

    fn rechunk(&self, ranges: &[Range<usize>]) -> Self::Output {
        let mut output_arrays = Vec::with_capacity(ranges.len());
        for range in ranges {
            output_arrays.push(self.take_range(range));
        }
        ChunkedGeometryArray::new(output_arrays)
    }

    // fn rechunk_num_geoms(&self, n_geoms_per_chunk: usize) -> Self::Output {
    //     let num_coords = self.len();
    //     let mut chunks = vec![];
    //     let mut counter = 0;
    //     while counter < self.len() {
    //         counter += n_geoms_per_chunk;
    //         chunks
    //     }

    //     self.rechunk(chunks.as_slice())
    // }
}

macro_rules! rechunk_impl {
    ($array_type:ty) => {
        impl<O: OffsetSizeTrait> Rechunk for $array_type {
            type Output = Result<ChunkedGeometryArray<Self>>;

            fn rechunk(&self, ranges: &[Range<usize>]) -> Self::Output {
                let mut output_arrays = Vec::with_capacity(ranges.len());
                for range in ranges {
                    output_arrays.push(self.take_range(range)?);
                }
                Ok(ChunkedGeometryArray::new(output_arrays))
            }
        }
    };
}

rechunk_impl!(LineStringArray<O, 2>);
rechunk_impl!(PolygonArray<O, 2>);
rechunk_impl!(MultiPointArray<O, 2>);
rechunk_impl!(MultiLineStringArray<O, 2>);
rechunk_impl!(MultiPolygonArray<O, 2>);
rechunk_impl!(MixedGeometryArray<O, 2>);
rechunk_impl!(GeometryCollectionArray<O, 2>);

// impl<O: OffsetSizeTrait> Rechunk for LineStringArray<O, 2> {
//     type Output = Result<ChunkedGeometryArray<Self>>;

//     fn rechunk(&self, ranges: &[Range<usize>]) -> Self::Output {
//         let mut output_arrays = Vec::with_capacity(ranges.len());
//         for range in ranges {
//             output_arrays.push(self.take_range(range)?);
//         }
//         Ok(ChunkedGeometryArray::new(output_arrays))
//     }
// }
