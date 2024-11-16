use std::ops::Range;

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

impl Rechunk for PointArray {
    type Output = ChunkedGeometryArray<PointArray>;

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
        impl Rechunk for $array_type {
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

rechunk_impl!(LineStringArray);
rechunk_impl!(PolygonArray);
rechunk_impl!(MultiPointArray);
rechunk_impl!(MultiLineStringArray);
rechunk_impl!(MultiPolygonArray);
rechunk_impl!(MixedGeometryArray);
rechunk_impl!(GeometryCollectionArray);

// impl<O: OffsetSizeTrait> Rechunk for LineStringArray {
//     type Output = Result<ChunkedGeometryArray<Self>>;

//     fn rechunk(&self, ranges: &[Range<usize>]) -> Self::Output {
//         let mut output_arrays = Vec::with_capacity(ranges.len());
//         for range in ranges {
//             output_arrays.push(self.take_range(range)?);
//         }
//         Ok(ChunkedGeometryArray::new(output_arrays))
//     }
// }
