#[cfg(feature = "rayon")]
use rayon::prelude::*;

use crate::NativeArray;
use crate::chunked_array::*;
use crate::error::Result;

pub trait MapChunks {
    type Chunk;

    fn map<F, R>(&self, map_op: F) -> Vec<R>
    where
        F: Fn(&Self::Chunk) -> R + Sync + Send,
        R: Send;
    fn try_map<F, R>(&self, map_op: F) -> Result<Vec<R>>
    where
        F: Fn(&Self::Chunk) -> Result<R> + Sync + Send,
        R: Send;

    #[cfg(feature = "rayon")]
    fn binary_map<F, R, Z>(&self, other: Z, map_op: F) -> Vec<R>
    where
        F: Fn((&Self::Chunk, Z::Item)) -> R + Sync + Send,
        R: Send,
        Z: IntoParallelIterator,
        Z::Iter: IndexedParallelIterator;

    #[cfg(not(feature = "rayon"))]
    fn binary_map<F, R, C>(&self, other: &[C], map_op: F) -> Vec<R>
    where
        F: Fn((&Self::Chunk, &C)) -> R + Sync + Send,
        R: Send;

    #[cfg(feature = "rayon")]
    fn try_binary_map<F, R, Z>(&self, other: Z, map_op: F) -> Result<Vec<R>>
    where
        F: Fn((&Self::Chunk, Z::Item)) -> Result<R> + Sync + Send,
        R: Send,
        Z: IntoParallelIterator,
        Z::Iter: IndexedParallelIterator;

    #[cfg(not(feature = "rayon"))]
    fn try_binary_map<F, R, C>(&self, other: &[C], map_op: F) -> Result<Vec<R>>
    where
        F: Fn((&Self::Chunk, &C)) -> Result<R> + Sync + Send,
        R: Send;
}

impl<G: NativeArray> MapChunks for ChunkedGeometryArray<G> {
    type Chunk = G;

    fn map<F, R>(&self, map_op: F) -> Vec<R>
    where
        F: Fn(&Self::Chunk) -> R + Sync + Send,
        R: Send,
    {
        #[cfg(feature = "rayon")]
        {
            let mut output_vec = Vec::with_capacity(self.chunks.len());
            self.chunks
                .par_iter()
                .map(map_op)
                .collect_into_vec(&mut output_vec);
            output_vec
        }

        #[cfg(not(feature = "rayon"))]
        {
            self.chunks.iter().map(map_op).collect()
        }
    }

    fn try_map<F, R>(&self, map_op: F) -> Result<Vec<R>>
    where
        F: Fn(&Self::Chunk) -> Result<R> + Sync + Send,
        R: Send,
    {
        #[cfg(feature = "rayon")]
        {
            self.chunks.par_iter().map(map_op).collect()
        }

        #[cfg(not(feature = "rayon"))]
        {
            self.chunks.iter().map(map_op).collect()
        }
    }

    #[cfg(feature = "rayon")]
    fn binary_map<F, R, Z>(&self, other: Z, map_op: F) -> Vec<R>
    where
        F: Fn((&Self::Chunk, Z::Item)) -> R + Sync + Send,
        R: Send,
        Z: IntoParallelIterator,
        Z::Iter: IndexedParallelIterator,
    {
        let mut output_vec = Vec::with_capacity(self.chunks.len());
        self.chunks()
            .par_iter()
            .zip_eq(other)
            .map(map_op)
            .collect_into_vec(&mut output_vec);
        output_vec
    }

    #[cfg(not(feature = "rayon"))]
    fn binary_map<F, R, C>(&self, other: &[C], map_op: F) -> Vec<R>
    where
        F: Fn((&Self::Chunk, &C)) -> R + Sync + Send,
        R: Send,
    {
        self.chunks.iter().zip(other).map(map_op).collect()
    }

    #[cfg(feature = "rayon")]
    fn try_binary_map<F, R, Z>(&self, other: Z, map_op: F) -> Result<Vec<R>>
    where
        F: Fn((&Self::Chunk, Z::Item)) -> Result<R> + Sync + Send,
        R: Send,
        Z: IntoParallelIterator,
        Z::Iter: IndexedParallelIterator,
    {
        let mut output_vec = Vec::with_capacity(self.chunks.len());
        self.chunks()
            .par_iter()
            .zip_eq(other)
            .map(map_op)
            .collect_into_vec(&mut output_vec);
        output_vec.into_iter().collect()
    }

    #[cfg(not(feature = "rayon"))]
    fn try_binary_map<F, R, C>(&self, other: &[C], map_op: F) -> Result<Vec<R>>
    where
        F: Fn((&Self::Chunk, &C)) -> Result<R> + Sync + Send,
        R: Send,
    {
        self.chunks.iter().zip(other).map(map_op).collect()
    }
}
