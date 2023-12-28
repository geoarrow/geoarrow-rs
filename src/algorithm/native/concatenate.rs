use arrow_array::OffsetSizeTrait;

use crate::array::*;
use crate::chunked_array::*;
use crate::error::Result;

pub trait Concatenate: Sized {
    type Output;

    fn concatenate(chunks: &[Self]) -> Self::Output;
}

impl Concatenate for PointArray {
    type Output = Self;

    fn concatenate(chunks: &[Self]) -> Self::Output {
        let output_capacity = chunks.iter().fold(0, |sum, val| sum + val.buffer_lengths());
        let mut builder = PointBuilder::with_capacity(output_capacity);
        chunks
            .iter()
            .for_each(|chunk| chunk.iter().for_each(|p| builder.push_point(p.as_ref())));
        builder.finish()
    }
}

impl<O: OffsetSizeTrait> Concatenate for LineStringArray<O> {
    type Output = Result<Self>;

    fn concatenate(chunks: &[Self]) -> Self::Output {
        let output_capacity = chunks
            .iter()
            .fold(LineStringCapacity::new_empty(), |sum, val| {
                sum + val.buffer_lengths()
            });
        let mut builder = LineStringBuilder::with_capacity(output_capacity);
        for chunk in chunks {
            for geom in chunk.iter() {
                builder.push_line_string(geom.as_ref())?;
            }
        }
        Ok(builder.finish())
    }
}

// TODO: put into macro

pub trait ConcatenateChunked {
    type Output;

    fn concatenate(&self) -> Self::Output;
}

impl ConcatenateChunked for ChunkedPointArray {
    type Output = PointArray;

    fn concatenate(&self) -> Self::Output {
        let output_capacity = self
            .chunks
            .iter()
            .fold(0, |sum, val| sum + val.buffer_lengths());
        let mut builder = PointBuilder::with_capacity(output_capacity);
        self.chunks
            .iter()
            .for_each(|chunk| chunk.iter().for_each(|p| builder.push_point(p.as_ref())));
        builder.finish()
    }
}
