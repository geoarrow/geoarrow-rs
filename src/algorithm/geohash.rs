use crate::array::*;
use crate::chunked_array::{ChunkedArray, ChunkedGeometryArrayTrait, ChunkedPointArray};
use crate::datatypes::GeoDataType;
use crate::error::{GeoArrowError, Result};
use crate::trait_::GeometryArrayAccessor;
use crate::GeometryArrayTrait;
use arrow::array::GenericStringBuilder;
use arrow_array::{GenericStringArray, OffsetSizeTrait};
use geohash::encode;

pub trait GeohashEncode {
    type Output<O: OffsetSizeTrait>;

    fn encode<O: OffsetSizeTrait>(&self, len: usize) -> Self::Output<O>;
}

impl GeohashEncode for PointArray {
    type Output<O: OffsetSizeTrait> = Result<GenericStringArray<O>>;

    fn encode<O: OffsetSizeTrait>(&self, len: usize) -> Self::Output<O> {
        let mut builder = GenericStringBuilder::with_capacity(self.len(), self.len() * len);
        for maybe_point in self.iter_geo() {
            if let Some(point) = maybe_point {
                builder.append_value(encode(point.0, len)?);
            } else {
                builder.append_null()
            }
        }
        Ok(builder.finish())
    }
}

impl GeohashEncode for &dyn GeometryArrayTrait {
    type Output<O: OffsetSizeTrait> = Result<GenericStringArray<O>>;

    fn encode<O: OffsetSizeTrait>(&self, len: usize) -> Self::Output<O> {
        match self.data_type() {
            GeoDataType::Point(_) => self.as_point().encode(len),
            _ => Err(GeoArrowError::IncorrectType("".into())),
        }
    }
}

impl GeohashEncode for ChunkedPointArray {
    type Output<O: OffsetSizeTrait> = Result<ChunkedArray<GenericStringArray<O>>>;

    fn encode<O: OffsetSizeTrait>(&self, len: usize) -> Self::Output<O> {
        let chunks = self.try_map(|chunk| chunk.encode(len))?;
        Ok(ChunkedArray::new(chunks))
    }
}

impl GeohashEncode for &dyn ChunkedGeometryArrayTrait {
    type Output<O: OffsetSizeTrait> = Result<ChunkedArray<GenericStringArray<O>>>;

    fn encode<O: OffsetSizeTrait>(&self, len: usize) -> Self::Output<O> {
        match self.data_type() {
            GeoDataType::Point(_) => self.as_point().encode(len),
            _ => Err(GeoArrowError::IncorrectType("".into())),
        }
    }
}

// pub trait GeohashDecode {
//     type Output<O: OffsetSizeTrait>;

//     fn decode<O: OffsetSizeTrait>(&self, len: usize) -> Self::Output<O>;
// }
