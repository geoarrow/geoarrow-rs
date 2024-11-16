use arrow::datatypes::ArrowPrimitiveType;
use arrow_array::builder::BooleanBuilder;
use arrow_array::{BooleanArray, PrimitiveArray};
use arrow_buffer::ArrowNativeType;
use arrow_buffer::{BooleanBufferBuilder, BufferBuilder, MutableBuffer, NullBuffer};
use arrow_data::ArrayData;

use crate::array::*;
use crate::error::{GeoArrowError, Result};
use crate::trait_::ArrayAccessor;

pub trait Binary<'a, Rhs: ArrayAccessor<'a> = Self>: ArrayAccessor<'a> {
    fn binary_boolean<F>(&'a self, rhs: &'a Rhs, op: F) -> Result<BooleanArray>
    where
        F: Fn(Self::Item, Rhs::Item) -> bool,
    {
        if self.len() != rhs.len() {
            return Err(GeoArrowError::General(
                "Cannot perform binary operation on arrays of different length".to_string(),
            ));
        }

        if self.is_empty() {
            return Ok(BooleanBuilder::new().finish());
        }

        let nulls = NullBuffer::union(self.nulls(), rhs.nulls());
        let mut builder = BooleanBufferBuilder::new(self.len());
        self.iter_values()
            .zip(rhs.iter_values())
            .for_each(|(left, right)| builder.append(op(left, right)));
        Ok(BooleanArray::new(builder.finish(), nulls))
    }

    fn try_binary_boolean<F>(&'a self, rhs: &'a Rhs, op: F) -> Result<BooleanArray>
    where
        F: Fn(Self::Item, Rhs::Item) -> Result<bool>,
    {
        if self.len() != rhs.len() {
            return Err(GeoArrowError::General(
                "Cannot perform binary operation on arrays of different length".to_string(),
            ));
        }

        if self.is_empty() {
            return Ok(BooleanBuilder::new().finish());
        }
        let len = self.len();

        if self.null_count() == 0 && rhs.null_count() == 0 {
            let mut builder = BooleanBufferBuilder::new(len);
            for idx in 0..len {
                let (left, right) =
                    unsafe { (self.value_unchecked(idx), rhs.value_unchecked(idx)) };
                builder.append(op(left, right)?);
            }
            Ok(BooleanArray::new(builder.finish(), None))
        } else {
            let nulls = NullBuffer::union(self.nulls(), rhs.nulls()).unwrap();

            let mut buffer = BooleanBufferBuilder::new(len);
            buffer.append_n(len, false);

            nulls.try_for_each_valid_idx(|idx| {
                let (left, right) =
                    unsafe { (self.value_unchecked(idx), rhs.value_unchecked(idx)) };
                buffer.set_bit(idx, op(left, right)?);
                Ok::<_, GeoArrowError>(())
            })?;

            Ok(BooleanArray::new(buffer.finish(), Some(nulls)))
        }
    }

    fn try_binary_primitive<F, O>(&'a self, rhs: &'a Rhs, op: F) -> Result<PrimitiveArray<O>>
    where
        O: ArrowPrimitiveType,
        F: Fn(Self::Item, Rhs::Item) -> Result<O::Native>,
    {
        if self.len() != rhs.len() {
            return Err(GeoArrowError::General(
                "Cannot perform binary operation on arrays of different length".to_string(),
            ));
        }

        if self.is_empty() {
            return Ok(PrimitiveArray::from(ArrayData::new_empty(&O::DATA_TYPE)));
        }

        let len = self.len();

        if self.null_count() == 0 && rhs.null_count() == 0 {
            let mut buffer = MutableBuffer::new(len * O::Native::get_byte_width());
            for idx in 0..len {
                unsafe {
                    buffer.push_unchecked(op(self.value_unchecked(idx), rhs.value_unchecked(idx))?);
                };
            }
            Ok(PrimitiveArray::new(buffer.into(), None))
        } else {
            let nulls = NullBuffer::union(self.nulls(), rhs.nulls()).unwrap();

            let mut buffer = BufferBuilder::<O::Native>::new(len);
            buffer.append_n_zeroed(len);
            let slice = buffer.as_slice_mut();

            nulls.try_for_each_valid_idx(|idx| {
                unsafe {
                    *slice.get_unchecked_mut(idx) =
                        op(self.value_unchecked(idx), rhs.value_unchecked(idx))?
                };
                Ok::<_, GeoArrowError>(())
            })?;

            let values = buffer.finish().into();
            Ok(PrimitiveArray::new(values, Some(nulls)))
        }
    }
}

// Implementations on PointArray
impl<'a> Binary<'a, PointArray> for PointArray {}
impl<'a> Binary<'a, PointArray> for RectArray {}
impl<'a> Binary<'a, PointArray> for LineStringArray {}
impl<'a> Binary<'a, PointArray> for PolygonArray {}
impl<'a> Binary<'a, PointArray> for MultiPointArray {}
impl<'a> Binary<'a, PointArray> for MultiLineStringArray {}
impl<'a> Binary<'a, PointArray> for MultiPolygonArray {}
impl<'a> Binary<'a, PointArray> for MixedGeometryArray {}
impl<'a> Binary<'a, PointArray> for GeometryCollectionArray {}

// Implementations on LineStringArray
impl<'a> Binary<'a, LineStringArray> for PointArray {}
impl<'a> Binary<'a, LineStringArray> for RectArray {}
impl<'a> Binary<'a, LineStringArray> for LineStringArray {}
impl<'a> Binary<'a, LineStringArray> for PolygonArray {}
impl<'a> Binary<'a, LineStringArray> for MultiPointArray {}
impl<'a> Binary<'a, LineStringArray> for MultiLineStringArray {}
impl<'a> Binary<'a, LineStringArray> for MultiPolygonArray {}
impl<'a> Binary<'a, LineStringArray> for MixedGeometryArray {}
impl<'a> Binary<'a, LineStringArray> for GeometryCollectionArray {}

// Implementations on PolygonArray
impl<'a> Binary<'a, PolygonArray> for PointArray {}
impl<'a> Binary<'a, PolygonArray> for RectArray {}
impl<'a> Binary<'a, PolygonArray> for LineStringArray {}
impl<'a> Binary<'a, PolygonArray> for PolygonArray {}
impl<'a> Binary<'a, PolygonArray> for MultiPointArray {}
impl<'a> Binary<'a, PolygonArray> for MultiLineStringArray {}
impl<'a> Binary<'a, PolygonArray> for MultiPolygonArray {}
impl<'a> Binary<'a, PolygonArray> for MixedGeometryArray {}
impl<'a> Binary<'a, PolygonArray> for GeometryCollectionArray {}

// Implementations on MultiPointArray
impl<'a> Binary<'a, MultiPointArray> for PointArray {}
impl<'a> Binary<'a, MultiPointArray> for RectArray {}
impl<'a> Binary<'a, MultiPointArray> for LineStringArray {}
impl<'a> Binary<'a, MultiPointArray> for PolygonArray {}
impl<'a> Binary<'a, MultiPointArray> for MultiPointArray {}
impl<'a> Binary<'a, MultiPointArray> for MultiLineStringArray {}
impl<'a> Binary<'a, MultiPointArray> for MultiPolygonArray {}
impl<'a> Binary<'a, MultiPointArray> for MixedGeometryArray {}
impl<'a> Binary<'a, MultiPointArray> for GeometryCollectionArray {}

// Implementations on MultiLineStringArray
impl<'a> Binary<'a, MultiLineStringArray> for PointArray {}
impl<'a> Binary<'a, MultiLineStringArray> for RectArray {}
impl<'a> Binary<'a, MultiLineStringArray> for LineStringArray {}
impl<'a> Binary<'a, MultiLineStringArray> for PolygonArray {}
impl<'a> Binary<'a, MultiLineStringArray> for MultiPointArray {}
impl<'a> Binary<'a, MultiLineStringArray> for MultiLineStringArray {}
impl<'a> Binary<'a, MultiLineStringArray> for MultiPolygonArray {}
impl<'a> Binary<'a, MultiLineStringArray> for MixedGeometryArray {}
impl<'a> Binary<'a, MultiLineStringArray> for GeometryCollectionArray {}

// Implementations on MultiPolygonArray
impl<'a> Binary<'a, MultiPolygonArray> for PointArray {}
impl<'a> Binary<'a, MultiPolygonArray> for RectArray {}
impl<'a> Binary<'a, MultiPolygonArray> for LineStringArray {}
impl<'a> Binary<'a, MultiPolygonArray> for PolygonArray {}
impl<'a> Binary<'a, MultiPolygonArray> for MultiPointArray {}
impl<'a> Binary<'a, MultiPolygonArray> for MultiLineStringArray {}
impl<'a> Binary<'a, MultiPolygonArray> for MultiPolygonArray {}
impl<'a> Binary<'a, MultiPolygonArray> for MixedGeometryArray {}
impl<'a> Binary<'a, MultiPolygonArray> for GeometryCollectionArray {}

// Implementations on MixedGeometryArray
impl<'a> Binary<'a, MixedGeometryArray> for PointArray {}
impl<'a> Binary<'a, MixedGeometryArray> for RectArray {}
impl<'a> Binary<'a, MixedGeometryArray> for LineStringArray {}
impl<'a> Binary<'a, MixedGeometryArray> for PolygonArray {}
impl<'a> Binary<'a, MixedGeometryArray> for MultiPointArray {}
impl<'a> Binary<'a, MixedGeometryArray> for MultiLineStringArray {}
impl<'a> Binary<'a, MixedGeometryArray> for MultiPolygonArray {}
impl<'a> Binary<'a, MixedGeometryArray> for MixedGeometryArray {}
impl<'a> Binary<'a, MixedGeometryArray> for GeometryCollectionArray {}

// Implementations on GeometryCollectionArray
impl<'a> Binary<'a, GeometryCollectionArray> for PointArray {}
impl<'a> Binary<'a, GeometryCollectionArray> for RectArray {}
impl<'a> Binary<'a, GeometryCollectionArray> for LineStringArray {}
impl<'a> Binary<'a, GeometryCollectionArray> for PolygonArray {}
impl<'a> Binary<'a, GeometryCollectionArray> for MultiPointArray {}
impl<'a> Binary<'a, GeometryCollectionArray> for MultiLineStringArray {}
impl<'a> Binary<'a, GeometryCollectionArray> for MultiPolygonArray {}
impl<'a> Binary<'a, GeometryCollectionArray> for MixedGeometryArray {}
impl<'a> Binary<'a, GeometryCollectionArray> for GeometryCollectionArray {}
