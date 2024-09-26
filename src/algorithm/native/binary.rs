use arrow::datatypes::ArrowPrimitiveType;
use arrow_array::builder::BooleanBuilder;
use arrow_array::{BooleanArray, OffsetSizeTrait, PrimitiveArray};
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

// Implementations on PointArray<2>
impl<'a> Binary<'a, PointArray<2>> for PointArray<2> {}
impl<'a> Binary<'a, PointArray<2>> for RectArray<2> {}
impl<'a, O: OffsetSizeTrait> Binary<'a, PointArray<2>> for LineStringArray<O, 2> {}
impl<'a, O: OffsetSizeTrait> Binary<'a, PointArray<2>> for PolygonArray<O, 2> {}
impl<'a, O: OffsetSizeTrait> Binary<'a, PointArray<2>> for MultiPointArray<O, 2> {}
impl<'a, O: OffsetSizeTrait> Binary<'a, PointArray<2>> for MultiLineStringArray<O, 2> {}
impl<'a, O: OffsetSizeTrait> Binary<'a, PointArray<2>> for MultiPolygonArray<O, 2> {}
impl<'a, O: OffsetSizeTrait> Binary<'a, PointArray<2>> for MixedGeometryArray<O, 2> {}
impl<'a, O: OffsetSizeTrait> Binary<'a, PointArray<2>> for GeometryCollectionArray<O, 2> {}

// Implementations on LineStringArray
impl<'a, O: OffsetSizeTrait> Binary<'a, LineStringArray<O, 2>> for PointArray<2> {}
impl<'a, O: OffsetSizeTrait> Binary<'a, LineStringArray<O, 2>> for RectArray<2> {}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, LineStringArray<O1, 2>>
    for LineStringArray<O2, 2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, LineStringArray<O1, 2>>
    for PolygonArray<O2, 2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, LineStringArray<O1, 2>>
    for MultiPointArray<O2, 2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, LineStringArray<O1, 2>>
    for MultiLineStringArray<O2, 2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, LineStringArray<O1, 2>>
    for MultiPolygonArray<O2, 2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, LineStringArray<O1, 2>>
    for MixedGeometryArray<O2, 2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, LineStringArray<O1, 2>>
    for GeometryCollectionArray<O2, 2>
{
}

// Implementations on PolygonArray
impl<'a, O: OffsetSizeTrait> Binary<'a, PolygonArray<O, 2>> for PointArray<2> {}
impl<'a, O: OffsetSizeTrait> Binary<'a, PolygonArray<O, 2>> for RectArray<2> {}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, PolygonArray<O1, 2>>
    for LineStringArray<O2, 2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, PolygonArray<O1, 2>>
    for PolygonArray<O2, 2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, PolygonArray<O1, 2>>
    for MultiPointArray<O2, 2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, PolygonArray<O1, 2>>
    for MultiLineStringArray<O2, 2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, PolygonArray<O1, 2>>
    for MultiPolygonArray<O2, 2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, PolygonArray<O1, 2>>
    for MixedGeometryArray<O2, 2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, PolygonArray<O1, 2>>
    for GeometryCollectionArray<O2, 2>
{
}

// Implementations on MultiPointArray
impl<'a, O: OffsetSizeTrait> Binary<'a, MultiPointArray<O, 2>> for PointArray<2> {}
impl<'a, O: OffsetSizeTrait> Binary<'a, MultiPointArray<O, 2>> for RectArray<2> {}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, MultiPointArray<O1, 2>>
    for LineStringArray<O2, 2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, MultiPointArray<O1, 2>>
    for PolygonArray<O2, 2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, MultiPointArray<O1, 2>>
    for MultiPointArray<O2, 2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, MultiPointArray<O1, 2>>
    for MultiLineStringArray<O2, 2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, MultiPointArray<O1, 2>>
    for MultiPolygonArray<O2, 2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, MultiPointArray<O1, 2>>
    for MixedGeometryArray<O2, 2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, MultiPointArray<O1, 2>>
    for GeometryCollectionArray<O2, 2>
{
}

// Implementations on MultiLineStringArray
impl<'a, O: OffsetSizeTrait> Binary<'a, MultiLineStringArray<O, 2>> for PointArray<2> {}
impl<'a, O: OffsetSizeTrait> Binary<'a, MultiLineStringArray<O, 2>> for RectArray<2> {}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, MultiLineStringArray<O1, 2>>
    for LineStringArray<O2, 2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, MultiLineStringArray<O1, 2>>
    for PolygonArray<O2, 2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, MultiLineStringArray<O1, 2>>
    for MultiPointArray<O2, 2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, MultiLineStringArray<O1, 2>>
    for MultiLineStringArray<O2, 2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, MultiLineStringArray<O1, 2>>
    for MultiPolygonArray<O2, 2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, MultiLineStringArray<O1, 2>>
    for MixedGeometryArray<O2, 2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, MultiLineStringArray<O1, 2>>
    for GeometryCollectionArray<O2, 2>
{
}

// Implementations on MultiPolygonArray
impl<'a, O: OffsetSizeTrait> Binary<'a, MultiPolygonArray<O, 2>> for PointArray<2> {}
impl<'a, O: OffsetSizeTrait> Binary<'a, MultiPolygonArray<O, 2>> for RectArray<2> {}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, MultiPolygonArray<O1, 2>>
    for LineStringArray<O2, 2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, MultiPolygonArray<O1, 2>>
    for PolygonArray<O2, 2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, MultiPolygonArray<O1, 2>>
    for MultiPointArray<O2, 2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, MultiPolygonArray<O1, 2>>
    for MultiLineStringArray<O2, 2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, MultiPolygonArray<O1, 2>>
    for MultiPolygonArray<O2, 2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, MultiPolygonArray<O1, 2>>
    for MixedGeometryArray<O2, 2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, MultiPolygonArray<O1, 2>>
    for GeometryCollectionArray<O2, 2>
{
}

// Implementations on MixedGeometryArray
impl<'a, O: OffsetSizeTrait> Binary<'a, MixedGeometryArray<O, 2>> for PointArray<2> {}
impl<'a, O: OffsetSizeTrait> Binary<'a, MixedGeometryArray<O, 2>> for RectArray<2> {}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, MixedGeometryArray<O1, 2>>
    for LineStringArray<O2, 2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, MixedGeometryArray<O1, 2>>
    for PolygonArray<O2, 2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, MixedGeometryArray<O1, 2>>
    for MultiPointArray<O2, 2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, MixedGeometryArray<O1, 2>>
    for MultiLineStringArray<O2, 2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, MixedGeometryArray<O1, 2>>
    for MultiPolygonArray<O2, 2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, MixedGeometryArray<O1, 2>>
    for MixedGeometryArray<O2, 2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, MixedGeometryArray<O1, 2>>
    for GeometryCollectionArray<O2, 2>
{
}

// Implementations on GeometryCollectionArray
impl<'a, O: OffsetSizeTrait> Binary<'a, GeometryCollectionArray<O, 2>> for PointArray<2> {}
impl<'a, O: OffsetSizeTrait> Binary<'a, GeometryCollectionArray<O, 2>> for RectArray<2> {}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, GeometryCollectionArray<O1, 2>>
    for LineStringArray<O2, 2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, GeometryCollectionArray<O1, 2>>
    for PolygonArray<O2, 2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, GeometryCollectionArray<O1, 2>>
    for MultiPointArray<O2, 2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, GeometryCollectionArray<O1, 2>>
    for MultiLineStringArray<O2, 2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, GeometryCollectionArray<O1, 2>>
    for MultiPolygonArray<O2, 2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, GeometryCollectionArray<O1, 2>>
    for MixedGeometryArray<O2, 2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, GeometryCollectionArray<O1, 2>>
    for GeometryCollectionArray<O2, 2>
{
}
