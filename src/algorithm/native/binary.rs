use arrow::datatypes::ArrowPrimitiveType;
use arrow_array::builder::BooleanBuilder;
use arrow_array::{BooleanArray, PrimitiveArray};
use arrow_buffer::ArrowNativeType;
use arrow_buffer::{BooleanBufferBuilder, BufferBuilder, MutableBuffer, NullBuffer};
use arrow_data::ArrayData;

use crate::array::*;
use crate::error::{GeoArrowError, Result};
use crate::trait_::{ArrayAccessor, NativeGeometryAccessor};

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
impl<'a> Binary<'a, PointArray<2>> for LineStringArray<2> {}
impl<'a> Binary<'a, PointArray<2>> for PolygonArray<2> {}
impl<'a> Binary<'a, PointArray<2>> for MultiPointArray<2> {}
impl<'a> Binary<'a, PointArray<2>> for MultiLineStringArray<2> {}
impl<'a> Binary<'a, PointArray<2>> for MultiPolygonArray<2> {}
impl<'a> Binary<'a, PointArray<2>> for MixedGeometryArray<2> {}
impl<'a> Binary<'a, PointArray<2>> for GeometryCollectionArray<2> {}

// Implementations on LineStringArray
impl<'a> Binary<'a, LineStringArray<2>> for PointArray<2> {}
impl<'a> Binary<'a, LineStringArray<2>> for RectArray<2> {}
impl<'a> Binary<'a, LineStringArray<2>> for LineStringArray<2> {}
impl<'a> Binary<'a, LineStringArray<2>> for PolygonArray<2> {}
impl<'a> Binary<'a, LineStringArray<2>> for MultiPointArray<2> {}
impl<'a> Binary<'a, LineStringArray<2>> for MultiLineStringArray<2> {}
impl<'a> Binary<'a, LineStringArray<2>> for MultiPolygonArray<2> {}
impl<'a> Binary<'a, LineStringArray<2>> for MixedGeometryArray<2> {}
impl<'a> Binary<'a, LineStringArray<2>> for GeometryCollectionArray<2> {}

// Implementations on PolygonArray
impl<'a> Binary<'a, PolygonArray<2>> for PointArray<2> {}
impl<'a> Binary<'a, PolygonArray<2>> for RectArray<2> {}
impl<'a> Binary<'a, PolygonArray<2>> for LineStringArray<2> {}
impl<'a> Binary<'a, PolygonArray<2>> for PolygonArray<2> {}
impl<'a> Binary<'a, PolygonArray<2>> for MultiPointArray<2> {}
impl<'a> Binary<'a, PolygonArray<2>> for MultiLineStringArray<2> {}
impl<'a> Binary<'a, PolygonArray<2>> for MultiPolygonArray<2> {}
impl<'a> Binary<'a, PolygonArray<2>> for MixedGeometryArray<2> {}
impl<'a> Binary<'a, PolygonArray<2>> for GeometryCollectionArray<2> {}

// Implementations on MultiPointArray
impl<'a> Binary<'a, MultiPointArray<2>> for PointArray<2> {}
impl<'a> Binary<'a, MultiPointArray<2>> for RectArray<2> {}
impl<'a> Binary<'a, MultiPointArray<2>> for LineStringArray<2> {}
impl<'a> Binary<'a, MultiPointArray<2>> for PolygonArray<2> {}
impl<'a> Binary<'a, MultiPointArray<2>> for MultiPointArray<2> {}
impl<'a> Binary<'a, MultiPointArray<2>> for MultiLineStringArray<2> {}
impl<'a> Binary<'a, MultiPointArray<2>> for MultiPolygonArray<2> {}
impl<'a> Binary<'a, MultiPointArray<2>> for MixedGeometryArray<2> {}
impl<'a> Binary<'a, MultiPointArray<2>> for GeometryCollectionArray<2> {}

// Implementations on MultiLineStringArray
impl<'a> Binary<'a, MultiLineStringArray<2>> for PointArray<2> {}
impl<'a> Binary<'a, MultiLineStringArray<2>> for RectArray<2> {}
impl<'a> Binary<'a, MultiLineStringArray<2>> for LineStringArray<2> {}
impl<'a> Binary<'a, MultiLineStringArray<2>> for PolygonArray<2> {}
impl<'a> Binary<'a, MultiLineStringArray<2>> for MultiPointArray<2> {}
impl<'a> Binary<'a, MultiLineStringArray<2>> for MultiLineStringArray<2> {}
impl<'a> Binary<'a, MultiLineStringArray<2>> for MultiPolygonArray<2> {}
impl<'a> Binary<'a, MultiLineStringArray<2>> for MixedGeometryArray<2> {}
impl<'a> Binary<'a, MultiLineStringArray<2>> for GeometryCollectionArray<2> {}

// Implementations on MultiPolygonArray
impl<'a> Binary<'a, MultiPolygonArray<2>> for PointArray<2> {}
impl<'a> Binary<'a, MultiPolygonArray<2>> for RectArray<2> {}
impl<'a> Binary<'a, MultiPolygonArray<2>> for LineStringArray<2> {}
impl<'a> Binary<'a, MultiPolygonArray<2>> for PolygonArray<2> {}
impl<'a> Binary<'a, MultiPolygonArray<2>> for MultiPointArray<2> {}
impl<'a> Binary<'a, MultiPolygonArray<2>> for MultiLineStringArray<2> {}
impl<'a> Binary<'a, MultiPolygonArray<2>> for MultiPolygonArray<2> {}
impl<'a> Binary<'a, MultiPolygonArray<2>> for MixedGeometryArray<2> {}
impl<'a> Binary<'a, MultiPolygonArray<2>> for GeometryCollectionArray<2> {}

// Implementations on MixedGeometryArray
impl<'a> Binary<'a, MixedGeometryArray<2>> for PointArray<2> {}
impl<'a> Binary<'a, MixedGeometryArray<2>> for RectArray<2> {}
impl<'a> Binary<'a, MixedGeometryArray<2>> for LineStringArray<2> {}
impl<'a> Binary<'a, MixedGeometryArray<2>> for PolygonArray<2> {}
impl<'a> Binary<'a, MixedGeometryArray<2>> for MultiPointArray<2> {}
impl<'a> Binary<'a, MixedGeometryArray<2>> for MultiLineStringArray<2> {}
impl<'a> Binary<'a, MixedGeometryArray<2>> for MultiPolygonArray<2> {}
impl<'a> Binary<'a, MixedGeometryArray<2>> for MixedGeometryArray<2> {}
impl<'a> Binary<'a, MixedGeometryArray<2>> for GeometryCollectionArray<2> {}

// Implementations on GeometryCollectionArray
impl<'a> Binary<'a, GeometryCollectionArray<2>> for PointArray<2> {}
impl<'a> Binary<'a, GeometryCollectionArray<2>> for RectArray<2> {}
impl<'a> Binary<'a, GeometryCollectionArray<2>> for LineStringArray<2> {}
impl<'a> Binary<'a, GeometryCollectionArray<2>> for PolygonArray<2> {}
impl<'a> Binary<'a, GeometryCollectionArray<2>> for MultiPointArray<2> {}
impl<'a> Binary<'a, GeometryCollectionArray<2>> for MultiLineStringArray<2> {}
impl<'a> Binary<'a, GeometryCollectionArray<2>> for MultiPolygonArray<2> {}
impl<'a> Binary<'a, GeometryCollectionArray<2>> for MixedGeometryArray<2> {}
impl<'a> Binary<'a, GeometryCollectionArray<2>> for GeometryCollectionArray<2> {}

pub(crate) fn try_binary_boolean_native_geometry<'a, const D: usize, L, R, F>(
    lhs: &'a L,
    rhs: &'a R,
    op: F,
) -> Result<BooleanArray>
where
    L: NativeGeometryAccessor<'a, D>,
    R: NativeGeometryAccessor<'a, D>,
    F: Fn(crate::scalar::Geometry<'a, D>, crate::scalar::Geometry<'a, D>) -> Result<bool>,
{
    if lhs.len() != rhs.len() {
        return Err(GeoArrowError::General(
            "Cannot perform binary operation on arrays of different length".to_string(),
        ));
    }

    if lhs.is_empty() {
        return Ok(BooleanBuilder::new().finish());
    }
    let len = lhs.len();

    if lhs.null_count() == 0 && rhs.null_count() == 0 {
        let mut builder = BooleanBufferBuilder::new(len);
        for idx in 0..len {
            let (left, right) = unsafe {
                (
                    lhs.value_as_geometry_unchecked(idx),
                    rhs.value_as_geometry_unchecked(idx),
                )
            };
            builder.append(op(left, right)?);
        }
        Ok(BooleanArray::new(builder.finish(), None))
    } else {
        let nulls = NullBuffer::union(lhs.nulls(), rhs.nulls()).unwrap();

        let mut buffer = BooleanBufferBuilder::new(len);
        buffer.append_n(len, false);

        nulls.try_for_each_valid_idx(|idx| {
            let (left, right) = unsafe {
                (
                    lhs.value_as_geometry_unchecked(idx),
                    rhs.value_as_geometry_unchecked(idx),
                )
            };
            buffer.set_bit(idx, op(left, right)?);
            Ok::<_, GeoArrowError>(())
        })?;

        Ok(BooleanArray::new(buffer.finish(), Some(nulls)))
    }
}

pub(crate) fn try_binary_primitive_native_geometry<'a, const D: usize, L, R, F, O>(
    lhs: &'a L,
    rhs: &'a R,
    op: F,
) -> Result<PrimitiveArray<O>>
where
    L: NativeGeometryAccessor<'a, D>,
    R: NativeGeometryAccessor<'a, D>,
    O: ArrowPrimitiveType,
    F: Fn(crate::scalar::Geometry<'a, D>, crate::scalar::Geometry<'a, D>) -> Result<O::Native>,
{
    if lhs.len() != rhs.len() {
        return Err(GeoArrowError::General(
            "Cannot perform binary operation on arrays of different length".to_string(),
        ));
    }

    if lhs.is_empty() {
        return Ok(PrimitiveArray::from(ArrayData::new_empty(&O::DATA_TYPE)));
    }

    let len = lhs.len();

    if lhs.null_count() == 0 && rhs.null_count() == 0 {
        let mut buffer = MutableBuffer::new(len * O::Native::get_byte_width());
        for idx in 0..len {
            unsafe {
                buffer.push_unchecked(op(
                    lhs.value_as_geometry_unchecked(idx),
                    rhs.value_as_geometry_unchecked(idx),
                )?);
            };
        }
        Ok(PrimitiveArray::new(buffer.into(), None))
    } else {
        let nulls = NullBuffer::union(lhs.nulls(), rhs.nulls()).unwrap();

        let mut buffer = BufferBuilder::<O::Native>::new(len);
        buffer.append_n_zeroed(len);
        let slice = buffer.as_slice_mut();

        nulls.try_for_each_valid_idx(|idx| {
            unsafe {
                *slice.get_unchecked_mut(idx) = op(
                    lhs.value_as_geometry_unchecked(idx),
                    rhs.value_as_geometry_unchecked(idx),
                )?
            };
            Ok::<_, GeoArrowError>(())
        })?;

        let values = buffer.finish().into();
        Ok(PrimitiveArray::new(values, Some(nulls)))
    }
}
