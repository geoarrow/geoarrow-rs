use arrow::datatypes::ArrowPrimitiveType;
use arrow_array::builder::BooleanBuilder;
use arrow_array::{BooleanArray, PrimitiveArray};
use arrow_buffer::ArrowNativeType;
use arrow_buffer::{BooleanBufferBuilder, BufferBuilder, MutableBuffer, NullBuffer};
use arrow_data::ArrayData;
use geo_traits::GeometryTrait;

use crate::array::*;
use crate::error::{GeoArrowError, Result};
use crate::trait_::ArrayAccessor;

pub trait Binary<'a, Rhs: ArrayAccessor<'a> = Self>: ArrayAccessor<'a> + NativeArray {
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

    fn try_binary_geometry<F, G>(
        &'a self,
        rhs: &'a Rhs,
        op: F,
        prefer_multi: bool,
    ) -> Result<GeometryArray>
    where
        G: GeometryTrait<T = f64>,
        F: Fn(Self::Item, Rhs::Item) -> Result<G>,
    {
        if self.len() != rhs.len() {
            return Err(GeoArrowError::General(
                "Cannot perform binary operation on arrays of different length".to_string(),
            ));
        }

        let mut builder = GeometryBuilder::with_capacity_and_options(
            Default::default(),
            self.coord_type(),
            self.metadata().clone(),
            prefer_multi,
        );

        if self.is_empty() {
            return Ok(builder.finish());
        }

        for (left, right) in self.iter().zip(rhs.iter()) {
            if let (Some(left), Some(right)) = (left, right) {
                builder.push_geometry(Some(&op(left, right)?))?;
            } else {
                builder.push_null();
            }
        }
        Ok(builder.finish())
    }
}

// Implementations on PointArray
impl Binary<'_, PointArray> for PointArray {}
impl Binary<'_, PointArray> for RectArray {}
impl Binary<'_, PointArray> for LineStringArray {}
impl Binary<'_, PointArray> for PolygonArray {}
impl Binary<'_, PointArray> for MultiPointArray {}
impl Binary<'_, PointArray> for MultiLineStringArray {}
impl Binary<'_, PointArray> for MultiPolygonArray {}
impl Binary<'_, PointArray> for MixedGeometryArray {}
impl Binary<'_, PointArray> for GeometryCollectionArray {}

// Implementations on LineStringArray
impl Binary<'_, LineStringArray> for PointArray {}
impl Binary<'_, LineStringArray> for RectArray {}
impl Binary<'_, LineStringArray> for LineStringArray {}
impl Binary<'_, LineStringArray> for PolygonArray {}
impl Binary<'_, LineStringArray> for MultiPointArray {}
impl Binary<'_, LineStringArray> for MultiLineStringArray {}
impl Binary<'_, LineStringArray> for MultiPolygonArray {}
impl Binary<'_, LineStringArray> for MixedGeometryArray {}
impl Binary<'_, LineStringArray> for GeometryCollectionArray {}

// Implementations on PolygonArray
impl Binary<'_, PolygonArray> for PointArray {}
impl Binary<'_, PolygonArray> for RectArray {}
impl Binary<'_, PolygonArray> for LineStringArray {}
impl Binary<'_, PolygonArray> for PolygonArray {}
impl Binary<'_, PolygonArray> for MultiPointArray {}
impl Binary<'_, PolygonArray> for MultiLineStringArray {}
impl Binary<'_, PolygonArray> for MultiPolygonArray {}
impl Binary<'_, PolygonArray> for MixedGeometryArray {}
impl Binary<'_, PolygonArray> for GeometryCollectionArray {}

// Implementations on MultiPointArray
impl Binary<'_, MultiPointArray> for PointArray {}
impl Binary<'_, MultiPointArray> for RectArray {}
impl Binary<'_, MultiPointArray> for LineStringArray {}
impl Binary<'_, MultiPointArray> for PolygonArray {}
impl Binary<'_, MultiPointArray> for MultiPointArray {}
impl Binary<'_, MultiPointArray> for MultiLineStringArray {}
impl Binary<'_, MultiPointArray> for MultiPolygonArray {}
impl Binary<'_, MultiPointArray> for MixedGeometryArray {}
impl Binary<'_, MultiPointArray> for GeometryCollectionArray {}

// Implementations on MultiLineStringArray
impl Binary<'_, MultiLineStringArray> for PointArray {}
impl Binary<'_, MultiLineStringArray> for RectArray {}
impl Binary<'_, MultiLineStringArray> for LineStringArray {}
impl Binary<'_, MultiLineStringArray> for PolygonArray {}
impl Binary<'_, MultiLineStringArray> for MultiPointArray {}
impl Binary<'_, MultiLineStringArray> for MultiLineStringArray {}
impl Binary<'_, MultiLineStringArray> for MultiPolygonArray {}
impl Binary<'_, MultiLineStringArray> for MixedGeometryArray {}
impl Binary<'_, MultiLineStringArray> for GeometryCollectionArray {}

// Implementations on MultiPolygonArray
impl Binary<'_, MultiPolygonArray> for PointArray {}
impl Binary<'_, MultiPolygonArray> for RectArray {}
impl Binary<'_, MultiPolygonArray> for LineStringArray {}
impl Binary<'_, MultiPolygonArray> for PolygonArray {}
impl Binary<'_, MultiPolygonArray> for MultiPointArray {}
impl Binary<'_, MultiPolygonArray> for MultiLineStringArray {}
impl Binary<'_, MultiPolygonArray> for MultiPolygonArray {}
impl Binary<'_, MultiPolygonArray> for MixedGeometryArray {}
impl Binary<'_, MultiPolygonArray> for GeometryCollectionArray {}

// Implementations on MixedGeometryArray
impl Binary<'_, MixedGeometryArray> for PointArray {}
impl Binary<'_, MixedGeometryArray> for RectArray {}
impl Binary<'_, MixedGeometryArray> for LineStringArray {}
impl Binary<'_, MixedGeometryArray> for PolygonArray {}
impl Binary<'_, MixedGeometryArray> for MultiPointArray {}
impl Binary<'_, MixedGeometryArray> for MultiLineStringArray {}
impl Binary<'_, MixedGeometryArray> for MultiPolygonArray {}
impl Binary<'_, MixedGeometryArray> for MixedGeometryArray {}
impl Binary<'_, MixedGeometryArray> for GeometryCollectionArray {}

// Implementations on GeometryCollectionArray
impl Binary<'_, GeometryCollectionArray> for PointArray {}
impl Binary<'_, GeometryCollectionArray> for RectArray {}
impl Binary<'_, GeometryCollectionArray> for LineStringArray {}
impl Binary<'_, GeometryCollectionArray> for PolygonArray {}
impl Binary<'_, GeometryCollectionArray> for MultiPointArray {}
impl Binary<'_, GeometryCollectionArray> for MultiLineStringArray {}
impl Binary<'_, GeometryCollectionArray> for MultiPolygonArray {}
impl Binary<'_, GeometryCollectionArray> for MixedGeometryArray {}
impl Binary<'_, GeometryCollectionArray> for GeometryCollectionArray {}
