use arrow_array::types::ArrowPrimitiveType;
use arrow_array::{BooleanArray, OffsetSizeTrait, PrimitiveArray};
use arrow_buffer::{BooleanBufferBuilder, BufferBuilder};

use crate::array::*;
use crate::geo_traits::*;
use crate::trait_::ArrayAccessor;

pub trait Unary<'a>: ArrayAccessor<'a> {
    // Note: This is derived from arrow-rs here:
    // https://github.com/apache/arrow-rs/blob/3ed7cc61d4157263ef2ab5c2d12bc7890a5315b3/arrow-array/src/array/primitive_array.rs#L753-L767
    fn unary_primitive<F, O>(&'a self, op: F) -> PrimitiveArray<O>
    where
        O: ArrowPrimitiveType,
        F: Fn(Self::Item) -> O::Native,
    {
        let nulls = self.nulls().cloned();
        let mut builder = BufferBuilder::<O::Native>::new(self.len());
        self.iter_values().for_each(|geom| builder.append(op(geom)));
        let buffer = builder.finish();
        PrimitiveArray::new(buffer.into(), nulls)
    }

    // Note: This is derived from arrow-rs here:
    // https://github.com/apache/arrow-rs/blob/3ed7cc61d4157263ef2ab5c2d12bc7890a5315b3/arrow-array/src/array/primitive_array.rs#L806-L830
    fn try_unary_primitive<F, O, E>(&'a self, op: F) -> std::result::Result<PrimitiveArray<O>, E>
    where
        O: ArrowPrimitiveType,
        F: Fn(Self::Item) -> std::result::Result<O::Native, E>,
    {
        let len = self.len();

        let nulls = self.nulls().cloned();
        let mut buffer = BufferBuilder::<O::Native>::new(len);
        buffer.append_n_zeroed(len);
        let slice = buffer.as_slice_mut();

        let f = |idx| {
            unsafe { *slice.get_unchecked_mut(idx) = op(self.value_unchecked(idx))? };
            Ok::<_, E>(())
        };

        match &nulls {
            Some(nulls) => nulls.try_for_each_valid_idx(f)?,
            None => (0..len).try_for_each(f)?,
        }

        let values = buffer.finish().into();
        Ok(PrimitiveArray::new(values, nulls))
    }

    fn unary_boolean<F>(&'a self, op: F) -> BooleanArray
    where
        F: Fn(Self::Item) -> bool,
    {
        let nulls = self.nulls().cloned();
        let mut builder = BooleanBufferBuilder::new(self.len());
        self.iter_values().for_each(|geom| builder.append(op(geom)));
        BooleanArray::new(builder.finish(), nulls)
    }

    /// Use this when the operation is relatively expensive and/or unlikely to auto-vectorize, and
    /// it's better to check the null bit to avoid the computation.
    fn try_unary_boolean<F, E>(&'a self, op: F) -> std::result::Result<BooleanArray, E>
    where
        F: Fn(Self::Item) -> std::result::Result<bool, E>,
    {
        let len = self.len();

        let nulls = self.nulls().cloned();
        let mut buffer = BooleanBufferBuilder::new(len);
        buffer.append_n(len, false);

        let f = |idx| {
            let value = unsafe { self.value_unchecked(idx) };
            buffer.set_bit(idx, op(value)?);
            Ok::<_, E>(())
        };

        match &nulls {
            Some(nulls) => nulls.try_for_each_valid_idx(f)?,
            None => (0..len).try_for_each(f)?,
        }

        Ok(BooleanArray::new(buffer.finish(), nulls))
    }
}

impl<'a, const D: usize> Unary<'a> for PointArray<D> {}
impl<'a, const D: usize> Unary<'a> for LineStringArray<D> {}
impl<'a, const D: usize> Unary<'a> for PolygonArray<D> {}
impl<'a, const D: usize> Unary<'a> for MultiPointArray<D> {}
impl<'a, const D: usize> Unary<'a> for MultiLineStringArray<D> {}
impl<'a, const D: usize> Unary<'a> for MultiPolygonArray<D> {}
impl<'a, const D: usize> Unary<'a> for MixedGeometryArray<D> {}
impl<'a, const D: usize> Unary<'a> for GeometryCollectionArray<D> {}
impl<'a, const D: usize> Unary<'a> for RectArray<D> {}
impl<'a, O: OffsetSizeTrait> Unary<'a> for WKBArray<O> {}

#[allow(dead_code)]
pub trait UnaryPoint<'a>: ArrayAccessor<'a> + NativeArray {
    fn unary_point<F, G>(&'a self, op: F) -> PointArray<2>
    where
        G: PointTrait<T = f64> + 'a,
        F: Fn(Self::Item) -> &'a G,
    {
        let nulls = self.nulls().cloned();
        let result_geom_iter = self.iter_values().map(op);
        let builder =
            PointBuilder::from_points(result_geom_iter, Some(self.coord_type()), self.metadata());
        let mut result = builder.finish();
        result.validity = nulls;
        result
    }

    fn try_unary_point<F, G, E>(&'a self, op: F) -> std::result::Result<PointArray<2>, E>
    where
        G: PointTrait<T = f64> + 'a,
        F: Fn(Self::Item) -> std::result::Result<G, E>,
    {
        let mut builder =
            PointBuilder::with_capacity_and_options(self.len(), self.coord_type(), self.metadata());

        for maybe_geom in self.iter() {
            if let Some(geom) = maybe_geom {
                builder.push_point(Some(&op(geom)?));
            } else {
                builder.push_null()
            }
        }

        Ok(builder.finish())
    }
}

impl<'a> UnaryPoint<'a> for PointArray<2> {}
impl<'a> UnaryPoint<'a> for LineStringArray<2> {}
impl<'a> UnaryPoint<'a> for PolygonArray<2> {}
impl<'a> UnaryPoint<'a> for MultiPointArray<2> {}
impl<'a> UnaryPoint<'a> for MultiLineStringArray<2> {}
impl<'a> UnaryPoint<'a> for MultiPolygonArray<2> {}
impl<'a> UnaryPoint<'a> for MixedGeometryArray<2> {}
impl<'a> UnaryPoint<'a> for GeometryCollectionArray<2> {}
impl<'a> UnaryPoint<'a> for RectArray<2> {}
