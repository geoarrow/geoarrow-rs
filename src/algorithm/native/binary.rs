use arrow_array::builder::BooleanBuilder;
use arrow_array::{BooleanArray, OffsetSizeTrait};
use arrow_buffer::{BooleanBufferBuilder, NullBuffer};

use crate::array::*;
use crate::error::{GeoArrowError, Result};
use crate::trait_::GeometryArrayAccessor;

pub trait Binary<'a, Rhs: GeometryArrayAccessor<'a> = Self>: GeometryArrayAccessor<'a> {
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

        let nulls = NullBuffer::union(self.logical_nulls().as_ref(), rhs.logical_nulls().as_ref());
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
            let nulls =
                NullBuffer::union(self.logical_nulls().as_ref(), rhs.logical_nulls().as_ref())
                    .unwrap();

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
}

// Implementations on PointArray
impl<'a> Binary<'a, PointArray> for PointArray {}
impl<'a> Binary<'a, PointArray> for RectArray {}
impl<'a, O: OffsetSizeTrait> Binary<'a, PointArray> for LineStringArray<O> {}
impl<'a, O: OffsetSizeTrait> Binary<'a, PointArray> for PolygonArray<O> {}
impl<'a, O: OffsetSizeTrait> Binary<'a, PointArray> for MultiPointArray<O> {}
impl<'a, O: OffsetSizeTrait> Binary<'a, PointArray> for MultiLineStringArray<O> {}
impl<'a, O: OffsetSizeTrait> Binary<'a, PointArray> for MultiPolygonArray<O> {}
impl<'a, O: OffsetSizeTrait> Binary<'a, PointArray> for MixedGeometryArray<O> {}
impl<'a, O: OffsetSizeTrait> Binary<'a, PointArray> for GeometryCollectionArray<O> {}
impl<'a, O: OffsetSizeTrait> Binary<'a, PointArray> for WKBArray<O> {}

// Implementations on LineStringArray
impl<'a, O: OffsetSizeTrait> Binary<'a, LineStringArray<O>> for PointArray {}
impl<'a, O: OffsetSizeTrait> Binary<'a, LineStringArray<O>> for RectArray {}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, LineStringArray<O1>>
    for LineStringArray<O2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, LineStringArray<O1>>
    for PolygonArray<O2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, LineStringArray<O1>>
    for MultiPointArray<O2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, LineStringArray<O1>>
    for MultiLineStringArray<O2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, LineStringArray<O1>>
    for MultiPolygonArray<O2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, LineStringArray<O1>>
    for MixedGeometryArray<O2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, LineStringArray<O1>>
    for GeometryCollectionArray<O2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, LineStringArray<O1>>
    for WKBArray<O2>
{
}

// Implementations on PolygonArray
impl<'a, O: OffsetSizeTrait> Binary<'a, PolygonArray<O>> for PointArray {}
impl<'a, O: OffsetSizeTrait> Binary<'a, PolygonArray<O>> for RectArray {}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, PolygonArray<O1>>
    for LineStringArray<O2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, PolygonArray<O1>>
    for PolygonArray<O2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, PolygonArray<O1>>
    for MultiPointArray<O2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, PolygonArray<O1>>
    for MultiLineStringArray<O2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, PolygonArray<O1>>
    for MultiPolygonArray<O2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, PolygonArray<O1>>
    for MixedGeometryArray<O2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, PolygonArray<O1>>
    for GeometryCollectionArray<O2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, PolygonArray<O1>> for WKBArray<O2> {}

// Implementations on MultiPointArray
impl<'a, O: OffsetSizeTrait> Binary<'a, MultiPointArray<O>> for PointArray {}
impl<'a, O: OffsetSizeTrait> Binary<'a, MultiPointArray<O>> for RectArray {}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, MultiPointArray<O1>>
    for LineStringArray<O2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, MultiPointArray<O1>>
    for PolygonArray<O2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, MultiPointArray<O1>>
    for MultiPointArray<O2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, MultiPointArray<O1>>
    for MultiLineStringArray<O2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, MultiPointArray<O1>>
    for MultiPolygonArray<O2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, MultiPointArray<O1>>
    for MixedGeometryArray<O2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, MultiPointArray<O1>>
    for GeometryCollectionArray<O2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, MultiPointArray<O1>>
    for WKBArray<O2>
{
}

// Implementations on MultiLineStringArray
impl<'a, O: OffsetSizeTrait> Binary<'a, MultiLineStringArray<O>> for PointArray {}
impl<'a, O: OffsetSizeTrait> Binary<'a, MultiLineStringArray<O>> for RectArray {}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, MultiLineStringArray<O1>>
    for LineStringArray<O2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, MultiLineStringArray<O1>>
    for PolygonArray<O2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, MultiLineStringArray<O1>>
    for MultiPointArray<O2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, MultiLineStringArray<O1>>
    for MultiLineStringArray<O2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, MultiLineStringArray<O1>>
    for MultiPolygonArray<O2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, MultiLineStringArray<O1>>
    for MixedGeometryArray<O2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, MultiLineStringArray<O1>>
    for GeometryCollectionArray<O2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, MultiLineStringArray<O1>>
    for WKBArray<O2>
{
}

// Implementations on MultiPolygonArray
impl<'a, O: OffsetSizeTrait> Binary<'a, MultiPolygonArray<O>> for PointArray {}
impl<'a, O: OffsetSizeTrait> Binary<'a, MultiPolygonArray<O>> for RectArray {}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, MultiPolygonArray<O1>>
    for LineStringArray<O2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, MultiPolygonArray<O1>>
    for PolygonArray<O2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, MultiPolygonArray<O1>>
    for MultiPointArray<O2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, MultiPolygonArray<O1>>
    for MultiLineStringArray<O2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, MultiPolygonArray<O1>>
    for MultiPolygonArray<O2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, MultiPolygonArray<O1>>
    for MixedGeometryArray<O2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, MultiPolygonArray<O1>>
    for GeometryCollectionArray<O2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, MultiPolygonArray<O1>>
    for WKBArray<O2>
{
}

// Implementations on MixedGeometryArray
impl<'a, O: OffsetSizeTrait> Binary<'a, MixedGeometryArray<O>> for PointArray {}
impl<'a, O: OffsetSizeTrait> Binary<'a, MixedGeometryArray<O>> for RectArray {}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, MixedGeometryArray<O1>>
    for LineStringArray<O2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, MixedGeometryArray<O1>>
    for PolygonArray<O2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, MixedGeometryArray<O1>>
    for MultiPointArray<O2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, MixedGeometryArray<O1>>
    for MultiLineStringArray<O2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, MixedGeometryArray<O1>>
    for MultiPolygonArray<O2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, MixedGeometryArray<O1>>
    for MixedGeometryArray<O2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, MixedGeometryArray<O1>>
    for GeometryCollectionArray<O2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, MixedGeometryArray<O1>>
    for WKBArray<O2>
{
}

// Implementations on GeometryCollectionArray
impl<'a, O: OffsetSizeTrait> Binary<'a, GeometryCollectionArray<O>> for PointArray {}
impl<'a, O: OffsetSizeTrait> Binary<'a, GeometryCollectionArray<O>> for RectArray {}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, GeometryCollectionArray<O1>>
    for LineStringArray<O2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, GeometryCollectionArray<O1>>
    for PolygonArray<O2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, GeometryCollectionArray<O1>>
    for MultiPointArray<O2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, GeometryCollectionArray<O1>>
    for MultiLineStringArray<O2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, GeometryCollectionArray<O1>>
    for MultiPolygonArray<O2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, GeometryCollectionArray<O1>>
    for MixedGeometryArray<O2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, GeometryCollectionArray<O1>>
    for GeometryCollectionArray<O2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, GeometryCollectionArray<O1>>
    for WKBArray<O2>
{
}

// Implementations on WKBArray
impl<'a, O: OffsetSizeTrait> Binary<'a, WKBArray<O>> for PointArray {}
impl<'a, O: OffsetSizeTrait> Binary<'a, WKBArray<O>> for RectArray {}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, WKBArray<O1>>
    for LineStringArray<O2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, WKBArray<O1>> for PolygonArray<O2> {}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, WKBArray<O1>>
    for MultiPointArray<O2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, WKBArray<O1>>
    for MultiLineStringArray<O2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, WKBArray<O1>>
    for MultiPolygonArray<O2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, WKBArray<O1>>
    for MixedGeometryArray<O2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, WKBArray<O1>>
    for GeometryCollectionArray<O2>
{
}
impl<'a, O1: OffsetSizeTrait, O2: OffsetSizeTrait> Binary<'a, WKBArray<O1>> for WKBArray<O2> {}
