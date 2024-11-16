use arrow_array::{ArrowPrimitiveType, PrimitiveArray};
use arrow_buffer::BufferBuilder;

use crate::array::PolygonArray;
use crate::datatypes::Dimension;
use crate::error::GeoArrowError;
use crate::io::geos::scalar::GEOSPolygon;
use crate::trait_::NativeGEOSGeometryAccessor;

// Note: This is derived from arrow-rs here:
// https://github.com/apache/arrow-rs/blob/3ed7cc61d4157263ef2ab5c2d12bc7890a5315b3/arrow-array/src/array/primitive_array.rs#L806-L830
#[allow(dead_code)]
pub(super) fn try_unary_primitive<'a, F, O, E>(
    array: &'a dyn NativeGEOSGeometryAccessor<'a>,
    op: F,
) -> std::result::Result<PrimitiveArray<O>, E>
where
    O: ArrowPrimitiveType,
    F: Fn(geos::Geometry) -> std::result::Result<O::Native, E>,
    E: std::convert::From<geos::Error>,
{
    let len = array.len();

    let nulls = array.nulls().cloned();
    let mut buffer = BufferBuilder::<O::Native>::new(len);
    buffer.append_n_zeroed(len);
    let slice = buffer.as_slice_mut();

    let f = |idx| {
        unsafe { *slice.get_unchecked_mut(idx) = op(array.value_as_geometry_unchecked(idx)?)? };
        Ok::<_, E>(())
    };

    match &nulls {
        Some(nulls) => nulls.try_for_each_valid_idx(f)?,
        None => (0..len).try_for_each(f)?,
    }

    let values = buffer.finish().into();
    Ok(PrimitiveArray::new(values, nulls))
}

pub(super) fn try_unary_polygon<'a, F>(
    array: &'a dyn NativeGEOSGeometryAccessor<'a>,
    op: F,
    output_dim: Dimension,
) -> std::result::Result<PolygonArray, GeoArrowError>
where
    F: Fn(geos::Geometry) -> std::result::Result<geos::Geometry, geos::Error>,
{
    let len = array.len();

    let mut buffer = vec![None; len];

    // Note: this assumes the output geometry is a polygon
    let f = |idx| {
        unsafe {
            buffer[idx] = Some(GEOSPolygon::new_unchecked(op(
                array.value_as_geometry_unchecked(idx)?
            )?))
        };
        Ok::<_, geos::Error>(())
    };

    match array.nulls() {
        Some(nulls) => nulls.try_for_each_valid_idx(f)?,
        None => (0..len).try_for_each(f)?,
    }

    Ok(PolygonArray::from((buffer, output_dim)))
}
