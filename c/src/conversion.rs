//! Conversions between the serialized (`geoarrow.wkb`, `geoarrow.wkt`) and
//! native geoarrow encodings.

use std::sync::Arc;

use arrow_schema::Field;
use arrow_schema::ffi::FFI_ArrowSchema;
use geoarrow_array::GeoArrowArray;
use geoarrow_array::cast::{AsGeoArrowArray, from_wkb, from_wkt, to_wkb, to_wkt};
use geoarrow_schema::{GeoArrowType, GeometryType};

use crate::error::{Error, GeoArrowError, GeoArrowErrorCode};
use crate::marshal::{Slot, unary_geo};
use crate::{ArrowArray, ArrowSchema};

/// Resolve the requested output type, defaulting to `geoarrow.geometry` with
/// the input's metadata when `target_schema` is null. Matches the Python
/// binding's `to_type=None`.
///
/// # Safety
/// `target_schema` must be null or address a readable [`ArrowSchema`].
unsafe fn target_or_geometry(
    target_schema: *const ArrowSchema,
    input: &dyn GeoArrowArray,
) -> Result<GeoArrowType, Error> {
    if target_schema.is_null() {
        let metadata = input.data_type().metadata().clone();
        return Ok(GeoArrowType::Geometry(GeometryType::new(metadata)));
    }
    let schema = unsafe { &*(target_schema as *const FFI_ArrowSchema) };
    Ok(GeoArrowType::try_from(&Field::try_from(schema)?)?)
}

/// Parse a `geoarrow.wkb` array into the native type described by
/// `target_schema`, or into `geoarrow.geometry` when it is null.
///
/// # Safety
/// The input pair is consumed, `target_schema` is borrowed, and the output pair
/// is written on success.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn GeoArrowRsFromWKB(
    in_array: *mut ArrowArray,
    in_schema: *mut ArrowSchema,
    target_schema: *const ArrowSchema,
    out_array: *mut ArrowArray,
    out_schema: *mut ArrowSchema,
    error: *mut GeoArrowError,
) -> GeoArrowErrorCode {
    unsafe {
        unary_geo(
            Slot::new(in_array, in_schema),
            Slot::new(out_array, out_schema),
            error,
            |input| {
                let to_type = target_or_geometry(target_schema, input)?;
                Ok(match input.data_type() {
                    GeoArrowType::Wkb(_) => from_wkb(input.as_wkb::<i32>(), to_type),
                    GeoArrowType::LargeWkb(_) => from_wkb(input.as_wkb::<i64>(), to_type),
                    GeoArrowType::WkbView(_) => from_wkb(input.as_wkb_view(), to_type),
                    other => {
                        return Err(Error::invalid(format!("expected WKB input, got {other:?}")));
                    }
                }?)
            },
        )
    }
}

/// Encode a native geoarrow array as `geoarrow.wkb` (i32-offset `Binary`).
///
/// # Safety
/// The input pair is consumed and the output pair is written on success.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn GeoArrowRsToWKB(
    in_array: *mut ArrowArray,
    in_schema: *mut ArrowSchema,
    out_array: *mut ArrowArray,
    out_schema: *mut ArrowSchema,
    error: *mut GeoArrowError,
) -> GeoArrowErrorCode {
    unsafe {
        unary_geo(
            Slot::new(in_array, in_schema),
            Slot::new(out_array, out_schema),
            error,
            |input| Ok(Arc::new(to_wkb::<i32>(input)?)),
        )
    }
}

/// Parse a `geoarrow.wkt` array into the native type described by
/// `target_schema`, or into `geoarrow.geometry` when it is null.
///
/// # Safety
/// The input pair is consumed, `target_schema` is borrowed, and the output pair
/// is written on success.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn GeoArrowRsFromWKT(
    in_array: *mut ArrowArray,
    in_schema: *mut ArrowSchema,
    target_schema: *const ArrowSchema,
    out_array: *mut ArrowArray,
    out_schema: *mut ArrowSchema,
    error: *mut GeoArrowError,
) -> GeoArrowErrorCode {
    unsafe {
        unary_geo(
            Slot::new(in_array, in_schema),
            Slot::new(out_array, out_schema),
            error,
            |input| {
                let to_type = target_or_geometry(target_schema, input)?;
                Ok(match input.data_type() {
                    GeoArrowType::Wkt(_) => from_wkt(input.as_wkt::<i32>(), to_type),
                    GeoArrowType::LargeWkt(_) => from_wkt(input.as_wkt::<i64>(), to_type),
                    GeoArrowType::WktView(_) => from_wkt(input.as_wkt_view(), to_type),
                    other => {
                        return Err(Error::invalid(format!("expected WKT input, got {other:?}")));
                    }
                }?)
            },
        )
    }
}

/// Encode a native geoarrow array as `geoarrow.wkt` (i32-offset `Utf8`).
///
/// # Safety
/// The input pair is consumed and the output pair is written on success.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn GeoArrowRsToWKT(
    in_array: *mut ArrowArray,
    in_schema: *mut ArrowSchema,
    out_array: *mut ArrowArray,
    out_schema: *mut ArrowSchema,
    error: *mut GeoArrowError,
) -> GeoArrowErrorCode {
    unsafe {
        unary_geo(
            Slot::new(in_array, in_schema),
            Slot::new(out_array, out_schema),
            error,
            |input| Ok(Arc::new(to_wkt::<i32>(input)?)),
        )
    }
}
