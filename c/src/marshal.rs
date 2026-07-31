//! Moving Arrow arrays across the C ABI, and the operation shapes built on top.
//!
//! Inputs follow the Arrow C Data Interface import convention: the caller's
//! structs are moved out and zeroed, so the caller must not release them
//! afterwards. Outputs are written into caller-allocated slots that the caller
//! releases with [`crate::GeoArrowRsArrayRelease`] /
//! [`crate::GeoArrowRsSchemaRelease`].

use std::sync::Arc;

use arrow_array::ffi::{from_ffi, to_ffi};
use arrow_array::{Array, ArrayRef, BooleanArray, Float64Array, Int32Array, make_array};
use arrow_data::ArrayData;
use arrow_data::ffi::FFI_ArrowArray;
use arrow_schema::ffi::FFI_ArrowSchema;
use arrow_schema::{DataType, Field};
use geoarrow_array::GeoArrowArray;
use geoarrow_array::array::from_arrow_array;
use geoarrow_schema::CoordType;

use crate::error::{Error, GeoArrowError, GeoArrowErrorCode, catching, finish};
use crate::types::coord_type;
use crate::{ArrowArray, ArrowSchema};

/// Field name for outputs carrying a GeoArrow extension type.
const GEOMETRY: &str = "geometry";
/// Field name for plain (non-geospatial) outputs.
const RESULT: &str = "result";

type GeoArray = Arc<dyn GeoArrowArray>;

/// One Arrow C Data Interface value: an array and the schema describing it. The
/// two always travel together, so they move as a unit internally even though the
/// C signatures spell them out separately.
#[derive(Clone, Copy)]
pub(crate) struct Slot {
    array: *mut ArrowArray,
    schema: *mut ArrowSchema,
}

impl Slot {
    pub(crate) fn new(array: *mut ArrowArray, schema: *mut ArrowSchema) -> Self {
        Self { array, schema }
    }

    fn is_null(&self) -> bool {
        self.array.is_null() || self.schema.is_null()
    }
}

/// Move the plain Arrow value out of `slot`, zeroing the caller's structs.
///
/// # Safety
/// `slot` must address writable, at-least-zeroed FFI structs.
pub(crate) unsafe fn consume_arrow(slot: Slot) -> Result<(ArrayRef, Field), Error> {
    if slot.is_null() {
        return Err(Error::invalid("input array/schema pointer is null"));
    }
    let array = slot.array as *mut FFI_ArrowArray;
    let schema = slot.schema as *mut FFI_ArrowSchema;

    let ffi_array = unsafe { std::ptr::read(array) };
    let ffi_schema = unsafe { std::ptr::read(schema) };
    unsafe {
        std::ptr::write_bytes(array, 0, 1);
        std::ptr::write_bytes(schema, 0, 1);
    }

    let field = Field::try_from(&ffi_schema)?;
    let data = unsafe { from_ffi(ffi_array, &ffi_schema) }?;
    Ok((make_array(data), field))
}

/// Move the array out of `slot` as a GeoArrow array, zeroing the caller's
/// structs.
///
/// # Safety
/// `slot` must address writable, at-least-zeroed FFI structs.
unsafe fn consume(slot: Slot) -> Result<GeoArray, Error> {
    let (array, field) = unsafe { consume_arrow(slot) }?;
    Ok(from_arrow_array(array.as_ref(), &field)?)
}

/// # Safety
/// Both slots must address writable, at-least-zeroed FFI structs.
unsafe fn consume_pair(left: Slot, right: Slot) -> Result<(GeoArray, GeoArray), Error> {
    // Both are consumed even when the first fails, so a bad left input still
    // zeroes the right one and the caller's release stays a no-op on both.
    let left = unsafe { consume(left) };
    let right = unsafe { consume(right) };
    Ok((
        left.map_err(|e| Error {
            message: format!("left input: {}", e.message),
            ..e
        })?,
        right.map_err(|e| Error {
            message: format!("right input: {}", e.message),
            ..e
        })?,
    ))
}

/// Write a GeoArrow array into `out`. The schema is rebuilt from the array's
/// `GeoArrowType` because `to_ffi` alone drops the extension metadata.
///
/// # Safety
/// `out` must address writable, zeroed FFI structs.
unsafe fn write_geoarrow(array: GeoArray, out: Slot) -> Result<(), Error> {
    let field = array.data_type().to_field(GEOMETRY, true);
    let data = array.to_array_ref().to_data();
    unsafe { write(data, field, out) }
}

/// # Safety
/// `out` must address writable, zeroed FFI structs.
pub(crate) unsafe fn write(data: ArrayData, field: Field, out: Slot) -> Result<(), Error> {
    if out.is_null() {
        return Err(Error::invalid("output array/schema pointer is null"));
    }
    let (ffi_array, _schema_without_extension) = to_ffi(&data)?;
    let ffi_schema = FFI_ArrowSchema::try_from(&field)?;
    unsafe {
        std::ptr::write(out.array as *mut FFI_ArrowArray, ffi_array);
        std::ptr::write(out.schema as *mut FFI_ArrowSchema, ffi_schema);
    }
    Ok(())
}

/// Run `op` over one consumed input and write its GeoArrow result.
///
/// # Safety
/// Both slots must address writable FFI structs.
pub(crate) unsafe fn unary_geo<F>(
    input: Slot,
    out: Slot,
    error: *mut GeoArrowError,
    op: F,
) -> GeoArrowErrorCode
where
    F: FnOnce(&dyn GeoArrowArray) -> Result<GeoArray, Error>,
{
    let result = catching(|| {
        let array = unsafe { consume(input) }?;
        let output = op(array.as_ref())?;
        unsafe { write_geoarrow(output, out) }
    });
    finish(result, error)
}

/// Run `op` over two consumed inputs and write its GeoArrow result.
///
/// # Safety
/// All three slots must address writable FFI structs.
pub(crate) unsafe fn binary_geo<F>(
    left: Slot,
    right: Slot,
    out: Slot,
    error: *mut GeoArrowError,
    op: F,
) -> GeoArrowErrorCode
where
    F: FnOnce(&dyn GeoArrowArray, &dyn GeoArrowArray) -> Result<GeoArray, Error>,
{
    let result = catching(|| {
        let (left, right) = unsafe { consume_pair(left, right) }?;
        let output = op(left.as_ref(), right.as_ref())?;
        unsafe { write_geoarrow(output, out) }
    });
    finish(result, error)
}

/// A plain Arrow array an operation can return, tagged with its type so the
/// output field can be built without a second dispatch.
pub(crate) enum Plain {
    Float64(Float64Array),
    Int32(Int32Array),
    Boolean(BooleanArray),
}

impl Plain {
    fn into_parts(self) -> (ArrayData, DataType) {
        match self {
            Self::Float64(a) => (a.into_data(), DataType::Float64),
            Self::Int32(a) => (a.into_data(), DataType::Int32),
            Self::Boolean(a) => (a.into_data(), DataType::Boolean),
        }
    }
}

impl From<Float64Array> for Plain {
    fn from(a: Float64Array) -> Self {
        Self::Float64(a)
    }
}

impl From<Int32Array> for Plain {
    fn from(a: Int32Array) -> Self {
        Self::Int32(a)
    }
}

impl From<BooleanArray> for Plain {
    fn from(a: BooleanArray) -> Self {
        Self::Boolean(a)
    }
}

/// Run `op` over one consumed input and write its plain Arrow result.
///
/// # Safety
/// Both slots must address writable FFI structs.
pub(crate) unsafe fn unary_plain<F, T>(
    input: Slot,
    out: Slot,
    error: *mut GeoArrowError,
    op: F,
) -> GeoArrowErrorCode
where
    F: FnOnce(&dyn GeoArrowArray) -> Result<T, Error>,
    T: Into<Plain>,
{
    let result = catching(|| {
        let array = unsafe { consume(input) }?;
        let (data, data_type) = op(array.as_ref())?.into().into_parts();
        unsafe { write(data, Field::new(RESULT, data_type, true), out) }
    });
    finish(result, error)
}

/// Run `op` over two consumed inputs and write its plain Arrow result.
///
/// # Safety
/// All three slots must address writable FFI structs.
pub(crate) unsafe fn binary_plain<F, T>(
    left: Slot,
    right: Slot,
    out: Slot,
    error: *mut GeoArrowError,
    op: F,
) -> GeoArrowErrorCode
where
    F: FnOnce(&dyn GeoArrowArray, &dyn GeoArrowArray) -> Result<T, Error>,
    T: Into<Plain>,
{
    let result = catching(|| {
        let (left, right) = unsafe { consume_pair(left, right) }?;
        let (data, data_type) = op(left.as_ref(), right.as_ref())?.into().into_parts();
        unsafe { write(data, Field::new(RESULT, data_type, true), out) }
    });
    finish(result, error)
}

/// [`unary_geo`] with the C coordinate-type code decoded once, so entry points
/// do not repeat the decode inside their closures.
///
/// # Safety
/// Both slots must address writable FFI structs.
pub(crate) unsafe fn unary_geo_coords<F>(
    input: Slot,
    coords: i32,
    out: Slot,
    error: *mut GeoArrowError,
    op: F,
) -> GeoArrowErrorCode
where
    F: FnOnce(&dyn GeoArrowArray, CoordType) -> Result<GeoArray, Error>,
{
    unsafe { unary_geo(input, out, error, |input| op(input, coord_type(coords)?)) }
}

/// [`binary_geo`] with the C coordinate-type code decoded once.
///
/// # Safety
/// All three slots must address writable FFI structs.
pub(crate) unsafe fn binary_geo_coords<F>(
    left: Slot,
    right: Slot,
    coords: i32,
    out: Slot,
    error: *mut GeoArrowError,
    op: F,
) -> GeoArrowErrorCode
where
    F: FnOnce(&dyn GeoArrowArray, &dyn GeoArrowArray, CoordType) -> Result<GeoArray, Error>,
{
    unsafe {
        binary_geo(left, right, out, error, |l, r| {
            op(l, r, coord_type(coords)?)
        })
    }
}
