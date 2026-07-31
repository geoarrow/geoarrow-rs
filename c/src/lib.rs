//! A C ABI over geoarrow-rs, for callers that cannot link Rust directly.
//!
//! Arrays cross the boundary through the [Arrow C Data Interface], so no
//! geospatial data is copied or reserialized. The type vocabulary
//! ([`GeoArrowError`], [`types::GeoArrowType`], and the coordinate, dimension
//! and edge enums) is shared with [geoarrow-c] so a caller can use one set of
//! constants with both libraries; the functions carry a `GeoArrowRs` prefix so
//! the two can be linked together.
//!
//! [Arrow C Data Interface]: https://arrow.apache.org/docs/format/CDataInterface.html
//! [geoarrow-c]: https://geoarrow.org/geoarrow-c

mod compute;
mod conversion;
mod error;
mod geoparquet;
mod marshal;
mod schema;
pub mod types;

use std::ffi::c_void;
use std::os::raw::c_char;

use arrow_data::ffi::FFI_ArrowArray;
use arrow_schema::ffi::FFI_ArrowSchema;
pub use error::{GEOARROW_OK, GeoArrowError, GeoArrowErrorCode};
pub use types::{GeoArrowCoordType, GeoArrowDimensions, GeoArrowEdgeType, GeoArrowType};

/// Arrow C Data Interface `ArrowSchema`.
///
/// Redeclared here, rather than reused from arrow-rs, so the C signatures can
/// name it without cbindgen walking into the arrow-rs workspace. The header
/// carries the canonical guarded definition from cbindgen.toml instead of this
/// one; `layouts_match_arrow_rs` pins the layout to arrow-rs's.
#[repr(C)]
pub struct ArrowSchema {
    pub format: *const c_char,
    pub name: *const c_char,
    pub metadata: *const c_char,
    pub flags: i64,
    pub n_children: i64,
    pub children: *mut *mut ArrowSchema,
    pub dictionary: *mut ArrowSchema,
    pub release: Option<unsafe extern "C" fn(*mut ArrowSchema)>,
    pub private_data: *mut c_void,
}

/// Arrow C Data Interface `ArrowArray`. See [`ArrowSchema`].
#[repr(C)]
pub struct ArrowArray {
    pub length: i64,
    pub null_count: i64,
    pub offset: i64,
    pub n_buffers: i64,
    pub n_children: i64,
    pub buffers: *mut *const c_void,
    pub children: *mut *mut ArrowArray,
    pub dictionary: *mut ArrowArray,
    pub release: Option<unsafe extern "C" fn(*mut ArrowArray)>,
    pub private_data: *mut c_void,
}

/// The library version, as a static NUL-terminated string.
#[unsafe(no_mangle)]
pub extern "C" fn GeoArrowRsVersion() -> *const c_char {
    concat!(env!("CARGO_PKG_VERSION"), "\0").as_ptr() as *const c_char
}

/// Release an array written by this library. Null is ignored.
///
/// # Safety
/// `array` must be null or address an array this library produced and that has
/// not already been released.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn GeoArrowRsArrayRelease(array: *mut ArrowArray) {
    if !array.is_null() {
        // Dropping FFI_ArrowArray invokes the embedded release callback; the
        // two structs are layout-identical.
        unsafe { std::ptr::drop_in_place(array as *mut FFI_ArrowArray) };
    }
}

/// Release a schema written by this library. Null is ignored.
///
/// # Safety
/// `schema` must be null or address a schema this library produced and that has
/// not already been released.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn GeoArrowRsSchemaRelease(schema: *mut ArrowSchema) {
    if !schema.is_null() {
        unsafe { std::ptr::drop_in_place(schema as *mut FFI_ArrowSchema) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_is_readable_as_a_c_string() {
        let version = unsafe { std::ffi::CStr::from_ptr(GeoArrowRsVersion()) };
        assert_eq!(version.to_str().unwrap(), env!("CARGO_PKG_VERSION"));
    }

    /// The redeclared ABI structs must stay byte-compatible with arrow-rs's,
    /// since every entry point casts between them.
    #[test]
    fn layouts_match_arrow_rs() {
        use std::mem::{align_of, size_of};
        assert_eq!(size_of::<ArrowArray>(), size_of::<FFI_ArrowArray>());
        assert_eq!(align_of::<ArrowArray>(), align_of::<FFI_ArrowArray>());
        assert_eq!(size_of::<ArrowSchema>(), size_of::<FFI_ArrowSchema>());
        assert_eq!(align_of::<ArrowSchema>(), align_of::<FFI_ArrowSchema>());
    }

    /// Z and M ordinates must survive a round trip through the C ABI. The
    /// kernels have their own XYZM coverage upstream, but those tests never
    /// cross the boundary, which is where the extension metadata can be lost.
    #[test]
    fn convex_hull_preserves_z_and_m_across_the_boundary() {
        use arrow_array::ffi::{from_ffi, to_ffi};
        use arrow_array::make_array;
        use arrow_schema::Field;
        use geo_traits::{CoordTrait, LineStringTrait, PolygonTrait};
        use geoarrow_array::array::from_arrow_array;
        use geoarrow_array::cast::AsGeoArrowArray;
        use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, test};
        use geoarrow_schema::{CoordType, Dimension};

        let input = test::polygon::array(CoordType::Separated, Dimension::XYZM);

        // Export the way a caller does: to_ffi for the array, and a schema
        // derived from the GeoArrowType so the extension metadata survives.
        let field: Field = input.data_type().to_field("input", true);
        let data = input.to_array_ref().to_data();
        let (mut in_array, _stripped) = to_ffi(&data).unwrap();
        let mut in_schema = FFI_ArrowSchema::try_from(&field).unwrap();
        // `empty()` holds a null release callback, so the write the call makes
        // into these slots overwrites nothing that needed dropping, and the
        // result is owned by the local afterwards.
        let mut out_array = FFI_ArrowArray::empty();
        let mut out_schema = FFI_ArrowSchema::empty();

        let rc = unsafe {
            compute::GeoArrowRsConvexHull(
                &mut in_array as *mut FFI_ArrowArray as *mut ArrowArray,
                &mut in_schema as *mut FFI_ArrowSchema as *mut ArrowSchema,
                types::GeoArrowCoordType::GEOARROW_COORD_TYPE_INTERLEAVED as i32,
                &mut out_array as *mut FFI_ArrowArray as *mut ArrowArray,
                &mut out_schema as *mut FFI_ArrowSchema as *mut ArrowSchema,
                std::ptr::null_mut(),
            )
        };
        assert_eq!(rc, GEOARROW_OK);

        let out_field = Field::try_from(&out_schema).unwrap();
        let data = unsafe { from_ffi(out_array, &out_schema) }.unwrap();
        let output = from_arrow_array(make_array(data).as_ref(), &out_field).unwrap();
        assert_eq!(output.data_type().dimension(), Some(Dimension::XYZM));

        let output = output.as_polygon();
        assert_eq!(output.len(), input.len());

        // Compared as bit patterns so the four ordinates form an `Eq` key; the
        // hull reuses input vertices verbatim, so exact equality is the point.
        fn xyzm(c: impl CoordTrait<T = f64>) -> [u64; 4] {
            [c.x(), c.y(), c.nth_or_panic(2), c.nth_or_panic(3)].map(f64::to_bits)
        }

        let mut checked = 0;
        for (source, hull) in input.iter().zip(output.iter()) {
            let (Some(Ok(source)), Some(Ok(hull))) = (source, hull) else {
                continue;
            };
            let (Some(source), Some(hull)) = (source.exterior(), hull.exterior()) else {
                continue;
            };
            let source: Vec<_> = source.coords().map(xyzm).collect();
            for (i, vertex) in hull.coords().map(xyzm).enumerate() {
                assert!(
                    source.contains(&vertex),
                    "hull vertex {i} is not one of the input vertices"
                );
            }
            checked += 1;
        }
        assert!(checked > 0, "fixture produced no non-null rows");
    }
}
