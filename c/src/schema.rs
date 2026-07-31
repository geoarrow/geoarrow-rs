//! Canonical `geoarrow.*` Arrow schema construction.

use std::os::raw::c_char;

use arrow_schema::ffi::FFI_ArrowSchema;

use crate::ArrowSchema;
use crate::error::{GeoArrowError, GeoArrowErrorCode, catching, finish};
use crate::types::{data_type, metadata};

/// Fill `out` with the Arrow schema for the geoarrow extension type named by
/// `type_`, whose value also selects the dimensions and coordinate layout.
///
/// On success the schema owns its allocations; release it with
/// [`crate::GeoArrowRsSchemaRelease`].
///
/// # Safety
/// `out` must address a writable [`ArrowSchema`]. `crs_projjson` must be null
/// or a NUL-terminated UTF-8 PROJJSON string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn GeoArrowRsSchemaInit(
    out: *mut ArrowSchema,
    type_: i32,
    edge_type: i32,
    crs_projjson: *const c_char,
    error: *mut GeoArrowError,
) -> GeoArrowErrorCode {
    let result = catching(|| {
        if out.is_null() {
            return Err(crate::error::Error::invalid("out schema pointer is null"));
        }
        let metadata = unsafe { metadata(crs_projjson, edge_type) }?;
        let field = data_type(type_, metadata)?.to_field("geometry", true);
        let schema = FFI_ArrowSchema::try_from(&field)?;
        unsafe { std::ptr::write(out as *mut FFI_ArrowSchema, schema) };
        Ok(())
    });
    finish(result, error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{EINVAL, GEOARROW_OK};

    /// Build a schema into an empty slot and return its Arrow format string, or
    /// the status code on failure. The slot owns the result and releases it.
    fn init(type_: i32, edge_type: i32, crs: *const c_char) -> Result<String, GeoArrowErrorCode> {
        let mut slot = FFI_ArrowSchema::empty();
        let rc = unsafe {
            GeoArrowRsSchemaInit(
                &mut slot as *mut FFI_ArrowSchema as *mut ArrowSchema,
                type_,
                edge_type,
                crs,
                std::ptr::null_mut(),
            )
        };
        match rc {
            GEOARROW_OK => Ok(slot.format().to_string()),
            _ => Err(rc),
        }
    }

    #[test]
    fn builds_the_arrow_layout_named_by_the_type() {
        let null = std::ptr::null();
        // Interleaved XY point is a FixedSizeList<double>[2]; the rest are
        // identified by their outer Arrow layout.
        assert_eq!(init(10001, 0, null).unwrap(), "+w:2");
        assert_eq!(init(1003, 0, null).unwrap(), "+l"); // XYZ polygon -> List
        assert_eq!(init(990, 0, null).unwrap(), "+s"); // box -> Struct
        assert_eq!(init(100001, 0, null).unwrap(), "z"); // wkb -> Binary
        assert_eq!(init(100003, 1, null).unwrap(), "u"); // wkt -> Utf8
    }

    #[test]
    fn rejects_bad_arguments() {
        let null = std::ptr::null();
        assert_eq!(init(99, 0, null).unwrap_err(), EINVAL);
        assert_eq!(init(1, 99, null).unwrap_err(), EINVAL);
        let rc =
            unsafe { GeoArrowRsSchemaInit(std::ptr::null_mut(), 1, 0, null, std::ptr::null_mut()) };
        assert_eq!(rc, EINVAL);
    }

    #[test]
    fn accepts_projjson_and_rejects_other_text() {
        let crs = std::ffi::CString::new(r#"{"authority":"EPSG","code":4326}"#).unwrap();
        assert!(init(1, 0, crs.as_ptr()).is_ok());
        let junk = std::ffi::CString::new("not valid json").unwrap();
        assert_eq!(init(1, 0, junk.as_ptr()).unwrap_err(), EINVAL);
    }

    /// The message must reach a caller-supplied sink, since there is no
    /// thread-local to fall back on.
    #[test]
    fn writes_detail_into_the_caller_sink() {
        let mut sink = GeoArrowError { message: [0; 1024] };
        let mut slot = FFI_ArrowSchema::empty();
        let rc = unsafe {
            GeoArrowRsSchemaInit(
                &mut slot as *mut FFI_ArrowSchema as *mut ArrowSchema,
                12345,
                0,
                std::ptr::null(),
                &mut sink,
            )
        };
        assert_eq!(rc, EINVAL);
        let message = unsafe { std::ffi::CStr::from_ptr(sink.message.as_ptr()) }
            .to_string_lossy()
            .into_owned();
        assert!(message.contains("12345"), "got {message:?}");
    }
}
