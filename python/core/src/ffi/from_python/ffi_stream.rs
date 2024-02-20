//! A custom implementation of ArrowArrayStreamReader to support ChunkedArrays: a stream of arrays
//! of any data type that is not expected to represent record batches.
//!
//! This is derived from
//! https://github.com/apache/arrow-rs/blob/9d0abcc6f4e11594c23811c2c2d297f2eb2963af/arrow/src/ffi_stream.rs

use std::ffi::CStr;
use std::sync::Arc;

use arrow::datatypes::{Field, FieldRef};
use arrow::error::ArrowError;
use arrow::ffi::{from_ffi_and_data_type, FFI_ArrowArray, FFI_ArrowSchema};
use arrow::ffi_stream::FFI_ArrowArrayStream;
use arrow_array::{make_array, Array};

#[derive(Debug)]
pub struct ArrowArrayStreamReader {
    stream: FFI_ArrowArrayStream,
    field: FieldRef,
}

/// Gets schema from a raw pointer of `FFI_ArrowArrayStream`. This is used when constructing
/// `ArrowArrayStreamReader` to cache schema.
fn get_stream_schema(stream_ptr: *mut FFI_ArrowArrayStream) -> Result<FieldRef, ArrowError> {
    let mut schema = FFI_ArrowSchema::empty();

    let ret_code = unsafe { (*stream_ptr).get_schema.unwrap()(stream_ptr, &mut schema) };

    if ret_code == 0 {
        let field = Field::try_from(&schema)?;
        Ok(Arc::new(field))
    } else {
        Err(ArrowError::CDataInterface(format!(
            "Cannot get schema from input stream. Error code: {ret_code:?}"
        )))
    }
}

impl ArrowArrayStreamReader {
    /// Creates a new `ArrowArrayStreamReader` from a `FFI_ArrowArrayStream`.
    /// This is used to import from the C Stream Interface.
    #[allow(dead_code)]
    pub fn try_new(mut stream: FFI_ArrowArrayStream) -> Result<Self, ArrowError> {
        if stream.release.is_none() {
            return Err(ArrowError::CDataInterface(
                "input stream is already released".to_string(),
            ));
        }

        let field = get_stream_schema(&mut stream)?;

        Ok(Self { stream, field })
    }

    pub fn field(&self) -> FieldRef {
        self.field.clone()
    }

    /// Get the last error from `ArrowArrayStreamReader`
    fn get_stream_last_error(&mut self) -> Option<String> {
        let get_last_error = self.stream.get_last_error?;

        let error_str = unsafe { get_last_error(&mut self.stream) };
        if error_str.is_null() {
            return None;
        }

        let error_str = unsafe { CStr::from_ptr(error_str) };
        Some(error_str.to_string_lossy().to_string())
    }
}

impl Iterator for ArrowArrayStreamReader {
    type Item = Result<Arc<dyn Array>, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut array = FFI_ArrowArray::empty();

        let ret_code = unsafe { self.stream.get_next.unwrap()(&mut self.stream, &mut array) };

        if ret_code == 0 {
            // The end of stream has been reached
            if array.is_released() {
                return None;
            }

            let result = unsafe { from_ffi_and_data_type(array, self.field().data_type().clone()) };

            Some(result.map(make_array))
        } else {
            let last_error = self.get_stream_last_error();
            let err = ArrowError::CDataInterface(last_error.unwrap());
            Some(Err(err))
        }
    }
}
