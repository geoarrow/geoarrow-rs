//! A custom implementation of ffi_stream to export chunked arrays, not RecordBatches
//!
//! This is derived from
//! https://github.com/apache/arrow-rs/blob/9d0abcc6f4e11594c23811c2c2d297f2eb2963af/arrow/src/ffi_stream.rs

use arrow::ffi_stream::FFI_ArrowArrayStream;
use std::ptr::addr_of;
use std::{
    convert::TryFrom,
    ffi::CString,
    os::raw::{c_char, c_int, c_void},
};

use arrow::array::Array;
use arrow::error::ArrowError;
use arrow::ffi::*;

use crate::ffi::to_python::chunked::ArrayReader;

const ENOMEM: i32 = 12;
const EIO: i32 = 5;
const EINVAL: i32 = 22;
const ENOSYS: i32 = 78;

/// Create a new [`FFI_ArrowArrayStream`] from an [`ArrayReader`].
pub fn new_stream(array_reader: Box<dyn ArrayReader + Send>) -> FFI_ArrowArrayStream {
    let private_data = Box::new(ArrayStreamPrivateData {
        array_reader,
        last_error: None,
    });

    FFI_ArrowArrayStream {
        get_schema: Some(get_schema),
        get_next: Some(get_next),
        get_last_error: Some(get_last_error),
        release: Some(release_stream),
        private_data: Box::into_raw(private_data) as *mut c_void,
    }
}

// callback used to drop [FFI_ArrowArrayStream] when it is exported.
unsafe extern "C" fn release_stream(stream: *mut FFI_ArrowArrayStream) {
    if stream.is_null() {
        return;
    }
    let stream = &mut *stream;

    stream.get_schema = None;
    stream.get_next = None;
    stream.get_last_error = None;

    let private_data = Box::from_raw(stream.private_data as *mut ArrayStreamPrivateData);
    drop(private_data);

    stream.release = None;
}

struct ArrayStreamPrivateData {
    array_reader: Box<dyn ArrayReader + Send>,
    last_error: Option<CString>,
}

// The callback used to get array schema
unsafe extern "C" fn get_schema(
    stream: *mut FFI_ArrowArrayStream,
    schema: *mut FFI_ArrowSchema,
) -> c_int {
    ExportedArrayStream { stream }.get_schema(schema)
}

// The callback used to get next array
unsafe extern "C" fn get_next(
    stream: *mut FFI_ArrowArrayStream,
    array: *mut FFI_ArrowArray,
) -> c_int {
    ExportedArrayStream { stream }.get_next(array)
}

// The callback used to get the error from last operation on the `FFI_ArrowArrayStream`
unsafe extern "C" fn get_last_error(stream: *mut FFI_ArrowArrayStream) -> *const c_char {
    let mut ffi_stream = ExportedArrayStream { stream };
    // The consumer should not take ownership of this string, we should return
    // a const pointer to it.
    match ffi_stream.get_last_error() {
        Some(err_string) => err_string.as_ptr(),
        None => std::ptr::null(),
    }
}

struct ExportedArrayStream {
    stream: *mut FFI_ArrowArrayStream,
}

impl ExportedArrayStream {
    fn get_private_data(&mut self) -> &mut ArrayStreamPrivateData {
        unsafe { &mut *((*self.stream).private_data as *mut ArrayStreamPrivateData) }
    }

    pub fn get_schema(&mut self, out: *mut FFI_ArrowSchema) -> i32 {
        let private_data = self.get_private_data();
        let reader = &private_data.array_reader;

        let schema = FFI_ArrowSchema::try_from(reader.field().as_ref());

        match schema {
            Ok(schema) => {
                unsafe { std::ptr::copy(addr_of!(schema), out, 1) };
                std::mem::forget(schema);
                0
            }
            Err(ref err) => {
                private_data.last_error = Some(
                    CString::new(err.to_string()).expect("Error string has a null byte in it."),
                );
                get_error_code(err)
            }
        }
    }

    pub fn get_next(&mut self, out: *mut FFI_ArrowArray) -> i32 {
        let private_data = self.get_private_data();
        let reader = &mut private_data.array_reader;

        match reader.next() {
            None => {
                // Marks ArrowArray released to indicate reaching the end of stream.
                unsafe { std::ptr::write(out, FFI_ArrowArray::empty()) }
                0
            }
            Some(next_array) => {
                if let Ok(array) = next_array {
                    let array = FFI_ArrowArray::new(&array.to_data());

                    unsafe { std::ptr::write_unaligned(out, array) };
                    0
                } else {
                    let err = &next_array.unwrap_err();
                    private_data.last_error = Some(
                        CString::new(err.to_string()).expect("Error string has a null byte in it."),
                    );
                    get_error_code(err)
                }
            }
        }
    }

    pub fn get_last_error(&mut self) -> Option<&CString> {
        self.get_private_data().last_error.as_ref()
    }
}

fn get_error_code(err: &ArrowError) -> i32 {
    match err {
        ArrowError::NotYetImplemented(_) => ENOSYS,
        ArrowError::MemoryError(_) => ENOMEM,
        ArrowError::IoError(_, _) => EIO,
        _ => EINVAL,
    }
}
