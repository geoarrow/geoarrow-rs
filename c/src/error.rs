//! Error reporting, following the geoarrow-c idiom: every entry point returns
//! an errno-compatible [`GeoArrowErrorCode`] and writes detail into a
//! caller-allocated [`GeoArrowError`].

use std::os::raw::c_char;

use geoarrow_schema::error::GeoArrowError as RsError;

/// Errno-compatible status code. [`GEOARROW_OK`] on success, otherwise one of
/// the values in `<errno.h>`.
pub type GeoArrowErrorCode = i32;

pub const GEOARROW_OK: GeoArrowErrorCode = 0;

/// Caller-allocated error sink. Layout-compatible with geoarrow-c's
/// `struct GeoArrowError`, so a consumer of both libraries can pass the same
/// struct to either.
#[repr(C)]
pub struct GeoArrowError {
    pub message: [c_char; 1024],
}

// Not `pub`: cbindgen would emit #defines that clash with <errno.h>.
pub(crate) const EIO: GeoArrowErrorCode = 5;
pub(crate) const ENOMEM: GeoArrowErrorCode = 12;
pub(crate) const EINVAL: GeoArrowErrorCode = 22;

/// An errno code paired with the message to write into the caller's sink.
#[derive(Debug)]
pub(crate) struct Error {
    pub code: GeoArrowErrorCode,
    pub message: String,
}

impl Error {
    /// Bad argument: null pointer, out-of-range enum, malformed pattern, wrong
    /// input array type.
    pub(crate) fn invalid(message: impl Into<String>) -> Self {
        Self {
            code: EINVAL,
            message: message.into(),
        }
    }

    /// Failure while reading, writing, parsing, or computing.
    pub(crate) fn io(message: impl Into<String>) -> Self {
        Self {
            code: EIO,
            message: message.into(),
        }
    }
}

impl From<RsError> for Error {
    fn from(e: RsError) -> Self {
        let code = match e {
            RsError::IOError(_) | RsError::GeoParquet(_) => EIO,
            _ => EINVAL,
        };
        Self {
            code,
            message: e.to_string(),
        }
    }
}

impl From<arrow_schema::ArrowError> for Error {
    fn from(e: arrow_schema::ArrowError) -> Self {
        match e {
            arrow_schema::ArrowError::MemoryError(_) => Self {
                code: ENOMEM,
                message: e.to_string(),
            },
            _ => Self::invalid(e.to_string()),
        }
    }
}

/// Collapse an operation's result into the C return value, writing any message
/// into `error` (which may be null).
pub(crate) fn finish(result: Result<(), Error>, error: *mut GeoArrowError) -> GeoArrowErrorCode {
    match result {
        Ok(()) => GEOARROW_OK,
        Err(e) => {
            set_error(error, &e.message);
            e.code
        }
    }
}

/// Run an entry point's body, converting a panic into an [`EIO`] error instead
/// of unwinding across the C boundary and aborting the caller's process.
pub(crate) fn catching<F>(f: F) -> Result<(), Error>
where
    F: FnOnce() -> Result<(), Error>,
{
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)).unwrap_or_else(|panic| {
        let message = panic
            .downcast_ref::<&str>()
            .map(|s| s.to_string())
            .or_else(|| panic.downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "unknown panic".to_string());
        Err(Error::io(format!("panic: {message}")))
    })
}

/// Write a NUL-terminated, truncated copy of `message` into `error`. Truncation
/// respects UTF-8 boundaries so the result stays valid UTF-8 as well as valid C.
fn set_error(error: *mut GeoArrowError, message: &str) {
    let Some(sink) = (unsafe { error.as_mut() }) else {
        return;
    };
    let mut len = message.len().min(sink.message.len() - 1);
    while !message.is_char_boundary(len) {
        len -= 1;
    }
    for (slot, byte) in sink.message.iter_mut().zip(&message.as_bytes()[..len]) {
        *slot = *byte as c_char;
    }
    sink.message[len] = 0;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read(sink: &GeoArrowError) -> String {
        unsafe { std::ffi::CStr::from_ptr(sink.message.as_ptr()) }
            .to_string_lossy()
            .into_owned()
    }

    fn empty_sink() -> GeoArrowError {
        GeoArrowError { message: [0; 1024] }
    }

    #[test]
    fn finish_writes_code_and_message() {
        let mut sink = empty_sink();
        let rc = finish(Err(Error::invalid("bad dimension")), &mut sink);
        assert_eq!(rc, EINVAL);
        assert_eq!(read(&sink), "bad dimension");
    }

    #[test]
    fn finish_tolerates_null_sink() {
        assert_eq!(finish(Err(Error::io("boom")), std::ptr::null_mut()), EIO);
        assert_eq!(finish(Ok(()), std::ptr::null_mut()), GEOARROW_OK);
    }

    /// A panic in an entry point must come back as an error code, never an
    /// abort of the host process.
    #[test]
    fn catching_turns_a_panic_into_eio() {
        let mut sink = empty_sink();
        let rc = finish(catching(|| panic!("data-dependent assert")), &mut sink);
        assert_eq!(rc, EIO);
        assert_eq!(read(&sink), "panic: data-dependent assert");
    }

    /// Truncation must not split a multi-byte character, or the message stops
    /// being valid UTF-8 for callers that decode it as such (the JVM does).
    #[test]
    fn long_message_truncates_on_a_char_boundary() {
        let mut sink = empty_sink();
        finish(Err(Error::invalid("é".repeat(1000))), &mut sink);
        let got = read(&sink);
        assert!(got.len() < 1024);
        assert!(got.chars().all(|c| c == 'é'));
    }
}
