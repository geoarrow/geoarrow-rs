//! Vendored from https://github.com/omerbenamram/pyo3-file under the MIT/Apache 2 license

use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyString, PyType};

use std::io;
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::fd::{AsRawFd, RawFd};

#[derive(Debug)]
pub struct PyFileLikeObject {
    inner: PyObject,
    is_text_io: bool,
}

/// Wraps a `PyObject`, and implements read, seek, and write for it.
impl PyFileLikeObject {
    /// Creates an instance of a `PyFileLikeObject` from a `PyObject`.
    /// To assert the object has the required methods methods,
    /// instantiate it with `PyFileLikeObject::require`
    pub fn new(object: PyObject) -> PyResult<Self> {
        Python::with_gil(|py| {
            let io = PyModule::import(py, "io")?;
            let text_io = io.getattr("TextIOBase")?;

            let text_io_type = text_io.extract::<&PyType>()?;
            let is_text_io = object.as_ref(py).is_instance(text_io_type)?;

            Ok(PyFileLikeObject {
                inner: object,
                is_text_io,
            })
        })
    }

    /// Same as `PyFileLikeObject::new`, but validates that the underlying
    /// python object has a `read`, `write`, and `seek` methods in respect to parameters.
    /// Will return a `TypeError` if object does not have `read`, `seek`, `write` and `fileno` methods.
    pub fn with_requirements(
        object: PyObject,
        read: bool,
        write: bool,
        seek: bool,
        fileno: bool,
    ) -> PyResult<Self> {
        Python::with_gil(|py| {
            if read && object.getattr(py, "read").is_err() {
                return Err(PyErr::new::<PyTypeError, _>(
                    "Object does not have a .read() method.",
                ));
            }

            if seek && object.getattr(py, "seek").is_err() {
                return Err(PyErr::new::<PyTypeError, _>(
                    "Object does not have a .seek() method.",
                ));
            }

            if write && object.getattr(py, "write").is_err() {
                return Err(PyErr::new::<PyTypeError, _>(
                    "Object does not have a .write() method.",
                ));
            }

            if fileno && object.getattr(py, "fileno").is_err() {
                return Err(PyErr::new::<PyTypeError, _>(
                    "Object does not have a .fileno() method.",
                ));
            }

            PyFileLikeObject::new(object)
        })
    }
}

impl Clone for PyFileLikeObject {
    fn clone(&self) -> Self {
        Python::with_gil(|py| {
            PyFileLikeObject::new(self.inner.clone_ref(py)).expect("Failed to clone")
        })
    }
}

/// Extracts a string repr from, and returns an IO error to send back to rust.
fn pyerr_to_io_err(e: PyErr) -> io::Error {
    Python::with_gil(|py| {
        let e_as_object: PyObject = e.into_py(py);

        match e_as_object.call_method(py, "__str__", (), None) {
            Ok(repr) => match repr.extract::<String>(py) {
                Ok(s) => io::Error::new(io::ErrorKind::Other, s),
                Err(_e) => io::Error::new(io::ErrorKind::Other, "An unknown error has occurred"),
            },
            Err(_) => io::Error::new(io::ErrorKind::Other, "Err doesn't have __str__"),
        }
    })
}

impl Read for PyFileLikeObject {
    fn read(&mut self, mut buf: &mut [u8]) -> Result<usize, io::Error> {
        Python::with_gil(|py| {
            if self.is_text_io {
                if buf.len() < 4 {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        "buffer size must be at least 4 bytes",
                    ));
                }
                let res = self
                    .inner
                    .call_method(py, "read", (buf.len() / 4,), None)
                    .map_err(pyerr_to_io_err)?;
                let pystring: &PyString = res
                    .downcast(py)
                    .expect("Expecting to be able to downcast into str from read result.");
                let bytes = pystring.to_str().unwrap().as_bytes();
                buf.write_all(bytes)?;
                Ok(bytes.len())
            } else {
                let res = self
                    .inner
                    .call_method(py, "read", (buf.len(),), None)
                    .map_err(pyerr_to_io_err)?;
                let pybytes: &PyBytes = res
                    .downcast(py)
                    .expect("Expecting to be able to downcast into bytes from read result.");
                let bytes = pybytes.as_bytes();
                buf.write_all(bytes)?;
                Ok(bytes.len())
            }
        })
    }
}

impl Write for PyFileLikeObject {
    fn write(&mut self, buf: &[u8]) -> Result<usize, io::Error> {
        Python::with_gil(|py| {
            let arg = if self.is_text_io {
                let s = std::str::from_utf8(buf)
                    .expect("Tried to write non-utf8 data to a TextIO object.");
                PyString::new(py, s).to_object(py)
            } else {
                PyBytes::new(py, buf).to_object(py)
            };

            let number_bytes_written = self
                .inner
                .call_method(py, "write", (arg,), None)
                .map_err(pyerr_to_io_err)?;

            if number_bytes_written.is_none(py) {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "write() returned None, expected number of bytes written",
                ));
            }

            number_bytes_written.extract(py).map_err(pyerr_to_io_err)
        })
    }

    fn flush(&mut self) -> Result<(), io::Error> {
        Python::with_gil(|py| {
            self.inner
                .call_method(py, "flush", (), None)
                .map_err(pyerr_to_io_err)?;

            Ok(())
        })
    }
}

impl Seek for PyFileLikeObject {
    fn seek(&mut self, pos: SeekFrom) -> Result<u64, io::Error> {
        Python::with_gil(|py| {
            let (whence, offset) = match pos {
                SeekFrom::Start(i) => (0, i as i64),
                SeekFrom::Current(i) => (1, i),
                SeekFrom::End(i) => (2, i),
            };

            let new_position = self
                .inner
                .call_method(py, "seek", (offset, whence), None)
                .map_err(pyerr_to_io_err)?;

            new_position.extract(py).map_err(pyerr_to_io_err)
        })
    }
}

impl AsRawFd for PyFileLikeObject {
    fn as_raw_fd(&self) -> RawFd {
        Python::with_gil(|py| {
            let fileno = self
                .inner
                .getattr(py, "fileno")
                .expect("Object does not have a fileno() method.");

            let fd = fileno
                .call(py, (), None)
                .expect("fileno() method did not return a file descriptor.");

            fd.extract(py).expect("File descriptor is not an integer.")
        })
    }
}
