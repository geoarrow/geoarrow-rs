//! Helpers for user input to access files and file objects in a synchronous manner.
//!
//! Partially vendored and derived from https://github.com/omerbenamram/pyo3-file under the
//! MIT/Apache 2 license

use bytes::Bytes;
use parquet::file::reader::{ChunkReader, Length};
use pyo3::exceptions::PyTypeError;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyString};

use std::fs::File;
use std::io::{self, BufRead, BufReader, BufWriter};
use std::io::{Read, Seek, SeekFrom, Write};
#[cfg(not(target_os = "windows"))]
use std::os::fd::{AsRawFd, RawFd};
use std::path::{Path, PathBuf};

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
            let io = PyModule::import_bound(py, "io")?;
            let text_io = io.getattr("TextIOBase")?;

            let is_text_io = object.bind(py).is_instance(&text_io)?;

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

    pub fn name(&self, py: Python) -> Option<String> {
        self.inner
            .getattr(py, intern!(py, "name"))
            .ok()
            .map(|x| x.to_string())
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

        match e_as_object.call_method_bound(py, "__str__", (), None) {
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
                    .call_method_bound(py, "read", (buf.len() / 4,), None)
                    .map_err(pyerr_to_io_err)?;
                let string: String = res.extract(py)?;
                let bytes = string.as_bytes();
                buf.write_all(bytes)?;
                Ok(bytes.len())
            } else {
                let res = self
                    .inner
                    .call_method_bound(py, "read", (buf.len(),), None)
                    .map_err(pyerr_to_io_err)?;
                let bytes: Vec<u8> = res.extract(py)?;
                buf.write_all(&bytes)?;
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
                PyString::new_bound(py, s).to_object(py)
            } else {
                PyBytes::new_bound(py, buf).to_object(py)
            };

            let number_bytes_written = self
                .inner
                .call_method_bound(py, "write", (arg,), None)
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
                .call_method_bound(py, "flush", (), None)
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
                .call_method_bound(py, "seek", (offset, whence), None)
                .map_err(pyerr_to_io_err)?;

            new_position.extract(py).map_err(pyerr_to_io_err)
        })
    }
}

#[cfg(not(target_os = "windows"))]
impl AsRawFd for PyFileLikeObject {
    fn as_raw_fd(&self) -> RawFd {
        Python::with_gil(|py| {
            let fileno = self
                .inner
                .getattr(py, "fileno")
                .expect("Object does not have a fileno() method.");

            let fd = fileno
                .call_bound(py, (), None)
                .expect("fileno() method did not return a file descriptor.");

            fd.extract(py).expect("File descriptor is not an integer.")
        })
    }
}

/// Implements Read + Seek
#[derive(Debug)]
pub enum FileReader {
    File(String, BufReader<File>),
    FileLike(BufReader<PyFileLikeObject>),
}

impl FileReader {
    fn try_clone(&self) -> std::io::Result<Self> {
        match self {
            Self::File(path, f) => Ok(Self::File(
                path.clone(),
                BufReader::new(f.get_ref().try_clone()?),
            )),
            Self::FileLike(f) => Ok(Self::FileLike(BufReader::new(f.get_ref().clone()))),
        }
    }
}

impl Read for FileReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::File(_, reader) => reader.read(buf),
            Self::FileLike(reader) => reader.read(buf),
        }
    }
}

impl Seek for FileReader {
    fn seek(&mut self, pos: SeekFrom) -> Result<u64, io::Error> {
        match self {
            Self::File(_, reader) => reader.seek(pos),
            Self::FileLike(reader) => reader.seek(pos),
        }
    }
}

impl BufRead for FileReader {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        match self {
            Self::File(_, reader) => reader.fill_buf(),
            Self::FileLike(reader) => reader.fill_buf(),
        }
    }

    fn consume(&mut self, amt: usize) {
        match self {
            Self::File(_, reader) => reader.consume(amt),
            Self::FileLike(reader) => reader.consume(amt),
        }
    }
}

impl Length for FileReader {
    fn len(&self) -> u64 {
        match self {
            Self::File(_path, f) => f.get_ref().len(),
            Self::FileLike(f) => {
                let mut file = f.get_ref().clone();
                // Keep track of current pos
                let pos = file.stream_position().unwrap();

                // Seek to end of file
                file.seek(std::io::SeekFrom::End(0)).unwrap();
                let len = file.stream_position().unwrap();

                // Seek back
                file.seek(std::io::SeekFrom::Start(pos)).unwrap();
                len
            }
        }
    }
}

impl ChunkReader for FileReader {
    type T = FileReader;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        let mut reader = self.try_clone()?;
        reader.seek(SeekFrom::Start(start))?;
        Ok(self.try_clone()?)
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        let mut buffer = Vec::with_capacity(length);
        let mut reader = self.try_clone()?;
        reader.seek(SeekFrom::Start(start))?;
        let read = reader.take(length as _).read_to_end(&mut buffer)?;

        if read != length {
            return Err(parquet::errors::ParquetError::EOF(format!(
                "Expected to read {} bytes, read only {}",
                length, read,
            )));
        }
        Ok(buffer.into())
    }
}

impl<'py> FromPyObject<'py> for FileReader {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        if let Ok(path_buf) = ob.extract::<PathBuf>() {
            let path = path_buf.to_string_lossy().to_string();
            Ok(Self::File(path, BufReader::new(File::open(path_buf)?)))
        } else if let Ok(path) = ob.extract::<String>() {
            Ok(Self::File(path.clone(), BufReader::new(File::open(path)?)))
        } else {
            Ok(Self::FileLike(BufReader::new(
                PyFileLikeObject::with_requirements(
                    ob.as_gil_ref().into(),
                    true,
                    false,
                    true,
                    false,
                )?,
            )))
        }
    }
}

pub enum FileWriter {
    File(String, BufWriter<File>),
    FileLike(BufWriter<PyFileLikeObject>),
}

impl Write for FileWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Self::File(_path, writer) => writer.write(buf),
            Self::FileLike(writer) => writer.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::File(_path, writer) => writer.flush(),
            Self::FileLike(writer) => writer.flush(),
        }
    }
}

impl Seek for FileWriter {
    fn seek(&mut self, pos: SeekFrom) -> Result<u64, io::Error> {
        match self {
            Self::File(_path, writer) => writer.seek(pos),
            Self::FileLike(writer) => writer.seek(pos),
        }
    }
}

impl FileWriter {
    pub fn file_stem(&self, py: Python) -> Option<String> {
        match self {
            Self::File(path, _writer) => {
                let path = Path::new(path);
                Some(path.file_stem()?.to_str()?.to_string())
            }
            Self::FileLike(writer) => {
                let name = writer.get_ref().name(py)?;
                let path = Path::new(&name).file_stem()?;
                Some(path.to_str()?.to_string())
            }
        }
    }

    pub fn file_name(&self, py: Python) -> Option<String> {
        match self {
            Self::File(path, _writer) => {
                let path = Path::new(path);
                Some(path.file_name()?.to_str()?.to_string())
            }
            Self::FileLike(writer) => writer.get_ref().name(py),
        }
    }
}

impl<'py> FromPyObject<'py> for FileWriter {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        if let Ok(path_buf) = ob.extract::<PathBuf>() {
            let path = path_buf.to_string_lossy().to_string();
            Ok(Self::File(path, BufWriter::new(File::create(path_buf)?)))
        } else if let Ok(path) = ob.extract::<String>() {
            Ok(Self::File(
                path.clone(),
                BufWriter::new(File::create(path)?),
            ))
        } else {
            Ok(Self::FileLike(BufWriter::new(
                PyFileLikeObject::with_requirements(
                    ob.as_gil_ref().into(),
                    false,
                    true,
                    true,
                    false,
                )?,
            )))
        }
    }
}
