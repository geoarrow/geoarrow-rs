//! Vendored and derived from https://github.com/omerbenamram/pyo3-file under the MIT/Apache 2
//! license

use pyo3::exceptions::{PyFileNotFoundError, PyTypeError};
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyString, PyType};

use std::fs::File;
use std::io::{self, BufRead, BufReader, BufWriter};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::fd::{AsRawFd, RawFd};
use std::path::Path;

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

// impl Length for PyFileLikeObject {
//     fn len(&self) -> u64 {
//         let len = self.seek(SeekFrom::End(0)).unwrap();
//         len
//     }
// }

// impl ChunkReader for PyFileLikeObject {
//     type T = Self;

//     fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
//         self.seek(SeekFrom::Start(start))?;
//         Ok(self.clone())
//     }

//     fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
//         todo!()
//     }
// }

/// Implements Read + Seek
pub enum BinaryFileReader {
    String(BufReader<File>),
    FileLike(BufReader<PyFileLikeObject>),
}

impl Read for BinaryFileReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::String(reader) => reader.read(buf),
            Self::FileLike(reader) => reader.read(buf),
        }
    }
}

impl Seek for BinaryFileReader {
    fn seek(&mut self, pos: SeekFrom) -> Result<u64, io::Error> {
        match self {
            Self::String(reader) => reader.seek(pos),
            Self::FileLike(reader) => reader.seek(pos),
        }
    }
}

impl BufRead for BinaryFileReader {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        match self {
            Self::String(reader) => reader.fill_buf(),
            Self::FileLike(reader) => reader.fill_buf(),
        }
    }

    fn consume(&mut self, amt: usize) {
        match self {
            Self::String(reader) => reader.consume(amt),
            Self::FileLike(reader) => reader.consume(amt),
        }
    }
}

// impl Length for BinaryFileReader {
//     fn len(&self) -> u64 {
//         match self {
//             Self::String(reader) => reader.get_ref().len(),
//             Self::FileLike(reader) => reader.get_ref().len(),
//         }
//     }
// }

// impl ChunkReader for BinaryFileReader {
//     type T = Self;

//     fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
//         match self {
//             Self::String(reader) => Ok(BinaryFileReader::String(reader.get_ref().get_read(start)?)),
//             Self::FileLike(reader) => Ok(BinaryFileReader::FileLike(BufReader::new(
//                 reader.get_ref().get_read(start)?,
//             ))),
//         }
//     }

//     fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
//         match self {
//             Self::String(reader) => reader.get_ref().get_bytes(start, length),
//             Self::FileLike(reader) => reader.get_ref().get_bytes(start, length),
//         }
//     }
// }

impl<'a> FromPyObject<'a> for BinaryFileReader {
    fn extract(ob: &'a PyAny) -> PyResult<Self> {
        if let Ok(string_ref) = ob.downcast::<PyString>() {
            let path = string_ref.to_string_lossy().to_string();
            let reader = BufReader::new(
                File::open(path).map_err(|err| PyFileNotFoundError::new_err(err.to_string()))?,
            );
            return Ok(Self::String(reader));
        }

        Python::with_gil(|py| {
            let module = PyModule::import(py, intern!(py, "pathlib"))?;
            let path = module.getattr(intern!(py, "Path"))?;
            let path_type = path.extract::<&PyType>()?;
            if ob.is_instance(path_type)? {
                let path = ob.to_string();
                let reader = BufReader::new(
                    File::open(path)
                        .map_err(|err| PyFileNotFoundError::new_err(err.to_string()))?,
                );
                return Ok(Self::String(reader));
            }

            match PyFileLikeObject::with_requirements(ob.into(), true, false, true, false) {
                Ok(f) => Ok(Self::FileLike(BufReader::new(f))),
                Err(e) => Err(e),
            }
        })
    }
}

pub enum BinaryFileWriter {
    String(String, BufWriter<File>),
    FileLike(BufWriter<PyFileLikeObject>),
}

impl Write for BinaryFileWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Self::String(_path, writer) => writer.write(buf),
            Self::FileLike(writer) => writer.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::String(_path, writer) => writer.flush(),
            Self::FileLike(writer) => writer.flush(),
        }
    }
}

impl Seek for BinaryFileWriter {
    fn seek(&mut self, pos: SeekFrom) -> Result<u64, io::Error> {
        match self {
            Self::String(_path, writer) => writer.seek(pos),
            Self::FileLike(writer) => writer.seek(pos),
        }
    }
}

impl BinaryFileWriter {
    pub fn file_stem(&self, py: Python) -> Option<String> {
        match self {
            Self::String(path, _writer) => {
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
            Self::String(path, _writer) => {
                let path = Path::new(path);
                Some(path.file_name()?.to_str()?.to_string())
            }
            Self::FileLike(writer) => writer.get_ref().name(py),
        }
    }
}

impl<'a> FromPyObject<'a> for BinaryFileWriter {
    fn extract(ob: &'a PyAny) -> PyResult<Self> {
        if let Ok(string_ref) = ob.downcast::<PyString>() {
            let path = string_ref.to_string_lossy().to_string();
            let writer = BufWriter::new(
                File::create(&path).map_err(|err| PyFileNotFoundError::new_err(err.to_string()))?,
            );
            return Ok(Self::String(path, writer));
        }

        Python::with_gil(|py| {
            let module = PyModule::import(py, intern!(py, "pathlib"))?;
            let path = module.getattr(intern!(py, "Path"))?;
            let path_type = path.extract::<&PyType>()?;
            if ob.is_instance(path_type)? {
                let path = ob.to_string();
                let writer = BufWriter::new(
                    File::create(&path)
                        .map_err(|err| PyFileNotFoundError::new_err(err.to_string()))?,
                );
                return Ok(Self::String(path, writer));
            }

            match PyFileLikeObject::with_requirements(ob.into(), false, true, true, false) {
                Ok(f) => Ok(Self::FileLike(BufWriter::new(f))),
                Err(e) => Err(e),
            }
        })
    }
}
