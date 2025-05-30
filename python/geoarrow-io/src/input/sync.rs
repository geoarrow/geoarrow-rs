//! Helpers for user input to access files and file objects in a synchronous manner.
//!
//! Partially vendored and derived from https://github.com/omerbenamram/pyo3-file under the
//! MIT/Apache 2 license

use bytes::Bytes;
use parquet::file::reader::{ChunkReader, Length};
use pyo3::prelude::*;
use pyo3_file::PyFileLikeObject;

use std::fs::File;
use std::io::{self, BufRead, BufReader, BufWriter};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

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
                PyFileLikeObject::with_requirements(ob.clone().unbind(), true, false, true, false)?,
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
                let name = writer.get_ref().py_name(py)?;
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
            Self::FileLike(writer) => writer.get_ref().py_name(py),
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
                PyFileLikeObject::with_requirements(ob.clone().unbind(), false, true, true, false)?,
            )))
        }
    }
}
