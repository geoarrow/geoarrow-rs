use std::fs::File;
use std::io::BufWriter;

use crate::error::PyGeoArrowResult;
use crate::table::GeoTable;

use geoarrow::io::parquet::{write_geoparquet as _write_geoparquet, GeoParquetWriterOptions};
use pyo3::exceptions::{PyFileNotFoundError, PyValueError};
use pyo3::prelude::*;

pub enum GeoParquetEncoding {
    WKB,
    Native,
}

impl<'a> FromPyObject<'a> for GeoParquetEncoding {
    fn extract(ob: &'a PyAny) -> PyResult<Self> {
        let s: String = ob.extract()?;
        match s.to_lowercase().as_str() {
            "wkb" => Ok(Self::WKB),
            "native" => Ok(Self::Native),
            _ => Err(PyValueError::new_err(
                "Unexpected encoding. Should be one of 'WKB' or 'native'.",
            )),
        }
    }
}

impl From<GeoParquetEncoding> for geoarrow::io::parquet::GeoParquetWriterEncoding {
    fn from(value: GeoParquetEncoding) -> Self {
        match value {
            GeoParquetEncoding::WKB => Self::WKB,
            GeoParquetEncoding::Native => Self::Native,
        }
    }
}

/// Write a GeoTable to a GeoParquet file on disk.
///
/// Args:
///     table: the table to write.
///     file: the path to the file or a Python file object in binary write mode.
///
/// Returns:
///     None
#[pyfunction]
#[pyo3(
    signature = (table, file, *, encoding = GeoParquetEncoding::WKB),
    text_signature = "(table, file, *, encoding = 'WKB')")
]
pub fn write_parquet(
    mut table: GeoTable,
    file: String,
    encoding: GeoParquetEncoding,
) -> PyGeoArrowResult<()> {
    let writer = BufWriter::new(
        File::create(file).map_err(|err| PyFileNotFoundError::new_err(err.to_string()))?,
    );
    let options = GeoParquetWriterOptions {
        encoding: encoding.into(),
        ..Default::default()
    };
    _write_geoparquet(&mut table.0, writer, &options)?;
    Ok(())
}
