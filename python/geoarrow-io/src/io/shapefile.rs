use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;

use crate::error::PyGeoArrowResult;
use crate::util::to_arro3_table;
use arrow::array::RecordBatchReader;
use geoarrow::io::shapefile::{ShapefileReaderBuilder, ShapefileReaderOptions};
use geoarrow::table::Table;
use pyo3::prelude::*;
use pyo3_arrow::export::Arro3Table;
use pyo3_geoarrow::PyCoordType;

#[pyfunction]
#[pyo3(
    signature = (
        shp_path,
        *,
        batch_size=65536,
        coord_type = PyCoordType::Interleaved,
    ),
    text_signature = "(shp_path, *, batch_size=65536, coord_type='interleaved')"
)]
pub fn read_shapefile(
    shp_path: PathBuf,
    batch_size: usize,
    coord_type: PyCoordType,
) -> PyGeoArrowResult<Arro3Table> {
    let shp_path = shp_path.canonicalize()?;
    let mut dbf_path = shp_path.clone();
    dbf_path.set_extension("dbf");

    let mut prj_path = shp_path.clone();
    prj_path.set_extension("prj");

    let mut crs: Option<String> = None;
    if let Ok(content) = std::fs::read(&prj_path) {
        if let Ok(content) = String::from_utf8(content) {
            crs = Some(content);
        }
    }

    let options = ShapefileReaderOptions {
        crs,
        batch_size: Some(batch_size),
        coord_type: coord_type.into(),
    };

    let shp_file = BufReader::new(File::open(shp_path)?);
    let dbf_file = BufReader::new(File::open(dbf_path)?);

    let mut builder = ShapefileReaderBuilder::try_new(shp_file, dbf_file, options)?;
    let reader = builder.read()?;

    // Note: this fails because it's trying to cast to `'static` when passing to
    // PyRecordBatchReader. We need to remove the `'a` lifetime in the core shapefile reader
    // implementation, but to do that we need to change the iterators in the `shapefile` crate to
    // be owning instead of borrowing.
    // Ok(PyRecordBatchReader::new(reader).into())

    let schema = reader.schema();
    let batches = reader.collect::<std::result::Result<Vec<_>, _>>()?;
    let table = Table::try_new(batches, schema).unwrap();
    Ok(to_arro3_table(table))
}
