use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;

use crate::error::PyGeoArrowResult;
use crate::util::to_arro3_table;
use geoarrow::io::shapefile::{ShapefileReaderOptions, read_shapefile as _read_shapefile};
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

    let table = _read_shapefile(shp_file, dbf_file, options)?;
    Ok(to_arro3_table(table))
}
