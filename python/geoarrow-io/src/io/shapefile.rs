use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;

use crate::error::PyGeoArrowResult;
use crate::util::to_arro3_table;
use geoarrow::io::shapefile::{read_shapefile as _read_shapefile, ShapefileReaderOptions};
use pyo3::prelude::*;
use pyo3_arrow::export::Arro3Table;

#[pyfunction]
// #[pyo3(signature = (file, *, batch_size=65536))]
pub fn read_shapefile(shp_path: PathBuf) -> PyGeoArrowResult<Arro3Table> {
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
        ..Default::default()
    };

    let shp_file = BufReader::new(File::open(shp_path)?);
    let dbf_file = BufReader::new(File::open(dbf_path)?);

    let table = _read_shapefile(shp_file, dbf_file, options)?;
    Ok(to_arro3_table(table))
}
